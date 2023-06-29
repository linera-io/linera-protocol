// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides web files to run a block explorer from linera service node.

mod graphql;
mod js_utils;

use graphql_client::reqwest::post_graphql;
use linera_base::{crypto::CryptoHash, identifiers::ChainId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_wasm_bindgen::from_value;
use std::str::FromStr;
use url::Url;
use wasm_bindgen::prelude::*;

use graphql::{
    applications::ApplicationsApplications, block::BlockBlock, blocks::BlocksBlocks, Applications,
    Block, Blocks, Chains,
};
use js_utils::{getf, log_str, setf, SER};

/// Page enum containing info for each page
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
enum Page {
    Unloaded,
    Home {
        blocks: Vec<BlocksBlocks>,
        apps: Vec<ApplicationsApplications>,
    },
    Blocks(Vec<BlocksBlocks>),
    Block(Box<BlockBlock>),
    Applications(Vec<ApplicationsApplications>),
    Error(String),
}

/// Config type dealt with localstorage
#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    node: String,
    tls: bool,
}

/// Data type for vue.js
#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    config: Config,
    page: Page,
    chains: Vec<Value>,
    chain: ChainId,
}

/// Get config from local storage
fn load_config() -> Config {
    let default = Config {
        node: "localhost:8080".to_string(),
        tls: false,
    };
    match web_sys::window()
        .expect("window object not found")
        .local_storage()
    {
        Ok(Some(st)) => match st.get_item("config") {
            Ok(Some(s)) => serde_json::from_str::<Config>(&s).unwrap_or(default),
            _ => default,
        },
        _ => default,
    }
}

/// Initialize vue.js data
#[wasm_bindgen]
pub fn data() -> JsValue {
    let data = Data {
        config: load_config(),
        page: Page::Unloaded,
        chains: Vec::new(),
        chain: ChainId::from_str(
            "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
        )
        .unwrap(),
    };
    data.serialize(&SER).unwrap()
}

fn node_service_address(c: &Config, ws: bool) -> String {
    let proto = if ws { "ws" } else { "http" };
    let tls = if c.tls { "s" } else { "" };
    format!("{}{}://{}", proto, tls, c.node)
}

async fn get_blocks(
    node: &str,
    chain_id: &ChainId,
    from: Option<CryptoHash>,
    limit: Option<u32>,
) -> Result<Vec<BlocksBlocks>, String> {
    let client = reqwest::Client::new();
    let variables = graphql::blocks::Variables {
        from,
        chain_id: Some(*chain_id),
        limit: limit.map(|x| x.into()),
    };
    let res = post_graphql::<Blocks, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    res.data.map_or(Ok(Vec::new()), |d| Ok(d.blocks))
}

async fn get_applications(
    node: &str,
    chain_id: &ChainId,
) -> Result<Vec<ApplicationsApplications>, String> {
    let client = reqwest::Client::new();
    let variables = graphql::applications::Variables {
        chain_id: Some(*chain_id),
    };
    let result = post_graphql::<Applications, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    result.data.map_or(Ok(Vec::new()), |d| Ok(d.applications))
}

fn error(msg: &str) -> (Page, String) {
    (Page::Error(msg.to_string()), "/error".to_string())
}

async fn home(node: &str, chain_id: &ChainId) -> Result<(Page, String), String> {
    let blocks = get_blocks(node, chain_id, None, None).await?;
    let apps = get_applications(node, chain_id).await?;
    Ok((Page::Home { blocks, apps }, "/".to_string()))
}

async fn blocks(
    node: &str,
    chain_id: &ChainId,
    from: Option<CryptoHash>,
    limit: Option<u32>,
) -> Result<(Page, String), String> {
    let blocks = get_blocks(node, chain_id, from, limit).await?;
    Ok((Page::Blocks(blocks), "/blocks".to_string()))
}

async fn block(
    node: &str,
    chain_id: &ChainId,
    hash: Option<CryptoHash>,
) -> Result<(Page, String), String> {
    let client = reqwest::Client::new();
    let variables = graphql::block::Variables {
        hash,
        chain_id: Some(*chain_id),
    };
    let result = post_graphql::<Block, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    let data = result.data.ok_or("no block data found".to_string())?;
    let block = data.block.ok_or("no block found".to_string())?;
    let hash = block.hash;
    Ok((Page::Block(Box::new(block)), format!("/block/{}", hash)))
}

async fn chains(app: &JsValue, node: &str) -> Result<ChainId, String> {
    let client = reqwest::Client::new();
    let variables = graphql::chains::Variables;
    let result = post_graphql::<Chains, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    let chains = result.data.unwrap().chains;
    let chains_js = chains
        .list
        .serialize(&SER)
        .expect("failed to serialize ChainIds");
    setf(app, "chains", &chains_js);
    Ok(chains.default)
}

async fn applications(node: &str, chain_id: &ChainId) -> Result<(Page, String), String> {
    let applications = get_applications(node, chain_id).await?;
    Ok((
        Page::Applications(applications),
        "/applications".to_string(),
    ))
}

fn format_bytes(value: &JsValue) -> JsValue {
    let modified_value = value.clone();
    if let Some(object) = js_sys::Object::try_from(value) {
        js_sys::Object::keys(object)
            .iter()
            .for_each(|k: JsValue| match k.as_string() {
                None => (),
                Some(key_str) => {
                    if &key_str == "bytes" {
                        let array: Vec<u8> = js_sys::Array::from(&getf(&modified_value, "bytes"))
                            .to_vec()
                            .iter()
                            .map(|x| x.as_f64().expect("byte not a u8") as u8)
                            .collect();
                        let array_hex = hex::encode(array);
                        let hex_len = array_hex.len();
                        let hex_minified = if hex_len > 128 {
                            // don't show all hexa if too long
                            format!("{}..{}", &array_hex[0..4], &array_hex[hex_len - 4..])
                        } else {
                            array_hex
                        };
                        setf(&modified_value, "bytes", &JsValue::from_str(&hex_minified))
                    } else {
                        setf(
                            &modified_value,
                            &key_str,
                            &format_bytes(&getf(&modified_value, &key_str)),
                        )
                    }
                }
            });
    };
    modified_value
}

/// Main function to switch between vue.js pages
async fn route_aux(
    app: &JsValue,
    data: &Data,
    path: &Option<String>,
    chain_id: &ChainId,
    args: &[(String, String)],
) {
    let chain_js: JsValue = chain_id
        .serialize(&SER)
        .expect("failed to serialize ChainId");
    setf(app, "chain", &chain_js);
    let path = match (path, data.page.clone()) {
        (Some(p), _) => p,
        (_, Page::Unloaded | Page::Home { .. }) => "",
        (_, Page::Block(_)) => "block",
        (_, Page::Blocks { .. }) => "blocks",
        (_, Page::Applications(_)) => "applications",
        (_, Page::Error(_)) => "error",
    };
    let address = node_service_address(&data.config, false);
    let result = match path {
        "" => home(&address, chain_id).await,
        "block" => {
            let hash = args.iter().find_map(|(k, v)| {
                if k == "block" {
                    Some(CryptoHash::from_str(v).unwrap())
                } else {
                    None
                }
            });
            block(&address, chain_id, hash).await
        }
        "blocks" => blocks(&address, chain_id, None, Some(20)).await,
        "applications" => applications(&address, chain_id).await,
        "error" => {
            let msg = args
                .iter()
                .find_map(|(k, v)| {
                    if k == "msg" {
                        Some(v.to_string())
                    } else {
                        None
                    }
                })
                .unwrap_or("unknown error".to_string());
            Err(msg)
        }
        _ => Err("unknown page".to_string()),
    };
    let (page, new_path) = result.unwrap_or_else(|e| error(&e));
    let page_js = format_bytes(&page.serialize(&SER).unwrap());
    setf(app, "page", &page_js);
    let new_path = format!("{}?chain={}", new_path, chain_id);
    web_sys::window()
        .expect("window object not found")
        .history()
        .expect("history object not found")
        .push_state_with_url(&page_js, &new_path, Some(&new_path))
        .expect("push_state failed");
}

#[wasm_bindgen]
pub async fn route(app: JsValue, path: JsValue, args: JsValue) {
    let path = path.as_string();
    let args = from_value::<Vec<(String, String)>>(args).unwrap_or(vec![]);
    let msg = format!(
        "route: {} {:?}",
        path.clone().unwrap_or("none".to_string()),
        args
    );
    log_str(&msg);
    let data = from_value::<Data>(app.clone()).expect("cannot parse vue data");
    let chain_id = args
        .iter()
        .find_map(|(k, v)| {
            if k == "chain" {
                Some(ChainId::from_str(v).expect("wrong chain id"))
            } else {
                None
            }
        })
        .unwrap_or(data.chain);
    route_aux(&app, &data, &path, &chain_id, &args).await
}

#[wasm_bindgen]
pub fn short(s: String) -> String {
    let n = s.len();
    format!("{}..{}", &s[..4], &s[n - 4..])
}

#[wasm_bindgen]
pub fn short_app(s: String) -> String {
    format!("{}..{}..{}..", &s[..4], &s[64..68], &s[152..156])
}

fn set_onpopstate(app: JsValue) {
    let callback = Closure::<dyn FnMut(JsValue)>::new(move |v: JsValue| {
        setf(&app, "page", &getf(&v, "state"));
    });
    web_sys::window()
        .expect("window object not found")
        .set_onpopstate(Some(callback.as_ref().unchecked_ref()));
    callback.forget()
}

/// Initialize pages and subscribe to notifications
#[wasm_bindgen]
pub async fn init(app: JsValue, uri: String) {
    console_error_panic_hook::set_once();
    set_onpopstate(app.clone());
    let data = from_value::<Data>(app.clone()).expect("cannot parse vue data");
    let address = node_service_address(&data.config, false);
    let default_chain = chains(&app, &address).await;
    match default_chain {
        Err(e) => {
            route_aux(
                &app,
                &data,
                &Some("error".to_string()),
                &data.chain,
                &[("msg".to_string(), e.to_string())],
            )
            .await;
        }
        Ok(default_chain) => {
            let uri = Url::parse(&uri).expect("failed to parse url");
            let pathname = uri.path();
            let mut queries = uri.query_pairs();
            let chain_id = queries
                .find_map(|(k, v)| {
                    if k == "chain" {
                        Some(ChainId::from_str(&v).expect("cannot build chainId"))
                    } else {
                        None
                    }
                })
                .unwrap_or(default_chain);
            let (path, args) = match pathname {
                "/blocks" => (Some("blocks".to_string()), vec![]),
                "/applications" => (Some("applications".to_string()), vec![]),
                pathname => {
                    let path = std::path::Path::new(pathname);
                    if path.starts_with("/block/") {
                        let hash = pathname[7..].to_string();
                        (Some("block".to_string()), vec![("block".to_string(), hash)])
                    } else if path.starts_with("/application/") {
                        let id = pathname[13..].to_string();
                        let link = format!("{}/applications/{}", address, id);
                        let app = serde_json::json!({"id": id, "link": link, "description": ""})
                            .to_string();
                        (
                            Some("application".to_string()),
                            vec![("app".to_string(), app)],
                        )
                    } else {
                        (None, vec![])
                    }
                }
            };
            route_aux(&app, &data, &path, &chain_id, &args).await;
        }
    }
}

#[wasm_bindgen]
pub fn save_config(app: JsValue) {
    let data = from_value::<Data>(app).expect("cannot parse vue data");
    if let Ok(Some(storage)) = web_sys::window()
        .expect("window object not found")
        .local_storage()
    {
        storage
            .set_item(
                "config",
                &serde_json::to_string::<Config>(&data.config)
                    .expect("cannot parse localstorage config"),
            )
            .expect("cannot set config");
    }
}
