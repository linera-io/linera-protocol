// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides web files to run a block explorer from linera service node.

mod graphql;
mod js_utils;

use graphql_client::reqwest::post_graphql;
use linera_base::{
    crypto::CryptoHash,
    identifiers::{ChainDescription, ChainId},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_wasm_bindgen::from_value;
use std::str::FromStr;
use url::Url;
use wasm_bindgen::prelude::*;

use graphql::{
    applications::ApplicationsApplications as Applications, block::BlockBlock as Block,
    blocks::BlocksBlocks as Blocks, Chains,
};
use js_utils::{getf, log_str, setf, SER};

/// Page enum containing info for each page
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
enum Page {
    Unloaded,
    Home {
        blocks: Vec<Blocks>,
        apps: Vec<Applications>,
    },
    Blocks(Vec<Blocks>),
    Block(Box<Block>),
    Applications(Vec<Applications>),
    Error(String),
}

/// Config type dealt with localstorage
#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    node: String,
    tls: bool,
}

impl Config {
    /// Load config from local storage
    fn load() -> Self {
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
}

/// type for Vue data
#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    config: Config,
    page: Page,
    chains: Vec<Value>,
    chain: ChainId,
}

/// Initializes Vue data
#[wasm_bindgen]
pub fn data() -> JsValue {
    let data = Data {
        config: Config::load(),
        page: Page::Unloaded,
        chains: Vec::new(),
        chain: ChainId::from(ChainDescription::Root(0)),
    };
    data.serialize(&SER).unwrap()
}

pub enum Protocol {
    Http,
    Websocket,
}

fn node_service_address(config: &Config, protocol: Protocol) -> String {
    let protocol = match protocol {
        Protocol::Http => "http",
        Protocol::Websocket => "ws",
    };
    let tls = if config.tls { "s" } else { "" };
    format!("{}{}://{}", protocol, tls, config.node)
}

async fn get_blocks(
    node: &str,
    chain_id: &ChainId,
    from: Option<CryptoHash>,
    limit: Option<u32>,
) -> Result<Vec<Blocks>, String> {
    let client = reqwest::Client::new();
    let variables = graphql::blocks::Variables {
        from,
        chain_id: Some(*chain_id),
        limit: limit.map(|x| x.into()),
    };
    let res = post_graphql::<crate::graphql::Blocks, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    match res.data {
        None => Err("null blocks data".to_string()),
        Some(data) => Ok(data.blocks),
    }
}

async fn get_applications(node: &str, chain_id: &ChainId) -> Result<Vec<Applications>, String> {
    let client = reqwest::Client::new();
    let variables = graphql::applications::Variables {
        chain_id: Some(*chain_id),
    };
    let result = post_graphql::<crate::graphql::Applications, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    match result.data {
        None => Err("null applications data".to_string()),
        Some(data) => Ok(data.applications),
    }
}

fn error(msg: &str) -> (Page, String) {
    (Page::Error(msg.to_string()), "/error".to_string())
}

async fn home(node: &str, chain_id: &ChainId) -> Result<(Page, String), String> {
    let blocks = get_blocks(node, chain_id, None, None).await?;
    let apps = get_applications(node, chain_id).await?;
    Ok((Page::Home { blocks, apps }, format!("/?chain={}", chain_id)))
}

async fn blocks(
    node: &str,
    chain_id: &ChainId,
    from: Option<CryptoHash>,
    limit: Option<u32>,
) -> Result<(Page, String), String> {
    // TODO: limit is not used in the UI, it should be implemented with some path arguments and select input
    let blocks = get_blocks(node, chain_id, from, limit).await?;
    Ok((Page::Blocks(blocks), format!("/blocks?chain={}", chain_id)))
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
    let result = post_graphql::<crate::graphql::Block, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    let data = result.data.ok_or("no block data found".to_string())?;
    let block = data.block.ok_or("no block found".to_string())?;
    let hash = block.hash;
    Ok((
        Page::Block(Box::new(block)),
        format!("/block/{}?chain={}", hash, chain_id),
    ))
}

async fn chains(app: &JsValue, node: &str) -> Result<ChainId, String> {
    let client = reqwest::Client::new();
    let variables = graphql::chains::Variables;
    let result = post_graphql::<Chains, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    let chains = result.data.ok_or("no data in chains query")?.chains;
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
        format!("/applications?chain={}", chain_id),
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
                        let array: Vec<u8> =
                            js_sys::Uint8Array::from(getf(&modified_value, "bytes")).to_vec();
                        let array_hex = hex::encode(array);
                        let hex_len = array_hex.len();
                        let hex_elided = if hex_len > 128 {
                            // don't show all hex digits if the bytes array is too long
                            format!("{}..{}", &array_hex[0..4], &array_hex[hex_len - 4..])
                        } else {
                            array_hex
                        };
                        setf(&modified_value, "bytes", &JsValue::from_str(&hex_elided))
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

fn page_name_and_args(page: &Page) -> (&str, Vec<(String, String)>) {
    match page {
        Page::Unloaded | Page::Home { .. } => ("", Vec::new()),
        Page::Block(b) => ("block", vec![("block".to_string(), b.hash.to_string())]),
        Page::Blocks { .. } => ("blocks", Vec::new()),
        Page::Applications(_) => ("applications", Vec::new()),
        Page::Error(_) => ("error", Vec::new()),
    }
}

fn find_arg(args: &[(String, String)], key: &str) -> Option<String> {
    args.iter()
        .find_map(|(k, v)| if k == key { Some(v.clone()) } else { None })
}

fn chain_id_from_args(
    app: &JsValue,
    data: &Data,
    args: &[(String, String)],
) -> Result<ChainId, String> {
    match find_arg(args, "chain") {
        None => Ok(data.chain),
        Some(chain_id) => {
            let chain_js: JsValue = chain_id
                .serialize(&SER)
                .expect("failed to serialize ChainId");
            setf(app, "chain", &chain_js);
            ChainId::from_str(&chain_id).map_err(|e| e.to_string())
        }
    }
}

/// Main function to switch between vue.js pages
async fn route_aux(app: &JsValue, data: &Data, path: &Option<String>, args: &[(String, String)]) {
    let chain_id = chain_id_from_args(app, data, args);
    let (page_name, args): (&str, Vec<(String, String)>) = match (path, &data.page) {
        (Some(p), _) => (p, args.to_vec()),
        (_, p) => page_name_and_args(p),
    };
    let address = node_service_address(&data.config, Protocol::Http);
    let result = match chain_id {
        Err(e) => Err(e),
        Ok(chain_id) => match page_name {
            "" => home(&address, &chain_id).await,
            "block" => {
                let hash = find_arg(&args, "block")
                    .map(|hash| CryptoHash::from_str(&hash).map_err(|e| e.to_string()));
                match hash {
                    Some(Err(e)) => Err(e),
                    None => block(&address, &chain_id, None).await,
                    Some(Ok(hash)) => block(&address, &chain_id, Some(hash)).await,
                }
            }
            "blocks" => blocks(&address, &chain_id, None, Some(20)).await,
            "applications" => applications(&address, &chain_id).await,
            "error" => {
                let msg = find_arg(&args, "msg").unwrap_or("unknown error".to_string());
                Err(msg)
            }
            _ => Err("unknown page".to_string()),
        },
    };
    let (page, new_path) = result.unwrap_or_else(|e| error(&e));
    let page_js = format_bytes(&page.serialize(&SER).unwrap());
    setf(app, "page", &page_js);
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
    let args = from_value::<Vec<(String, String)>>(args).unwrap_or(Vec::new());
    let msg = format!("route: {} {:?}", path.as_deref().unwrap_or("none"), args);
    log_str(&msg);
    let data = from_value::<Data>(app.clone()).expect("cannot parse Vue data");
    route_aux(&app, &data, &path, &args).await
}

#[wasm_bindgen]
pub fn short_cryptohash(s: String) -> String {
    let n = s.len();
    format!("{}..{}", &s[..4], &s[n - 4..])
}

#[wasm_bindgen]
pub fn short_app_id(s: String) -> String {
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

/// Initializes pages and subscribes to notifications
#[wasm_bindgen]
pub async fn init(app: JsValue, uri: String) {
    console_error_panic_hook::set_once();
    set_onpopstate(app.clone());
    let data = from_value::<Data>(app.clone()).expect("cannot parse vue data");
    let address = node_service_address(&data.config, Protocol::Http);
    let default_chain = chains(&app, &address).await;
    match default_chain {
        Err(e) => {
            route_aux(
                &app,
                &data,
                &Some("error".to_string()),
                &[("msg".to_string(), e.to_string())],
            )
            .await
        }
        Ok(default_chain) => {
            let uri = Url::parse(&uri).expect("failed to parse url");
            let pathname = uri.path();
            let mut args: Vec<(String, String)> = uri.query_pairs().into_owned().collect();
            args.push(("chain".to_string(), default_chain.to_string()));
            let path = match pathname {
                "/blocks" => Some("blocks".to_string()),
                "/applications" => Some("applications".to_string()),
                pathname => match pathname.strip_prefix("/block/") {
                    Some(hash) => {
                        args.push(("block".to_string(), hash.to_string()));
                        Some("block".to_string())
                    }
                    _ => None,
                },
            };
            route_aux(&app, &data, &path, &args).await;
        }
    }
}

/// Save config to local storage
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
