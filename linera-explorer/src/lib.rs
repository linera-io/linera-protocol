use graphql_client::{reqwest::post_graphql, GraphQLQuery};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, RoundNumber, Timestamp},
    identifiers::{ChainId, Destination, Owner},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use serde_wasm_bindgen::to_value;
use wasm_bindgen::prelude::*;

type Epoch = Value;
type Message = Value;
type Operation = Value;
type Event = Value;
type Origin = Value;

#[derive(Serialize, Deserialize)]
enum Page {
    #[serde(rename = "home")]
    Home,
    #[serde(rename = "block")]
    Block(Box<block_query::BlockQueryBlock>),
}

#[derive(Serialize, Deserialize)]
pub struct Data {
    message: String,
    node: String,
    blocks: Vec<blocks_query::BlocksQueryBlocks>,
    path: String,
    page: Page,
    chains: Vec<ChainId>,
    chain: Option<ChainId>,
}

pub struct Context {
    chain: Option<ChainId>,
    node: String,
    path: String,
    page: Page,
}

#[wasm_bindgen(start)]
pub fn main_js() -> Result<(), JsValue> {
    Ok(())
}

#[wasm_bindgen]
pub fn data() -> JsValue {
    let data = Data {
        message: "Hello".to_string(),
        node: "http://localhost:8080".to_string(),
        blocks: Vec::new(),
        path: "".to_string(),
        page: Page::Home,
        chains: Vec::new(),
        chain: None,
    };
    to_value(&data).unwrap()
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/blocks_query.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct BlocksQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/block_query.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct BlockQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/chains_query.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct ChainsQuery;

pub fn setf(target: &JsValue, field: &str, value: &JsValue) {
    js_sys::Reflect::set(target, &JsValue::from_str(field), value)
        .unwrap_or_else(|_| panic!("failed to set '{}'", field));
}

pub fn getf(target: &JsValue, field: &str) -> JsValue {
    js_sys::Reflect::get(target, &JsValue::from_str(field))
        .unwrap_or_else(|_| panic!("failed to get '{}'", field))
}

pub fn log_str(s: &str) {
    web_sys::console::log_1(&JsValue::from_str(s))
}

pub fn set_default_chain(app: &JsValue, data: &Context, l: &[blocks_query::BlocksQueryBlocks]) {
    match (l.get(0), data.chain) {
        (None, _) | (_, Some(_)) => (),
        (Some(b), _) => {
            let js = serde_wasm_bindgen::to_value(&b.value.executed_block.block.chain_id).unwrap();
            setf(app, "chain", &js)
        }
    }
}

pub async fn get_blocks(app: &JsValue, data: &Context, from: &Option<CryptoHash>) {
    let variables = blocks_query::Variables {
        from: *from,
        chain_id: data.chain,
        limit: None,
    };
    let client = reqwest::Client::new();
    let res = post_graphql::<BlocksQuery, _>(&client, &data.node, variables)
        .await
        .unwrap();
    let blocks = res.data.unwrap().blocks;
    set_default_chain(app, data, &blocks);
    let res_js = serde_wasm_bindgen::to_value(&blocks).unwrap();
    setf(app, "blocks", &res_js);
}

pub async fn get_block(app: &JsValue, data: &Context, hash: &Option<CryptoHash>) {
    let variables = block_query::Variables {
        hash: *hash,
        chain_id: data.chain,
    };
    let client = reqwest::Client::new();
    let res = post_graphql::<BlockQuery, _>(&client, &data.node, variables)
        .await
        .unwrap();
    let b = res.data.unwrap().block.unwrap();
    let page_js = serde_wasm_bindgen::to_value(&Page::Block(Box::new(b))).unwrap();
    setf(app, "page", &page_js);
}

pub async fn get_chains(app: &JsValue, data: &Context) {
    let variables = chains_query::Variables;
    let client = reqwest::Client::new();
    let res = post_graphql::<ChainsQuery, _>(&client, &data.node, variables)
        .await
        .unwrap();
    let l = res.data.unwrap().chains;
    let chains_js = serde_wasm_bindgen::to_value(&l).unwrap();
    setf(app, "chains", &chains_js);
}

async fn route_aux(
    app: &JsValue,
    data: &Context,
    path: &Option<String>,
    args: Vec<(String, CryptoHash)>,
    _refresh: bool,
) {
    log_str(&format!("route: {}", path.clone().unwrap_or("none".to_string())));
    let path = path.clone().unwrap_or(data.path.clone());
    match path.as_str() {
        "" => {
            get_blocks(app, data, &None).await;
            get_chains(app, data).await;
            setf(app, "page", &JsValue::from_str("home"));
        }
        "block" => {
            let hash = args.into_iter().find(|(k, _)| k == "hash");
            match hash {
                None => log_str("no hash given"),
                Some((_, hash)) => get_block(app, data, &Some(hash)).await
            }
        }
        _ => (),
    };
    let path_js = JsValue::from_str(&path);
    setf(app, "path", &path_js);
    let state = js_sys::Object::new();
    setf(
        &state,
        "page",
        &serde_wasm_bindgen::to_value(&data.page).unwrap(),
    );
    setf(&state, "path", &path_js);
    setf(
        &state,
        "chain",
        &serde_wasm_bindgen::to_value(&data.chain).unwrap(),
    );
    web_sys::window()
        .unwrap()
        .history()
        .unwrap()
        .push_state(&state, &path)
        .expect("failed pushstate");
}

fn context_from_app(app: &JsValue) -> Context {
    Context {
        chain: serde_wasm_bindgen::from_value::<Option<ChainId>>(getf(app, "chain")).unwrap(),
        node: serde_wasm_bindgen::from_value::<String>(getf(app, "node")).unwrap(),
        path: serde_wasm_bindgen::from_value::<String>(getf(app, "path")).unwrap(),
        page: serde_wasm_bindgen::from_value::<Page>(getf(app, "page")).unwrap(),
    }
}

#[wasm_bindgen]
pub async fn route(app: JsValue, path: JsValue, args: JsValue, refresh: Option<bool>) {
    let path = path.as_string();
    let context = context_from_app(&app);
    let args = serde_wasm_bindgen::from_value::<Vec<(String, CryptoHash)>>(args).unwrap_or(vec![]);
    route_aux(&app, &context, &path, args, refresh.unwrap_or(true)).await
}

#[wasm_bindgen]
pub fn short(s: String) -> String {
    let n = s.len();
    format!("{}..{}", &s[..4], &s[n - 4..])
}

fn set_onpopstate(app: JsValue) {
    let a = Closure::<dyn FnMut(JsValue)>::new(move |e: JsValue| {
        let data_js = getf(&e, "state");
        setf(&app, "page", &getf(&data_js, "page"));
        setf(&app, "path", &getf(&data_js, "path"));
        setf(&app, "chain", &getf(&data_js, "chain"));
    });
    web_sys::window()
        .unwrap()
        .set_onpopstate(Some(a.as_ref().unchecked_ref()));
    a.forget()
}

#[wasm_bindgen]
pub async fn init(app: JsValue) {
    console_error_panic_hook::set_once();
    set_onpopstate(app.clone());
    let context = context_from_app(&app);
    route_aux(&app, &context, &None, Vec::new(), true).await
}
