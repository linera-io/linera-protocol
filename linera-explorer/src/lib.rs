use graphql_client::{reqwest::post_graphql, GraphQLQuery, Response};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{ChainId, Destination, Owner},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use serde_wasm_bindgen::{from_value, Serializer};
use wasm_bindgen::prelude::*;
use ws_stream_wasm::*;
use futures::prelude::*;
use uuid::Uuid;
use wasm_bindgen_futures::spawn_local;
// use pharos::{ObserveConfig, Observable};
use std::str::FromStr;

type Epoch = Value;
type Message = Value;
type Operation = Value;
type Event = Value;
type Origin = Value;

#[derive(Serialize, Deserialize, Clone)]
enum Page {
    #[serde(rename = "unloaded")]
    Unloaded,
    #[serde(rename = "blocks")]
    Blocks {
        chain: ChainId,
        blocks: Vec<blocks_query::BlocksQueryBlocks>,
    },
    #[serde(rename = "block")]
    Block(Box<block_query::BlockQueryBlock>),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    node: String,
    page: Page,
    chains: Vec<ChainId>,
}

#[derive(Serialize, Deserialize)]
pub struct GQuery<T> {
    id: Option<String>,
    #[serde(rename = "type")]
    typ: String,
    payload: Option<T>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Notification {
    pub chain_id: ChainId,
    pub reason: Reason,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Reason {
    NewBlock {
        height: BlockHeight,
        hash: CryptoHash,
    },
    NewIncomingMessage {
        origin: Origin,
        height: BlockHeight,
    },
}

const SER : Serializer = serde_wasm_bindgen::Serializer::json_compatible().serialize_bytes_as_arrays(true);

#[wasm_bindgen(start)]
pub fn main_js() -> Result<(), JsValue> {
    Ok(())
}

#[wasm_bindgen]
pub fn data() -> JsValue {
    let data = Data {
        node: "http://localhost:8080".to_string(),
        page: Page::Unloaded,
        chains: Vec::new(),
    };
    data.serialize(&SER).unwrap()
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/blocks_query.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct BlocksQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/block_query.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct BlockQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/chains_query.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct ChainsQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/notifications.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct NotificationsSubscription;

fn setf(target: &JsValue, field: &str, value: &JsValue) {
    js_sys::Reflect::set(target, &JsValue::from_str(field), value)
        .unwrap_or_else(|_| panic!("failed to set '{}'", field));
}

fn getf(target: &JsValue, field: &str) -> JsValue {
    js_sys::Reflect::get(target, &JsValue::from_str(field))
        .unwrap_or_else(|_| panic!("failed to get '{}'", field))
}

fn log_str(s: &str) {
    web_sys::console::log_1(&JsValue::from_str(s))
}

fn default_chain(chain: Option<ChainId>, l: &Vec<blocks_query::BlocksQueryBlocks>) -> ChainId {
    match (chain, l.get(0)) {
        (Some(id), _) => id,
        (_, Some(b)) => b.value.executed_block.block.chain_id,
        _ => ChainId::from_str("").unwrap(),
    }
}

async fn blocks(node: &str, chain_id: Option<ChainId>, from: Option<CryptoHash>) -> Page {
    let client = reqwest::Client::new();
    let variables = blocks_query::Variables { from, chain_id, limit: None };
    let res = post_graphql::<BlocksQuery, _>(&client, node, variables).await.unwrap();
    let blocks = res.data.unwrap().blocks;
    let chain = default_chain(chain_id, &blocks);
    Page::Blocks { chain, blocks }
}

async fn block(node: &str, chain_id: Option<ChainId>, hash: Option<CryptoHash>) -> Page {
    let client = reqwest::Client::new();
    let variables = block_query::Variables { hash, chain_id };    
    let res = post_graphql::<BlockQuery, _>(&client, node, variables).await.unwrap();
    let block = res.data.unwrap().block.unwrap();
    Page::Block(Box::new(block))
}

async fn chains(app: &JsValue, node: &str) {
    let client = reqwest::Client::new();
    let variables = chains_query::Variables;    
    let res = post_graphql::<ChainsQuery, _>(&client, node, variables).await.unwrap();
    let l = res.data.unwrap().chains;
    let chains_js = l.serialize(&SER).unwrap();
    setf(app, "chains", &chains_js);
}

async fn route_aux(app: &JsValue, data: &Data, path: &Option<String>, args: Vec<(String, String)>) {
    let path = match (path, data.page.clone()) {
        (Some(p), _) => p,
        (_, Page::Unloaded) => "",
        (_, Page::Block(_)) => "block",
        (_, Page::Blocks {..}) => "blocks",
    };
    let chain_id = args.iter().find(|(k, _)| k == "chain").map(|x| ChainId::from_str(&x.1).unwrap());
    let page = match path {
        "" => blocks(&data.node, None, None).await,
        "block" => {
            let hash = args.into_iter().find(|(k, _)| k == "block").map(|x| CryptoHash::from_str(&x.1).unwrap());
            block(&data.node, chain_id, hash).await
        },
        "blocks" => {
            blocks(&data.node, chain_id, None).await
        },
        _ => Page::Unloaded,
    };
    let page_js = page.serialize(&SER).unwrap();
    setf(app, "page", &page_js);
    let page_jstr = js_sys::JSON::stringify(&page_js).expect("failed page stringify");
    web_sys::window().unwrap().history().unwrap().push_state(&page_jstr.into(), &path).expect("push_state failed");
}

#[wasm_bindgen]
pub async fn route(app: JsValue, path: JsValue, args: JsValue) {    
    let path = path.as_string();
    let args = from_value::<Vec<(String, String)>>(args).unwrap_or(vec![]);
    log_str(&format!("route: {} {:?}", path.clone().unwrap_or("none".to_string()), args));
    let data = from_value::<Data>(app.clone()).unwrap();    
    route_aux(&app, &data, &path, args).await
}

#[wasm_bindgen]
pub fn short(s: String) -> String {
    let n = s.len();
    format!("{}..{}", &s[..4], &s[n - 4..])
}

fn set_onpopstate(app: JsValue) {
    let a = Closure::<dyn FnMut(JsValue)>::new(move |e: JsValue| {
        let page_str = getf(&e, "state").as_string().unwrap();
        let page = js_sys::JSON::parse(&page_str).expect("failed parse page");
        setf(&app, "page", &page);
    });
    web_sys::window().unwrap().set_onpopstate(Some(a.as_ref().unchecked_ref()));
    a.forget()
}

fn page_chain(data: &Data) -> Option<ChainId> {
    match &data.page {
        Page::Unloaded => None,
        Page::Block(b) => Some(b.value.executed_block.block.chain_id),
        Page::Blocks { blocks, ..} => blocks.get(0).map(|b| b.value.executed_block.block.chain_id)
    }
}


async fn subscribe(app: JsValue) {
    spawn_local( async move {
        let (_ws, mut wsio) = WsMeta::connect( "ws://localhost:8080/ws", Some(vec!("graphql-transport-ws")) ).await.unwrap();
        wsio.send(WsMessage::Text("{\"type\": \"connection_init\", \"payload\": {}}".to_string())).await.unwrap();
        wsio.next().await;
        let uuid = Uuid::new_v3(&Uuid::NAMESPACE_DNS, b"linera.dev");
        let query = format!("{{\"id\": \"{}\", \"type\": \"subscribe\", \"payload\": {{\"query\": \"subscription {{ notifications }}\"}}}}", uuid);
        wsio.send(WsMessage::Text(query)).await.unwrap();
        while let Some( evt ) = wsio.next().await {
            match evt {
                WsMessage::Text(s) => {
                    let gq = serde_json::from_str::<GQuery<Response<notifications_subscription::ResponseData>>>(&s).unwrap();
                    if let Some(p) = gq.payload {
                        if let Some(d) = p.data {
                            let data = from_value::<Data>(app.clone()).unwrap();
                            let chain = page_chain(&data);
                            match (Some(d.notifications.chain_id) == chain, d.notifications.reason) {
                                (true, Reason::NewBlock { hash: _hash, .. }) => {
                                    route_aux(&app, &data, &None, vec!()).await
                                },
                                _ => ()
                            }
                        }
                    };
                },
                WsMessage::Binary(_) => ()
            }
        }
    })
}

#[wasm_bindgen]
pub async fn init(app: JsValue) {
    console_error_panic_hook::set_once();
    set_onpopstate(app.clone());
    let data = from_value::<Data>(app.clone()).unwrap();
    chains(&app, &data.node).await;
    route_aux(&app, &data, &None, Vec::new()).await;
    subscribe(app.clone()).await;
}
