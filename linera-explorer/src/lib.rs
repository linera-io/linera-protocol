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

type Epoch = Value;
type Message = Value;
type Operation = Value;
type Event = Value;
type Origin = Value;

#[derive(Serialize, Deserialize, Clone)]
enum Page {
    #[serde(rename = "home")]
    Home,
    #[serde(rename = "block")]
    Block(Box<block_query::BlockQueryBlock>),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    message: String,
    node: String,
    blocks: Vec<blocks_query::BlocksQueryBlocks>,
    path: String,
    page: Page,
    chains: Vec<ChainId>,
    chain: Option<ChainId>,
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



const SER : Serializer = serde_wasm_bindgen::Serializer::json_compatible();

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

fn set_default_chain(app: &JsValue, data: &Data, l: &[blocks_query::BlocksQueryBlocks]) {
    match (l.get(0), data.chain) {
        (None, _) | (_, Some(_)) => (),
        (Some(b), _) => {
            let js = b.value.executed_block.block.chain_id.serialize(&SER).unwrap();
            setf(app, "chain", &js)
        }
    }
}

pub async fn get_blocks(app: &JsValue, data: &Data, from: &Option<CryptoHash>) {
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
    if let Ok(res_js) = blocks.serialize(&SER) {
        setf(app, "blocks", &res_js)
    }
}

async fn get_block(app: &JsValue, data: &Data, hash: &Option<CryptoHash>) {
    let variables = block_query::Variables {
        hash: *hash,
        chain_id: data.chain,
    };
    let client = reqwest::Client::new();
    let res = post_graphql::<BlockQuery, _>(&client, &data.node, variables)
        .await
        .unwrap();
    let b = res.data.unwrap().block.unwrap();
    if let Ok(page_js) = Page::Block(Box::new(b)).serialize(&SER) {
        setf(app, "page", &page_js)
    }
}

async fn get_chains(app: &JsValue, data: &Data) {
    let variables = chains_query::Variables;
    let client = reqwest::Client::new();
    let res = post_graphql::<ChainsQuery, _>(&client, &data.node, variables)
        .await
        .unwrap();
    let l = res.data.unwrap().chains;
    let chains_js = l.serialize(&SER).unwrap();
    setf(app, "chains", &chains_js);
}

async fn route_aux(
    app: &JsValue,
    data: &Data,
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
        &data.page.serialize(&SER).unwrap(),
    );
    setf(&state, "path", &path_js);
    setf(
        &state,
        "chain",
        &data.chain.serialize(&SER).unwrap(),
    );
    web_sys::window()
        .unwrap()
        .history()
        .unwrap()
        .push_state(&state, &path)
        .expect("failed pushstate");
}

#[wasm_bindgen]
pub async fn route(app: JsValue, path: JsValue, args: JsValue, refresh: Option<bool>) {
    let path = path.as_string();
    log_str(&format!("route: {}", path.clone().unwrap_or("none".to_string())));
    let data = from_value::<Data>(app.clone()).unwrap();
    let args = from_value::<Vec<(String, CryptoHash)>>(args).unwrap_or(vec![]);
    route_aux(&app, &data, &path, args, refresh.unwrap_or(true)).await
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
                            match (Some(d.notifications.chain_id) == data.chain, d.notifications.reason) {
                                (true, Reason::NewBlock { hash: _hash, .. }) => {
                                    route_aux(&app, &data, &None, vec!(), false).await
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
    route_aux(&app, &data, &None, Vec::new(), true).await;
    subscribe(app.clone()).await;
}
