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
use std::str::FromStr;
// use graphql_introspection_query::introspection_response::IntrospectionResponse;

type Epoch = Value;
type Message = Value;
type Operation = Value;
type Event = Value;
type Origin = Value;
type UserApplicationDescription = Value;
type ApplicationId = String;

#[derive(Serialize, Deserialize, Clone)]
enum Page {
    #[serde(rename = "unloaded")]
    Unloaded,
    #[serde(rename = "home")]
    Home {
        chain: ChainId,
        blocks: Vec<blocks_query::BlocksQueryBlocks>,
        apps: Vec<applications::ApplicationsApplications>,
    },
    #[serde(rename = "blocks")]
    Blocks {
        chain: ChainId,
        blocks: Vec<blocks_query::BlocksQueryBlocks>,
    },
    #[serde(rename = "block")]
    Block(Box<block_query::BlockQueryBlock>),
    #[serde(rename = "applications")]
    Applications(Vec<applications::ApplicationsApplications>),
    #[serde(rename = "application")]
    Application {
        app: applications::ApplicationsApplications,
        introspection: Value,
    },
    #[serde(rename = "error")]
    Error(String)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    node: String,
    tls: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    config: Config,
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

fn load_config() -> Config {
    let default = Config {
        node: "localhost:8080".to_string(),
        tls: false,
    };
    match web_sys::window().unwrap().local_storage() {
        Ok(Some(st)) => {
            match st.get_item("config") {
                Ok(Some(s)) => {
                    serde_json::from_str::<Config>(&s).unwrap_or(default)
                }
                _ => default
            }
        },
        _ => default
    }
}


#[wasm_bindgen]
pub fn data() -> JsValue {
    let data = Data {
        config: load_config(),
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

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/applications.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Applications;

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
        _ => ChainId::from_str("e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65").unwrap(),
    }
}

fn data_address(c: &Config, ws: bool) -> String {
    let proto = if ws { "ws" } else { "http" };
    let tls = if c.tls { "s" } else { "" };
    format!("{}{}://{}", proto, tls, c.node)
}

async fn get_blocks(node: &str, chain_id: Option<ChainId>, from: Option<CryptoHash>, limit: Option<u32>) -> Vec<blocks_query::BlocksQueryBlocks> {
    let client = reqwest::Client::new();
    let variables = blocks_query::Variables { from, chain_id, limit: limit.map(|x| x.into()) };
    let res = post_graphql::<BlocksQuery, _>(&client, node, variables).await.unwrap();
    res.data.unwrap().blocks
}

async fn get_applications(node: &str, chain_id: Option<ChainId>) -> Vec<applications::ApplicationsApplications> {
    let client = reqwest::Client::new();
    let variables = applications::Variables { chain_id };
    let res = post_graphql::<Applications, _>(&client, node, variables).await.unwrap();
    res.data.unwrap().applications
}

async fn home(node: &str, chain_id: Option<ChainId>) -> Page {
    let blocks = get_blocks(node, chain_id, None, None).await;
    let chain = default_chain(chain_id, &blocks);
    let apps = get_applications(node, chain_id).await;
    Page::Home { blocks, chain, apps }
}

async fn blocks(node: &str, chain_id: Option<ChainId>, from: Option<CryptoHash>, limit: Option<u32>) -> Page {
    let blocks = get_blocks(node, chain_id, from, limit).await;
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

async fn applications(node: &str, chain_id: Option<ChainId>) -> Page {
    let applications = get_applications(node, chain_id).await;
    Page::Applications(applications)
}

async fn introspection(url: &str) -> Value {
    let client = reqwest::Client::new();
    let res = client.post(url).body("{\"query\":\"query IntrospectionQuery {__schema{queryType{name}mutationType{name}subscriptionType{name}types{...FullType}directives{name description locations args{...InputValue}}}}fragment FullType on __Type{kind name description fields(includeDeprecated:true){name description args{...InputValue}type{...TypeRef}isDeprecated deprecationReason}inputFields{...InputValue}interfaces{...TypeRef}enumValues(includeDeprecated:true){name description isDeprecated deprecationReason}possibleTypes{...TypeRef}}fragment InputValue on __InputValue{name description type{...TypeRef}defaultValue}fragment TypeRef on __Type{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name}}}}}}}}\", \"operationName\":\"IntrospectionQuery\"}").send().await.unwrap().text().await.unwrap();
    serde_json::from_str(&res).unwrap()
}

async fn application(app: applications::ApplicationsApplications) -> Page {
    let introspection = introspection(&app.link).await;
    Page::Application { app, introspection }
}

fn format_bytes(v: &JsValue) -> JsValue {
    let vf = v.clone();
    match js_sys::Object::try_from(v) {
        None => vf,
        Some(o) => {
            js_sys::Object::keys(o).iter().for_each(|k : JsValue| match k.as_string() {
                None => (),
                Some(kstr) => {
                    if kstr == "bytes".to_string() {
                        let a = js_sys::Array::from(&getf(&vf, "bytes"));
                        // if a.length() > 64 {
                        let av = a.to_vec();
                        let av2 : Vec<u8> = av.iter().map(|x| x.as_f64().unwrap() as u8).collect();
                        setf(&vf, "bytes", &JsValue::from_str(&hex::encode(av2)))
                        // }
                    } else {
                        setf(&vf, &kstr, &format_bytes(&getf(&vf, &kstr)))
                    }
                }
            });
            vf
        }
    }
}

async fn route_aux(app: &JsValue, data: &Data, path: &Option<String>, args: Vec<(String, String)>) {
    let path = match (path, data.page.clone()) {
        (Some(p), _) => p,
        (_, Page::Unloaded | Page::Home {..} ) => "",
        (_, Page::Block(_)) => "block",
        (_, Page::Blocks {..}) => "blocks",
        (_, Page::Applications(_)) => "applications",
        (_, Page::Application {..}) => "application",
        (_, Page::Error(_)) => "error",
    };
    let chain_id = args.iter().find(|(k, _)| k == "chain").map(|x| ChainId::from_str(&x.1).unwrap());
    let address = data_address(&data.config, false);
    let page = match path {
        "" => home(&address, chain_id).await,
        "block" => {
            let hash = args.into_iter().find(|(k, _)| k == "block").map(|x| CryptoHash::from_str(&x.1).unwrap());
            block(&address, chain_id, hash).await
        },
        "blocks" => blocks(&address, chain_id, None, Some(20)).await,
        "applications" => applications(&address, chain_id).await,
        "application" => {
            match args.into_iter().find(|(k, _)| k == "app").map(|x| js_sys::JSON::parse(&x.1).unwrap()) {
                None => Page::Error("no application given".to_string()),
                Some(js) => {
                    let app = from_value::<applications::ApplicationsApplications>(js).unwrap();
                    application(app).await
                }
            }
        }
        _ => Page::Unloaded,
    };
    let page_js = format_bytes(&page.serialize(&SER).unwrap());
    setf(app, "page", &page_js);
    web_sys::window().unwrap().history().unwrap().push_state(&page_js, &path).expect("push_state failed");
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

#[wasm_bindgen]
pub fn short_app(s: String) -> String {
    format!("{}..{}..{}..", &s[..4], &s[64..68], &s[152..156])
}

fn set_onpopstate(app: JsValue) {
    let a = Closure::<dyn FnMut(JsValue)>::new(move |e: JsValue| {
        setf(&app, "page", &getf(&e, "state"));
    });
    web_sys::window().unwrap().set_onpopstate(Some(a.as_ref().unchecked_ref()));
    a.forget()
}

fn application_chain(a: &applications::ApplicationsApplications) -> ChainId {
    ChainId::from_str(a.description.as_object().unwrap().get("creation").unwrap().as_object().unwrap().get("chain_id").unwrap().as_str().unwrap()).unwrap()
}

fn page_chain(data: &Data) -> Option<ChainId> {
    match &data.page {
        Page::Block(b) => Some(b.value.executed_block.block.chain_id),
        Page::Blocks { chain, .. } => Some(*chain),
        Page::Applications(l) => l.get(0).map(|a| application_chain(&a)),
        Page::Application {app, ..} => Some(application_chain(&app)),
        _ => None,
    }
}

async fn subscribe(app: JsValue) {
    spawn_local( async move {
        let data = from_value::<Data>(app.clone()).unwrap();
        let address = data_address(&data.config, true);
        let (_ws, mut wsio) = WsMeta::connect( &format!("{}/ws", address), Some(vec!("graphql-transport-ws")) ).await.unwrap();
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
    let address = data_address(&data.config, false);
    chains(&app, &address).await;
    route_aux(&app, &data, &None, Vec::new()).await;
    subscribe(app.clone()).await;
}

#[wasm_bindgen]
pub fn save_config(app: JsValue) {
    let data = from_value::<Data>(app).unwrap();
    if let Ok(Some(st)) = web_sys::window().unwrap().local_storage() {
        st.set_item("config", &serde_json::to_string::<Config>(&data.config).unwrap()).unwrap();
    }
}
