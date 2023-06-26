use graphql_client::{reqwest::post_graphql, GraphQLQuery, Response};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{ChainId, Destination, Owner},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use futures::prelude::*;
use serde_wasm_bindgen::{from_value, Serializer};
use std::str::FromStr;
use uuid::Uuid;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use ws_stream_wasm::*;

mod graphql;
mod js_utils;

use js_utils::{getf, js_to_json, log_str, parse, setf};

type Epoch = Value;
type Message = Value;
type Operation = Value;
type Event = Value;
type Origin = Value;
type UserApplicationDescription = Value;
type ApplicationId = String;

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
enum Page {
    Unloaded,
    Home {
        chain: ChainId,
        blocks: Vec<blocks_query::BlocksQueryBlocks>,
        apps: Vec<applications::ApplicationsApplications>,
    },
    Blocks {
        chain: ChainId,
        blocks: Vec<blocks_query::BlocksQueryBlocks>,
    },
    Block(Box<block_query::BlockQueryBlock>),
    Applications(Vec<applications::ApplicationsApplications>),
    Application {
        app: applications::ApplicationsApplications,
        queries: Value,
        mutations: Value,
        subscriptions: Value,
    },
    Error(String),
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

const SER: Serializer =
    serde_wasm_bindgen::Serializer::json_compatible().serialize_bytes_as_arrays(true);

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

fn default_blocks_chain(chain: Option<ChainId>, l: &[blocks_query::BlocksQueryBlocks]) -> ChainId {
    match (chain, l.get(0)) {
        (Some(id), _) => id,
        (_, Some(b)) => b.value.executed_block.block.chain_id,
        _ => ChainId::from_str("e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65")
            .unwrap(),
    }
}

fn node_service_address(c: &Config, ws: bool) -> String {
    let proto = if ws { "ws" } else { "http" };
    let tls = if c.tls { "s" } else { "" };
    format!("{}{}://{}", proto, tls, c.node)
}

async fn get_blocks(
    node: &str,
    chain_id: Option<ChainId>,
    from: Option<CryptoHash>,
    limit: Option<u32>,
) -> Result<Vec<blocks_query::BlocksQueryBlocks>, String> {
    let client = reqwest::Client::new();
    let variables = blocks_query::Variables {
        from,
        chain_id,
        limit: limit.map(|x| x.into()),
    };
    let res = post_graphql::<BlocksQuery, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    res.data.map_or(Ok(Vec::new()), |d| Ok(d.blocks))
}

async fn get_applications(
    node: &str,
    chain_id: Option<ChainId>,
) -> Result<Vec<applications::ApplicationsApplications>, String> {
    let client = reqwest::Client::new();
    let variables = applications::Variables { chain_id };
    let res = post_graphql::<Applications, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    res.data.map_or(Ok(Vec::new()), |d| Ok(d.applications))
}

fn error(msg: &str) -> (Page, String) {
    (Page::Error(msg.to_string()), "/error".to_string())
}

async fn home(node: &str, chain_id: Option<ChainId>) -> Result<(Page, String), String> {
    let blocks = get_blocks(node, chain_id, None, None).await?;
    let chain = default_blocks_chain(chain_id, &blocks);
    let apps = get_applications(node, chain_id).await?;
    Ok((
        Page::Home {
            blocks,
            chain,
            apps,
        },
        "/".to_string(),
    ))
}

async fn blocks(
    node: &str,
    chain_id: Option<ChainId>,
    from: Option<CryptoHash>,
    limit: Option<u32>,
) -> Result<(Page, String), String> {
    let blocks = get_blocks(node, chain_id, from, limit).await?;
    let chain = default_blocks_chain(chain_id, &blocks);
    Ok((Page::Blocks { chain, blocks }, "/blocks".to_string()))
}

async fn block(
    node: &str,
    chain_id: Option<ChainId>,
    hash: Option<CryptoHash>,
) -> Result<(Page, String), String> {
    let client = reqwest::Client::new();
    let variables = block_query::Variables { hash, chain_id };
    let res = post_graphql::<BlockQuery, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    let data = res.data.ok_or("no block data found".to_string())?;
    let block = data.block.ok_or("no block found".to_string())?;
    let hash = block.hash;
    Ok((Page::Block(Box::new(block)), format!("/block/{}", hash)))
}

async fn chains(app: &JsValue, node: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let variables = chains_query::Variables;
    let res = post_graphql::<ChainsQuery, _>(&client, node, variables)
        .await
        .map_err(|e| e.to_string())?;
    let l = res.data.unwrap().chains;
    let chains_js = l.serialize(&SER).unwrap();
    setf(app, "chains", &chains_js);
    Ok(())
}

async fn applications(node: &str, chain_id: Option<ChainId>) -> Result<(Page, String), String> {
    let applications = get_applications(node, chain_id).await?;
    Ok((
        Page::Applications(applications),
        "/applications".to_string(),
    ))
}

fn list_entrypoints(types: &[Value], name: &Value) -> Option<Value> {
    types
        .iter()
        .find(|x: &&Value| &x["name"] == name)
        .map(|x| x["fields"].clone())
}

fn fill_type(t: &Value, types: &Vec<Value>) -> Value {
    match t {
        Value::Array(l) => Value::Array(l.iter().map(|x: &Value| fill_type(x, types)).collect()),
        Value::Object(m) => {
            let mut m = m.clone();
            let name = t["name"].as_str();
            let kind = t["kind"].as_str();
            let of_type = &t["ofType"];
            match (kind, name, of_type) {
                (Some("OBJECT"), Some(name), _) => {
                    match types.iter().find(|x: &&Value| x["name"] == name) {
                        None => (),
                        Some(t2) => {
                            let fields: Vec<Value> = t2["fields"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|x| fill_type(x, types))
                                .collect();
                            m.insert("fields".to_string(), Value::Array(fields));
                        }
                    }
                }
                (Some("INPUT_OBJECT"), Some(name), _) => {
                    match types.iter().find(|x: &&Value| x["name"] == name) {
                        None => (),
                        Some(t2) => {
                            let fields: Vec<Value> = t2["inputfields"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|x| fill_type(x, types))
                                .collect();
                            m.insert("inputfields".to_string(), Value::Array(fields));
                        }
                    }
                }
                (Some("LIST" | "NON_NULL"), Some(name), Value::Null) => {
                    match types.iter().find(|x: &&Value| x["name"] == name) {
                        None => (),
                        Some(t2) => {
                            m.insert("ofType".to_string(), fill_type(t2, types));
                        }
                    }
                }
                _ => (),
            };
            m.insert("ofType".to_string(), fill_type(&t["ofType"], types));
            m.insert("type".to_string(), fill_type(&t["type"], types));
            Value::Object(m)
        }
        t => t.clone(),
    }
}

async fn application(
    app: applications::ApplicationsApplications,
) -> Result<(Page, String), String> {
    let schema = graphql::introspection(&app.link).await?;
    let sch = &schema["data"]["__schema"];
    let types = sch["types"]
        .as_array()
        .expect("introspection types is not an array")
        .clone();
    let queries =
        list_entrypoints(&types, &sch["queryType"]["name"]).unwrap_or(Value::Array(Vec::new()));
    let queries = fill_type(&queries, &types);
    let mutations =
        list_entrypoints(&types, &sch["mutationType"]["name"]).unwrap_or(Value::Array(Vec::new()));
    let mutations = fill_type(&mutations, &types);
    let subscriptions = list_entrypoints(&types, &sch["subscriptionType"]["name"])
        .unwrap_or(Value::Array(Vec::new()));
    let subscriptions = fill_type(&subscriptions, &types);
    let pathname = format!("/application/{}", app.id);
    Ok((
        Page::Application {
            app,
            queries,
            mutations,
            subscriptions,
        },
        pathname,
    ))
}

fn format_bytes(v: &JsValue) -> JsValue {
    let vf = v.clone();
    if let Some(o) = js_sys::Object::try_from(v) {
        js_sys::Object::keys(o)
            .iter()
            .for_each(|k: JsValue| match k.as_string() {
                None => (),
                Some(kstr) => {
                    if &kstr == "bytes" {
                        let a = js_sys::Array::from(&getf(&vf, "bytes"));
                        let av = a.to_vec();
                        let av2: Vec<u8> = av
                            .iter()
                            .map(|x| x.as_f64().expect("byte not a u8") as u8)
                            .collect();
                        let h = hex::encode(av2);
                        let n = h.len();
                        let h = if n > 128 {
                            // don't show all hexa if too long
                            format!("{}..{}", &h[0..4], &h[n - 4..])
                        } else {
                            h
                        };
                        setf(&vf, "bytes", &JsValue::from_str(&h))
                    } else {
                        setf(&vf, &kstr, &format_bytes(&getf(&vf, &kstr)))
                    }
                }
            });
    };
    vf
}

async fn route_aux(app: &JsValue, data: &Data, path: &Option<String>, args: &[(String, String)]) {
    let path = match (path, data.page.clone()) {
        (Some(p), _) => p,
        (_, Page::Unloaded | Page::Home { .. }) => "",
        (_, Page::Block(_)) => "block",
        (_, Page::Blocks { .. }) => "blocks",
        (_, Page::Applications(_)) => "applications",
        (_, Page::Application { .. }) => "application",
        (_, Page::Error(_)) => "error",
    };
    let chain_id = args
        .iter()
        .find(|(k, _)| k == "chain")
        .map(|x| ChainId::from_str(&x.1).expect("wrong chain id"));
    let chain_id = match data.page.clone() {
        Page::Home { chain, .. } => Some(chain_id.unwrap_or(chain)),
        Page::Blocks { chain, .. } => Some(chain_id.unwrap_or(chain)),
        _ => chain_id,
    };
    let address = node_service_address(&data.config, false);
    let res = match path {
        "" => home(&address, chain_id).await,
        "block" => {
            let hash = args
                .iter()
                .find(|(k, _)| k == "block")
                .map(|x| CryptoHash::from_str(&x.1).unwrap());
            block(&address, chain_id, hash).await
        }
        "blocks" => blocks(&address, chain_id, None, Some(20)).await,
        "applications" => applications(&address, chain_id).await,
        "application" => match args.iter().find(|(k, _)| k == "app").map(|x| parse(&x.1)) {
            None => Err("unknown application".to_string()),
            Some(js) => {
                let app = from_value::<applications::ApplicationsApplications>(js).unwrap();
                application(app).await
            }
        },
        _ => Err("unknown page".to_string()),
    };
    let (page, new_path) = res.unwrap_or_else(|e| error(&e));
    let page_js = format_bytes(&page.serialize(&SER).unwrap());
    setf(app, "page", &page_js);
    let new_path = match chain_id {
        None => new_path,
        Some(id) => format!("{}?chain={}", new_path, id),
    };
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
    route_aux(&app, &data, &path, &args).await
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
    web_sys::window()
        .expect("window object not found")
        .set_onpopstate(Some(a.as_ref().unchecked_ref()));
    a.forget()
}

fn application_chain(a: &applications::ApplicationsApplications) -> Option<ChainId> {
    a.description["creation"]["chain_id"]
        .as_str()
        .map(|id| ChainId::from_str(id).expect("cannot build chainID from string"))
}

fn page_chain(data: &Data) -> Option<ChainId> {
    match &data.page {
        Page::Block(b) => Some(b.value.executed_block.block.chain_id),
        Page::Blocks { chain, .. } => Some(*chain),
        Page::Applications(l) => match l.get(0) {
            None => None,
            Some(a) => application_chain(a),
        },
        Page::Application { app, .. } => application_chain(app),
        _ => None,
    }
}

async fn subscribe(app: JsValue, args: Vec<(String, String)>) {
    spawn_local(async move {
        let data = from_value::<Data>(app.clone()).expect("cannot parse vue data");
        let address = node_service_address(&data.config, true);
        let (_ws, mut wsio) = WsMeta::connect(
            &format!("{}/ws", address),
            Some(vec!["graphql-transport-ws"]),
        )
        .await
        .expect("cannot connect to websocket");
        wsio.send(WsMessage::Text(
            "{\"type\": \"connection_init\", \"payload\": {}}".to_string(),
        ))
        .await
        .expect("cannot send to websocket");
        wsio.next().await;
        let uuid = Uuid::new_v3(&Uuid::NAMESPACE_DNS, b"linera.dev");
        let query = format!("{{\"id\": \"{}\", \"type\": \"subscribe\", \"payload\": {{\"query\": \"subscription {{ notifications }}\"}}}}", uuid);
        wsio.send(WsMessage::Text(query))
            .await
            .expect("cannot send to websocket");
        while let Some(evt) = wsio.next().await {
            match evt {
                WsMessage::Text(s) => {
                    let gq = serde_json::from_str::<
                        GQuery<Response<notifications_subscription::ResponseData>>,
                    >(&s)
                    .expect("unexpected websocket response");
                    if let Some(p) = gq.payload {
                        if let Some(d) = p.data {
                            let data =
                                from_value::<Data>(app.clone()).expect("cannot parse vue data");
                            let chain_id = args
                                .iter()
                                .find(|(k, _)| k == "chain")
                                .map(|x| ChainId::from_str(&x.1).expect("wrong chain id"));
                            let chain_id = match chain_id {
                                None => page_chain(&data),
                                _ => chain_id,
                            };
                            if let (true, Reason::NewBlock { hash: _hash, .. }) = (
                                Some(d.notifications.chain_id) == chain_id,
                                d.notifications.reason,
                            ) {
                                route_aux(&app, &data, &None, &Vec::new()).await
                            };
                        }
                    };
                }
                WsMessage::Binary(_) => (),
            }
        }
    })
}

#[wasm_bindgen]
pub async fn init(app: JsValue, pathname: String, search: String) {
    console_error_panic_hook::set_once();
    set_onpopstate(app.clone());
    let data = from_value::<Data>(app.clone()).expect("cannot parse vue data");
    let address = node_service_address(&data.config, false);
    let _ = chains(&app, &address).await;
    let (path, mut args) = match pathname.as_str() {
        "" => (Some("".to_string()), vec![]),
        "/blocks" => (Some("blocks".to_string()), vec![]),
        "/applications" => (Some("applications".to_string()), vec![]),
        s => {
            let p = std::path::Path::new(s);
            if p.starts_with("/block/") {
                let hash = s[7..].to_string();
                (Some("block".to_string()), vec![("block".to_string(), hash)])
            } else if p.starts_with("/application/") {
                let id = s[13..].to_string();
                let link = format!("{}/applications/{}", address, id);
                let app =
                    serde_json::json!({"id": id, "link": link, "description": ""}).to_string();
                (
                    Some("application".to_string()),
                    vec![("app".to_string(), app)],
                )
            } else {
                (None, vec![])
            }
        }
    };
    if search.len() > 7 && &search[..6] == "?chain" {
        args.push(("chain".to_string(), search[7..].to_string()));
    };
    route_aux(&app, &data, &path, &args).await;
    subscribe(app.clone(), args).await;
}

#[wasm_bindgen]
pub fn save_config(app: JsValue) {
    let data = from_value::<Data>(app).expect("cannot parse vue data");
    if let Ok(Some(st)) = web_sys::window()
        .expect("window object not found")
        .local_storage()
    {
        st.set_item(
            "config",
            &serde_json::to_string::<Config>(&data.config)
                .expect("cannot parse localstorage config"),
        )
        .expect("cannot set config");
    }
}

fn forge_arg_type(arg: &Value, non_null: bool) -> Option<String> {
    if arg["kind"] == serde_json::json!("SCALAR") {
        if non_null {
            Some(format!("{}", arg["_input"]))
        } else {
            arg.get("_input").map(|input| format!("{}", input))
        }
    } else if arg["kind"] == serde_json::json!("NON_NULL") {
        forge_arg_type(&arg["ofType"], true)
    } else {
        None
    }
}

fn forge_arg(arg: &Value) -> Option<String> {
    forge_arg_type(&arg["type"], false).map(|s| {
        format!(
            "{}: {}",
            arg["name"].as_str().expect("name is not a string"),
            s
        )
    })
}

fn forge_args(args: Vec<Value>) -> String {
    let args: Vec<String> = args.iter().filter_map(forge_arg).collect();
    if !args.is_empty() {
        format!("({})", args.join(","))
    } else {
        "".to_string()
    }
}

fn forge_response_type(t: &Value, name: Option<&Value>, root: bool) -> String {
    let is_non_null_or_list = matches!(t["kind"].as_str(), Some("NON_NULL") | Some("LIST"));
    let incl = matches!(t.get("_include"), Some(Value::Bool(true)));
    if !(incl || root || is_non_null_or_list) {
        "".to_string()
    } else {
        match t["kind"].as_str().unwrap() {
            "SCALAR" => name.unwrap_or(&t["name"]).as_str().unwrap().to_string(),
            "NON_NULL" | "LIST" => forge_response_type(&t["ofType"], name, root),
            "OBJECT" => {
                let fields: Vec<String> = t["fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x: &Value| forge_response_type(&x["type"], Some(&x["name"]), false))
                    .collect();
                if root {
                    format!("{{ {} }}", fields.join(" "))
                } else {
                    format!("{} {{ {} }}", t["name"].as_str().unwrap(), fields.join(" "))
                }
            }
            _ => "".to_string(),
        }
    }
}

fn forge_response(t: &Value) -> String {
    if empty_output_aux(t) {
        "".to_string()
    } else {
        forge_response_type(t, None, true)
    }
}

#[wasm_bindgen]
pub async fn query(app: JsValue, query: JsValue, kind: String) {
    let link =
        from_value::<String>(getf(&app, "link")).expect("cannot parse application vue argument");
    let query_json = js_to_json(&query);
    let name = query_json["name"].as_str().unwrap();
    let args = query_json["args"].as_array().unwrap().to_vec();
    let args = forge_args(args);
    let input = format!("{}{}", name, args);
    let response = forge_response(&query_json["type"]);
    let body =
        serde_json::json!({"query": format!("{} {{{} {}}}", kind, input, response) }).to_string();
    log_str(&body);
    let client = reqwest::Client::new();
    let res = client
        .post(&link)
        .body(body)
        .send()
        .await
        .expect("fail query send")
        .text()
        .await
        .expect("cannot get text of query response");
    let res_json = serde_json::from_str::<Value>(&res).expect("cannot translate JSON to JS");
    setf(&app, "result", &res_json["data"].serialize(&SER).unwrap());
    setf(
        &app,
        "errors",
        &res_json
            .get("errors")
            .unwrap_or(&Value::Null)
            .serialize(&SER)
            .unwrap(),
    );
}

fn empty_output_aux(v: &Value) -> bool {
    match v.get("kind") {
        None => true,
        Some(s) => match s.as_str() {
            Some("SCALAR") => true,
            Some("LIST") | Some("NON_NULL") => empty_output_aux(&v["ofType"]),
            _ => false,
        },
    }
}

#[wasm_bindgen]
pub fn empty_output(t: JsValue) -> bool {
    let t = js_to_json(&t);
    empty_output_aux(&t)
}
