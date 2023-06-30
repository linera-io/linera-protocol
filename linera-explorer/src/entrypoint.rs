// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::js_utils::{getf, js_to_json, setf, SER};

use serde::Serialize;
use serde_json::Value;
use serde_wasm_bindgen::from_value;
use wasm_bindgen::prelude::*;

/// Recursively forge query argument
fn forge_arg_type(arg: &Value, non_null: bool) -> Option<String> {
    match arg["kind"].as_str() {
        Some("SCALAR") => {
            if non_null {
                Some(format!("{}", arg["_input"]))
            } else {
                arg.get("_input").map(|input| format!("{}", input))
            }
        }
        Some("NON_NULL") => forge_arg_type(&arg["ofType"], true),
        Some("LIST") => {
            let args: Vec<String> = arg["_input"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| forge_arg_type(x, false))
                .collect();
            Some(format!("[{}]", args.join(", ")))
        }
        Some("ENUM") => arg["_input"].as_str().map(|x| x.to_string()),
        Some("INPUT_OBJECT") => {
            let args: Vec<String> = arg["inputFields"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| {
                    let name = x["name"].as_str().unwrap();
                    forge_arg_type(&x["type"], false).map(|arg| format!("{}: {}", name, arg))
                })
                .collect();
            Some(format!("{{{}}}", args.join(", ")))
        }
        _ => None,
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

/// Recursively forge query response
fn forge_response_type(t: &Value, name: Option<&str>, root: bool) -> String {
    let is_non_null_or_list = matches!(t["kind"].as_str(), Some("NON_NULL") | Some("LIST"));
    let incl = matches!(t.get("_include"), Some(Value::Bool(true)));
    if !(incl || root || is_non_null_or_list) {
        "".to_string()
    } else {
        match t["kind"].as_str().unwrap() {
            "SCALAR" | "ENUM" => name
                .unwrap_or_else(|| t["name"].as_str().unwrap())
                .to_string(),
            "NON_NULL" | "LIST" => forge_response_type(&t["ofType"], name, root),
            "OBJECT" => {
                let fields: Vec<String> = t["fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x: &Value| forge_response_type(&x["type"], x["name"].as_str(), false))
                    .collect();
                if root {
                    format!("{{ {} }}", fields.join(" "))
                } else {
                    format!(
                        "{} {{ {} }}",
                        name.unwrap_or_else(|| t["name"].as_str().unwrap()),
                        fields.join(" ")
                    )
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

/// Query mutations or queries for applications
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
        serde_json::json!({ "query": format!("{} {{{} {}}}", kind, input, response) }).to_string();
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
    match serde_json::from_str::<Value>(&res) {
        Ok(res_json) => {
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
        Err(_) => setf(&app, "errors", &JsValue::from_str(&res)),
    }
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

/// Check if response fields are not needed
#[wasm_bindgen]
pub fn empty_output(t: JsValue) -> bool {
    let t = js_to_json(&t);
    empty_output_aux(&t)
}
