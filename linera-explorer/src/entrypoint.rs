// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use serde_json::Value;
use serde_wasm_bindgen::from_value;
use wasm_bindgen::prelude::*;

use super::js_utils::{getf, js_to_json, setf, SER};
use crate::reqwest_client;

/// Auxiliary recursive function for `forge_arg`.
fn forge_arg_type(arg: &Value, non_null: bool) -> Option<String> {
    let deprecated = matches!(arg.get("isDeprecated"), Some(Value::Bool(true)));
    if deprecated {
        return None;
    };
    match arg["kind"].as_str() {
        Some("SCALAR") => {
            if non_null {
                Some(arg["_input"].to_string())
            } else {
                arg.get("_input").map(Value::to_string)
            }
        }
        Some("NON_NULL") => forge_arg_type(&arg["ofType"], true),
        Some("LIST") => {
            let args = arg["_input"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| forge_arg_type(x, false))
                .collect::<Vec<_>>();
            Some(format!("[{}]", args.join(", ")))
        }
        Some("ENUM") => arg["_input"].as_str().map(|x| x.to_string()),
        Some("INPUT_OBJECT") => {
            let args = arg["inputFields"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| {
                    let name = x["name"].as_str().unwrap();
                    forge_arg_type(&x["type"], false).map(|arg| format!("{}: {}", name, arg))
                })
                .collect::<Vec<_>>();
            Some(format!("{{{}}}", args.join(", ")))
        }
        _ => None,
    }
}

/// Forges a query argument.
fn forge_arg(arg: &Value) -> Option<String> {
    forge_arg_type(&arg["type"], false).map(|s| {
        format!(
            "{}: {}",
            arg["name"].as_str().expect("name is not a string"),
            s
        )
    })
}

/// Forges query arguments.
fn forge_args(args: Vec<Value>) -> String {
    let args = args.iter().filter_map(forge_arg).collect::<Vec<_>>();
    if !args.is_empty() {
        format!("({})", args.join(","))
    } else {
        "".to_string()
    }
}

/// Auxiliary recursive function for `forge_response`.
fn forge_response_type(output: &Value, name: Option<&str>, root: bool) -> Option<String> {
    let is_non_null_or_list = matches!(output["kind"].as_str(), Some("NON_NULL") | Some("LIST"));
    let incl = matches!(output.get("_include"), Some(Value::Bool(true)));
    let deprecated = matches!(output.get("isDeprecated"), Some(Value::Bool(true)));
    if !(incl || root || is_non_null_or_list) || deprecated {
        return None;
    }
    match output["kind"].as_str().unwrap() {
        "SCALAR" | "ENUM" => Some(
            name.unwrap_or_else(|| output["name"].as_str().unwrap())
                .to_string(),
        ),
        "NON_NULL" | "LIST" => forge_response_type(&output["ofType"], name, root),
        "OBJECT" => {
            let fields = output["fields"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|elt: &Value| {
                    forge_response_type(&elt["type"], elt["name"].as_str(), false)
                })
                .collect::<Vec<_>>();
            if root {
                Some(format!("{{ {} }}", fields.join(" ")))
            } else {
                Some(format!(
                    "{} {{ {} }}",
                    name.unwrap_or_else(|| output["name"].as_str().unwrap()),
                    fields.join(" ")
                ))
            }
        }
        _ => None,
    }
}

/// Forges query response.
fn forge_response(output: &Value) -> String {
    if empty_response_aux(output) {
        "".to_string()
    } else {
        forge_response_type(output, None, true).unwrap_or("".to_string())
    }
}

/// Queries mutations or queries for applications.
#[wasm_bindgen]
pub async fn query(app: JsValue, query: JsValue, kind: String) {
    let link =
        from_value::<String>(getf(&app, "link")).expect("cannot parse application vue argument");
    let fetch_json = js_to_json(&query);
    let name = fetch_json["name"].as_str().unwrap();
    let args = fetch_json["args"].as_array().unwrap().to_vec();
    let args = forge_args(args);
    let input = format!("{}{}", name, args);
    let response = forge_response(&fetch_json["type"]);
    let body =
        serde_json::json!({ "query": format!("{} {{{} {}}}", kind, input, response) }).to_string();
    let client = reqwest_client();
    match client.post(&link).body(body).send().await {
        Err(e) => setf(&app, "errors", &JsValue::from_str(&e.to_string())),
        Ok(response) => match response.text().await {
            Err(e) => setf(&app, "errors", &JsValue::from_str(&e.to_string())),
            Ok(response_txt) => match serde_json::from_str::<Value>(&response_txt) {
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
                Err(_) => setf(&app, "errors", &JsValue::from_str(&response_txt)),
            },
        },
    }
}

/// Checks if response fields are not needed.
#[wasm_bindgen]
pub fn empty_response(output: JsValue) -> bool {
    let output = js_to_json(&output);
    empty_response_aux(&output)
}

/// Auxiliary recursive function for `empty_response`
fn empty_response_aux(output: &Value) -> bool {
    match output.get("kind") {
        None => true,
        Some(s) => match s.as_str() {
            Some("SCALAR") => true,
            Some("LIST") | Some("NON_NULL") => empty_response_aux(&output["ofType"]),
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn test_forge_response() {
        let json = json!(
            { "_include":true, "args":null, "kind":"OBJECT", "name":"OutputObject", "ofType":null, "type":null,
                "fields":[
                    { "args":[], "name":"field1", "ofType":null,
                       "type": { "_include":true, "args":null, "isDeprecated":false, "kind":"SCALAR", "name":"String", "ofType":null, "type":null } },
                    { "args":[], "name": "field2", "ofType":null,
                       "type": { "_include":false, "args":null, "isDeprecated":false, "kind":"SCALAR", "name":"Int", "ofType":null, "type":null } },
                    { "args":[], "name":"field3", "ofType":null,
                       "type": { "_include":true, "args":null, "isDeprecated":false, "kind":"OBJECT", "name":"field3Output", "ofType":null, "type":null,
                                   "fields":[
                                       { "args":[], "isDeprecated":false, "name":"field31", "ofType":null,
                                          "type": { "_include":true, "args":null, "kind":"SCALAR", "name":"Boolean", "ofType":null, "type":null } },

                                   ]
                       }
                    }
                ]
            }
        );
        let result = super::forge_response(&json);
        assert_eq!(&result, "{ field1 field3 { field31 } }")
    }

    #[test]
    fn test_forge_args() {
        let json = json!(
            { "args":null, "name":"arg", "ofType":null,
               "type":{ "args":null, "kind":"NON_NULL", "name":null, "type":null,
                         "ofType":{ "args":null, "kind":"INPUT_OBJECT", "name":"InputObject", "ofType":null, "type":null, "inputFields":[
                             { "args":null, "name":"field1", "ofType":null,
                                "type":{ "_input":[{ "args":null, "kind":"NON_NULL", "name":null,
                                                      "ofType":{ "_include":true, "args":null, "kind":"SCALAR", "name":"String", "ofType":null, "type":null, "_input":"foo"} ,"type":null }],
                                           "args":null, "kind":"LIST", "name":null,
                                           "ofType":{ "args":null, "kind":"NON_NULL", "name":null,
                                                       "ofType":{ "_include":true, "args":null, "kind":"SCALAR", "name":"String", "ofType":null, "type":null }, "type":null}, "type":null } },
                             { "args":null, "name":"field2", "ofType":null,
                                "type":{ "_include":true, "args":null, "kind":"ENUM", "name":"Enum", "ofType":null, "type":null, "_input":"E2",
                                           "enumValues":[
                                               { "args":null, "isDeprecated":false, "name":"E1", "ofType":null, "type":null },
                                               { "args":null, "isDeprecated":false, "name":"E2", "ofType":null,"type":null} ] } },
                             { "args":null, "name":"field3", "ofType":null,
                                "type":{ "args":null, "kind":"INPUT_OBJECT","name":"InputObject0","ofType":null,"type":null,
                                          "inputFields":[
                                              { "args":null, "name":"field31",
                                                 "ofType":null,"type":{ "_include":true, "args":null, "kind":"SCALAR", "name":"Boolean", "ofType":null,"type":null,"_input":true} },
                                              { "args":null, "name":"field32", "ofType":null, "type":{ "_include":true, "args":null, "kind":"SCALAR", "name":"Int", "ofType":null, "type":null, "_input":42 } }]
                                } }
                         ] }
               }
            }
        );
        let result = super::forge_args(vec![json]);
        assert_eq!(
            &result,
            "(arg: {field1: [\"foo\"], field2: E2, field3: {field31: true, field32: 42}})"
        )
    }
}
