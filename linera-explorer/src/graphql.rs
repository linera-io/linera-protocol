// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use serde_json::Value;

use crate::reqwest_client;

pub async fn introspection(url: &str) -> Result<Value> {
    let client = reqwest_client();
    let graphql_query =
        "query { \
           __schema { \
             queryType { name } \
             mutationType { name } \
             subscriptionType { name } \
             types { ...FullType } \
             directives { name description locations args { ...InputValue } } } } \
         fragment FullType on __Type { \
           kind name description \
           fields(includeDeprecated:true) { \
             name description \
             args { ...InputValue } \
             type{ ...TypeRef } \
             isDeprecated deprecationReason } \
           inputFields { ...InputValue } \
           interfaces { ...TypeRef } \
           enumValues(includeDeprecated:true) { name description isDeprecated deprecationReason } \
           possibleTypes { ...TypeRef } } \
         fragment InputValue on __InputValue { \
           name description \
           type { ...TypeRef } \
           defaultValue } \
         fragment TypeRef on __Type { \
           kind name \
           ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType {kind name} } } } } } } }";
    let res = client
        .post(url)
        .body(format!("{{\"query\":\"{}\"}}", graphql_query))
        .send()
        .await?
        .text()
        .await?;
    Ok(serde_json::from_str::<Value>(&res)?)
}
