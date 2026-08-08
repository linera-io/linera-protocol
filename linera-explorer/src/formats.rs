// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fetch BCS serde formats from the local linera-service node and decode
//! user-operation bytes against them. The [`Formats`] type and the decode
//! helpers come from `linera_sdk::formats`.

use anyhow::{anyhow, Context as _, Result};
pub use linera_sdk::formats::Formats;
use serde_json::Value;

use crate::{js_utils::log_str, reqwest_client};

/// Issues `query { applicationFormats(chainId: ..., formatsBlobHash: ...) }`
/// against the linera-service node and returns the bytes registered for the
/// given application's module. Returns `Ok(None)` if the local node has no
/// such blob.
pub async fn fetch_formats(
    node: &str,
    chain_id_hex: &str,
    formats_blob_hash_hex: &str,
) -> Result<Option<Formats>> {
    let url = node.to_string();
    let query = format!(
        r#"{{"query":"query {{ applicationFormats(chainId: \"{chain_id_hex}\", formatsBlobHash: \"{formats_blob_hash_hex}\") }}"}}"#
    );
    log_str(&format!("fetch_formats: POST {url}"));
    log_str(&format!("fetch_formats: body {query}"));
    let response = reqwest_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(query)
        .send()
        .await?
        .text()
        .await?;
    log_str(&format!("fetch_formats: response {response}"));
    let response: Value = serde_json::from_str(&response)
        .with_context(|| format!("invalid JSON from node: {response}"))?;
    if let Some(errors) = response.get("errors") {
        return Err(anyhow!("application formats query failed: {errors}"));
    }
    let bytes_value = &response["data"]["applicationFormats"];
    if bytes_value.is_null() {
        log_str("fetch_formats: node returned null (formats blob not present locally)");
        return Ok(None);
    }
    let bytes: Vec<u8> = bytes_value
        .as_array()
        .ok_or_else(|| anyhow!("application formats query returned non-array bytes"))?
        .iter()
        .map(|v| {
            v.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| anyhow!("application formats query returned non-u8 byte"))
        })
        .collect::<Result<_>>()?;
    let formats: Formats = linera_sdk::bcs::from_bytes(&bytes)
        .context("formats blob did not deserialize as Formats BCS")?;
    Ok(Some(formats))
}
