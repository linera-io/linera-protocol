// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fetch BCS serde formats from a deployed `formats-registry` application and
//! decode user-operation bytes against them. The [`Formats`] type and the
//! decode helpers come from `linera_sdk::formats`.

use anyhow::{anyhow, Context as _, Result};
pub use linera_sdk::formats::Formats;
use serde_json::Value;

use crate::reqwest_client;

/// Issues `query { get(moduleId: "<hex>") }` against the formats-registry
/// application service and parses the returned bytes as a JSON-encoded
/// [`Formats`]. Returns `Ok(None)` if the registry has no entry for that
/// module.
pub async fn fetch_formats(
    node: &str,
    registry_app_id: &str,
    module_id_hex: &str,
) -> Result<Option<Formats>> {
    let url = format!("{node}/applications/{registry_app_id}");
    let query = format!(r#"{{"query":"query {{ get(moduleId: \"{module_id_hex}\") }}"}}"#);
    let response = reqwest_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(query)
        .send()
        .await?
        .text()
        .await?;
    let response: Value = serde_json::from_str(&response)
        .with_context(|| format!("invalid JSON from formats registry: {response}"))?;
    if let Some(errors) = response.get("errors") {
        return Err(anyhow!("formats registry query failed: {errors}"));
    }
    let get = &response["data"]["get"];
    if get.is_null() {
        return Ok(None);
    }
    let bytes: Vec<u8> = get
        .as_array()
        .ok_or_else(|| anyhow!("formats registry returned non-array bytes"))?
        .iter()
        .map(|v| {
            v.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| anyhow!("formats registry returned non-u8 byte"))
        })
        .collect::<Result<_>>()?;
    let formats: Formats = serde_json::from_slice(&bytes)
        .context("registry bytes did not deserialize as Formats JSON")?;
    Ok(Some(formats))
}
