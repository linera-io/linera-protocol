// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::rc::Rc;

use linera_base::identifiers::ChainId;
use linera_client::config::GenesisConfig;
use linera_core::wallet;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use super::JsResult;

/// A wallet that stores the user's chains and keys in memory.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Wallet(pub(crate) Rc<wallet::Memory>);

impl Wallet {
    /// Returns a reference to the genesis configuration.
    #[must_use]
    pub fn genesis_config(&self) -> &GenesisConfig {
        self.0.genesis_config()
    }

    /// Returns the default chain ID, if one is set.
    #[must_use]
    pub fn default_chain(&self) -> Option<ChainId> {
        self.0.default_chain()
    }
}

#[wasm_bindgen]
impl Wallet {
    /// Set the owner of a chain (the account used to sign blocks on this chain).
    ///
    /// # Errors
    ///
    /// If the provided `ChainId` or `AccountOwner` are in the wrong format.
    #[wasm_bindgen(js_name = setOwner)]
    pub async fn set_owner(&self, chain_id: JsValue, owner: JsValue) -> JsResult<()> {
        let chain_id = serde_wasm_bindgen::from_value(chain_id)?;
        let owner = serde_wasm_bindgen::from_value(owner)?;
        self.0
            .mutate(chain_id, |chain| chain.owner = Some(owner))
            .ok_or(JsError::new(&format!(
                "chain {chain_id} doesn't exist in wallet"
            )))
    }
}
