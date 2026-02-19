// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::rc::Rc;

use linera_base::identifiers::ChainId;
use linera_client::config::GenesisConfig;
use linera_core::wallet;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{lock::Lock, Error, Result};

/// A wallet that stores the user's chains and keys in memory.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Wallet {
    pub(crate) chains: Rc<wallet::Memory>,
    pub(crate) default: Option<ChainId>,
    pub(crate) genesis_config: GenesisConfig,
    pub(crate) lock: Option<Rc<Lock>>,
}

#[wasm_bindgen]
impl Wallet {
    /// Set the owner of a chain (the account used to sign blocks on this chain).
    ///
    /// # Errors
    ///
    /// If the provided `ChainId` or `AccountOwner` are in the wrong format.
    #[wasm_bindgen(js_name = setOwner)]
    pub async fn set_owner(&self, chain_id: JsValue, owner: JsValue) -> Result<()> {
        let chain_id = serde_wasm_bindgen::from_value(chain_id)?;
        let owner = serde_wasm_bindgen::from_value(owner)?;
        self.chains
            .mutate(chain_id, |chain| chain.owner = Some(owner))
            .ok_or(Error::new(&format!("chain {chain_id} doesn't exist in wallet")).into())
    }

    #[must_use]
    /// Get the name of the wallet. Wallets with different names should use different
    /// storage; only one wallet can use the same name at a time.
    pub fn name(&self) -> String {
        self.default
            .map_or_else(|| "default".into(), |name| name.to_string())
    }

    /// Lock the wallet, preventing anyone else from using a wallet with this name.
    ///
    /// # Errors
    /// If the wallet is already locked.
    pub async fn lock(&mut self) -> Result<()> {
        self.lock = Some(Rc::new(Lock::try_acquire(&self.name()).await?));
        Ok(())
    }
}

impl std::ops::Deref for Wallet {
    type Target = wallet::Memory;
    fn deref(&self) -> &Self::Target {
        &*self.chains
    }
}
