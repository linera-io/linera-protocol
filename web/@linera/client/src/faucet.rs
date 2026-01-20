// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::AccountOwner;
use linera_core::wallet;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use super::{JsResult, Wallet};

#[wasm_bindgen]
pub struct Faucet(linera_faucet_client::Faucet);

#[wasm_bindgen]
impl Faucet {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(url: String) -> Faucet {
        Faucet(linera_faucet_client::Faucet::new(url))
    }

    /// Creates a new wallet from the faucet.
    ///
    /// # Errors
    /// If we couldn't retrieve the genesis config from the faucet.
    #[wasm_bindgen(js_name = createWallet)]
    pub async fn create_wallet(&self) -> JsResult<Wallet> {
        let genesis_config = self.0.genesis_config().await?;
        Ok(Wallet(std::rc::Rc::new(wallet::Memory::new(
            genesis_config,
        ))))
    }

    /// Claims a new chain from the faucet, with a new keypair and some tokens.
    ///
    /// # Errors
    /// - if we fail to get the list of current validators from the faucet
    /// - if we fail to claim the chain from the faucet
    /// - if we fail to persist the new chain or keypair to the wallet
    ///
    /// # Panics
    /// If an error occurs in the chain listener task.
    #[wasm_bindgen(js_name = claimChain)]
    pub async fn claim_chain(&self, wallet: &Wallet, owner: AccountOwner) -> JsResult<String> {
        tracing::info!(
            "Requesting a new chain for owner {} using the faucet at address {}",
            owner,
            self.0.url(),
        );
        let description = self.0.claim(&owner).await?;
        let chain_id = description.id();
        wallet.0.insert(
            chain_id,
            wallet::Chain {
                owner: Some(owner),
                ..description.into()
            },
        );
        Ok(chain_id.to_string())
    }
}
