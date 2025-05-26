// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.

pub mod in_memory {
    use linera_base::{
        crypto::{AccountPublicKey, AccountSignature, CryptoHash, Signer},
        identifiers::AccountOwner,
    };
    use linera_persistent::{self as persistent};

    use wasm_bindgen::prelude::*;
    use web_sys::wasm_bindgen;

    /// A signer that stores the user's keys in memory.
    #[wasm_bindgen(js_name = "InMemorySigner")]

    pub struct JsInMemorySigner(persistent::Memory<linera_base::crypto::InMemorySigner>);

    #[wasm_bindgen]
    impl JsInMemorySigner {
        /// Creates a new in-memory signer and returns it.
        #[wasm_bindgen(constructor)]
        pub async fn new() -> JsInMemorySigner {
            JsInMemorySigner(persistent::Memory::new(
                linera_base::crypto::InMemorySigner::new(None),
            ))
        }
    }

    impl Signer for JsInMemorySigner {
        type Error = <linera_base::crypto::InMemorySigner as Signer>::Error;

        async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
            self.0.contains_key(owner).await
        }

        async fn get_public_key(
            &self,
            owner: &AccountOwner,
        ) -> Result<AccountPublicKey, Self::Error> {
            self.0.get_public_key(owner).await
        }

        async fn sign(
            &self,
            owner: &AccountOwner,
            value: &CryptoHash,
        ) -> Result<AccountSignature, Self::Error> {
            self.0.sign(owner, value).await
        }
    }
}
