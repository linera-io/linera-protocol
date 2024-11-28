// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::{Arc, Mutex};

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use fungible::{Operation, Parameters};
use linera_sdk::{
    base::{AccountOwner, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    Service, ServiceRuntime,
};
use native_fungible::{AccountEntry, TICKER_SYMBOL};

#[derive(Clone)]
pub struct NativeFungibleTokenService {
    runtime: Arc<Mutex<ServiceRuntime<Self>>>,
}

linera_sdk::service!(NativeFungibleTokenService);

impl WithServiceAbi for NativeFungibleTokenService {
    type Abi = fungible::FungibleTokenAbi;
}

impl Service for NativeFungibleTokenService {
    type Parameters = Parameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        NativeFungibleTokenService {
            runtime: Arc::new(Mutex::new(runtime)),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema =
            Schema::build(self.clone(), Operation::mutation_root(), EmptySubscription).finish();
        schema.execute(request).await
    }
}

struct Accounts {
    runtime: Arc<Mutex<ServiceRuntime<NativeFungibleTokenService>>>,
}

#[Object]
impl Accounts {
    // Define a field that lets you query by key
    async fn entry(&self, key: AccountOwner) -> AccountEntry {
        let runtime = self
            .runtime
            .try_lock()
            .expect("Services only run in a single thread");

        let value = runtime.owner_balance(key);

        AccountEntry { key, value }
    }

    async fn entries(&self) -> Vec<AccountEntry> {
        let runtime = self
            .runtime
            .try_lock()
            .expect("Services only run in a single thread");
        runtime
            .owner_balances()
            .into_iter()
            .map(|(owner, amount)| AccountEntry {
                key: owner,
                value: amount,
            })
            .collect()
    }

    async fn keys(&self) -> Vec<AccountOwner> {
        let runtime = self
            .runtime
            .try_lock()
            .expect("Services only run in a single thread");
        runtime.balance_owners()
    }
}

// Implements additional fields not derived from struct members of FungibleToken.
#[Object]
impl NativeFungibleTokenService {
    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        Ok(String::from(TICKER_SYMBOL))
    }

    async fn accounts(&self) -> Result<Accounts, async_graphql::Error> {
        Ok(Accounts {
            runtime: self.runtime.clone(),
        })
    }
}
