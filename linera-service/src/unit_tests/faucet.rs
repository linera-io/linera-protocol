// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::sync::Arc;

use async_trait::async_trait;
use futures::lock::Mutex;
use linera_base::{
    crypto::KeyPair,
    data_types::{Amount, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_core::{
    client::ChainClient,
    test_utils::{FaultType, MemoryStorageBuilder, NodeProvider, StorageBuilder as _, TestBuilder},
};
use linera_storage::{MemoryStorage, TestClock};

use super::MutationRoot;
use crate::{chain_listener, wallet::Wallet};

#[derive(Default)]
struct ClientContext {
    update_calls: usize,
}

type TestStorage = MemoryStorage<TestClock>;
type TestProvider = NodeProvider<TestStorage>;

#[async_trait]
impl chain_listener::ClientContext for ClientContext {
    type ValidatorNodeProvider = TestProvider;
    type Storage = TestStorage;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn make_chain_client(&self, _: ChainId) -> ChainClient<TestProvider, TestStorage> {
        unimplemented!()
    }

    fn update_wallet_for_new_chain(&mut self, _: ChainId, _: Option<KeyPair>, _: Timestamp) {
        self.update_calls += 1;
    }

    async fn update_wallet<'a>(&'a mut self, _: &'a mut ChainClient<TestProvider, TestStorage>) {
        self.update_calls += 1;
    }
}

#[tokio::test]
async fn test_faucet_rate_limiting() {
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await.unwrap();
    let client = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(6))
        .await
        .unwrap();
    let client = Arc::new(Mutex::new(client));
    let context = Arc::new(Mutex::new(ClientContext::default()));
    let root = MutationRoot {
        client,
        context: context.clone(),
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
    };
    // The faucet is releasing one token every 1000 microseconds. So at 1000 one claim should
    // succeed. At 3000, two more should have been unlocked.
    clock.set(Timestamp::from(999));
    assert!(root.do_claim(KeyPair::generate().public()).await.is_err());
    clock.set(Timestamp::from(1000));
    assert!(root.do_claim(KeyPair::generate().public()).await.is_ok());
    assert!(root.do_claim(KeyPair::generate().public()).await.is_err());
    clock.set(Timestamp::from(3000));
    assert!(root.do_claim(KeyPair::generate().public()).await.is_ok());
    assert!(root.do_claim(KeyPair::generate().public()).await.is_ok());
    assert!(root.do_claim(KeyPair::generate().public()).await.is_err());
    // If a validator is offline, it will create a pending block and then fail.
    clock.set(Timestamp::from(6000));
    builder.set_fault_type([0, 1], FaultType::Offline).await;
    assert!(root.do_claim(KeyPair::generate().public()).await.is_err());
    assert_eq!(context.lock().await.update_calls, 4); // Also called in the last error case.
}

#[test]
fn test_multiply() {
    let mul = MutationRoot::<(), MemoryStorage<TestClock>, ()>::multiply;
    assert_eq!(mul((1 << 127) + (1 << 63), 1 << 63), [1 << 62, 1 << 62, 0]);
    assert_eq!(mul(u128::MAX, u64::MAX), [u64::MAX - 1, u64::MAX, 1]);
}
