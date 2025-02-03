// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::sync::Arc;

use async_trait::async_trait;
use futures::lock::Mutex;
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{Amount, Timestamp},
    identifiers::ChainId,
};
use linera_client::{chain_listener, wallet::Wallet};
use linera_core::{
    client::ChainClient,
    test_utils::{FaultType, MemoryStorageBuilder, NodeProvider, StorageBuilder as _, TestBuilder},
};
use linera_storage::{DbStorage, TestClock};
use linera_views::memory::MemoryStore;

use super::MutationRoot;

struct ClientContext {
    client: ChainClient<TestProvider, TestStorage>,
    update_calls: usize,
}

type TestStorage = DbStorage<MemoryStore, TestClock>;
type TestProvider = NodeProvider<TestStorage>;

#[async_trait]
impl chain_listener::ClientContext for ClientContext {
    type ValidatorNodeProvider = TestProvider;
    type Storage = TestStorage;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainClient<TestProvider, TestStorage>, linera_client::Error> {
        assert_eq!(chain_id, self.client.chain_id());
        Ok(self.client.clone())
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        _: ChainId,
        _: Option<KeyPair>,
        _: Timestamp,
    ) -> Result<(), linera_client::Error> {
        self.update_calls += 1;
        Ok(())
    }

    async fn update_wallet(
        &mut self,
        _: &ChainClient<TestProvider, TestStorage>,
    ) -> Result<(), linera_client::Error> {
        self.update_calls += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_faucet_rate_limiting() {
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await.unwrap();
    let client = builder
        .add_root_chain(1, Amount::from_tokens(6))
        .await
        .unwrap();
    let chain_id = client.chain_id();
    let context = ClientContext {
        client,
        update_calls: 0,
    };
    let context = Arc::new(Mutex::new(context));
    let root = MutationRoot {
        chain_id,
        context: context.clone(),
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
    };
    // The faucet is releasing one token every 1000 microseconds. So at 1000 one claim should
    // succeed. At 3000, two more should have been unlocked.
    clock.set(Timestamp::from(999));
    assert!(root.do_claim(PublicKey::test_key(0).into()).await.is_err());
    clock.set(Timestamp::from(1000));
    assert!(root.do_claim(PublicKey::test_key(1).into()).await.is_ok());
    assert!(root.do_claim(PublicKey::test_key(2).into()).await.is_err());
    clock.set(Timestamp::from(3000));
    assert!(root.do_claim(PublicKey::test_key(3).into()).await.is_ok());
    assert!(root.do_claim(PublicKey::test_key(4).into()).await.is_ok());
    assert!(root.do_claim(PublicKey::test_key(5).into()).await.is_err());
    // If a validator is offline, it will create a pending block and then fail.
    clock.set(Timestamp::from(6000));
    builder.set_fault_type([0, 1], FaultType::Offline).await;
    assert!(root.do_claim(PublicKey::test_key(6).into()).await.is_err());
    assert_eq!(context.lock().await.update_calls, 4); // Also called in the last error case.
}

#[test]
fn test_multiply() {
    let mul = MutationRoot::<()>::multiply;
    assert_eq!(mul((1 << 127) + (1 << 63), 1 << 63), [1 << 62, 1 << 62, 0]);
    assert_eq!(mul(u128::MAX, u64::MAX), [u64::MAX - 1, u64::MAX, 1]);
}
