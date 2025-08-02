// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::sync::Arc;

use futures::lock::Mutex;
use linera_base::{
    crypto::{AccountPublicKey, InMemorySigner},
    data_types::{Amount, Timestamp},
    identifiers::{AccountOwner, ChainId},
};
use linera_client::{chain_listener, wallet::Wallet};
use linera_core::{
    client::ChainClient,
    environment,
    test_utils::{FaultType, MemoryStorageBuilder, StorageBuilder as _, TestBuilder},
};

use super::MutationRoot;

struct ClientContext {
    client: ChainClient<environment::Test>,
    update_calls: usize,
}

impl chain_listener::ClientContext for ClientContext {
    type Environment = environment::Test;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn storage(&self) -> &environment::TestStorage {
        self.client.storage_client()
    }

    fn client(&self) -> &Arc<linera_core::client::Client<environment::Test>> {
        unimplemented!()
    }

    fn timing_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<(u64, linera_core::client::TimingType)>> {
        None
    }

    fn make_chain_client(&self, chain_id: ChainId) -> ChainClient<environment::Test> {
        assert_eq!(chain_id, self.client.chain_id());
        self.client.clone()
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        _: ChainId,
        _: Option<AccountOwner>,
        _: Timestamp,
    ) -> Result<(), linera_client::Error> {
        self.update_calls += 1;
        Ok(())
    }

    async fn update_wallet(
        &mut self,
        _: &ChainClient<environment::Test>,
    ) -> Result<(), linera_client::Error> {
        self.update_calls += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_faucet_rate_limiting() {
    let storage_builder = MemoryStorageBuilder::default();
    let keys = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys).await.unwrap();
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
    assert!(root
        .do_claim(AccountPublicKey::test_key(0).into())
        .await
        .is_err());
    clock.set(Timestamp::from(1000));
    assert!(root
        .do_claim(AccountPublicKey::test_key(1).into())
        .await
        .is_ok());
    assert!(root
        .do_claim(AccountPublicKey::test_key(2).into())
        .await
        .is_err());
    clock.set(Timestamp::from(3000));
    assert!(root
        .do_claim(AccountPublicKey::test_key(3).into())
        .await
        .is_ok());
    assert!(root
        .do_claim(AccountPublicKey::test_key(4).into())
        .await
        .is_ok());
    assert!(root
        .do_claim(AccountPublicKey::test_key(5).into())
        .await
        .is_err());
    // If a validator is offline, it will create a pending block and then fail.
    clock.set(Timestamp::from(6000));
    builder.set_fault_type([0, 1], FaultType::Offline).await;
    assert!(root
        .do_claim(AccountPublicKey::test_key(6).into())
        .await
        .is_err());
    assert_eq!(context.lock().await.update_calls, 4); // Also called in the last error case.
}

#[test]
fn test_multiply() {
    use super::multiply;

    assert_eq!(
        multiply((1 << 127) + (1 << 63), 1 << 63),
        [1 << 62, 1 << 62, 0]
    );
    assert_eq!(multiply(u128::MAX, u64::MAX), [u64::MAX - 1, u64::MAX, 1]);
}
