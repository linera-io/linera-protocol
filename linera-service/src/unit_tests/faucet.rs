// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::MutationRoot;
use futures::lock::Mutex;
use linera_base::{
    crypto::KeyPair,
    data_types::{Amount, Timestamp},
    identifiers::ChainDescription,
};
use linera_core::client::client_test_utils::{
    MakeMemoryStoreClient, StoreBuilder as _, TestBuilder,
};
use std::sync::Arc;

#[tokio::test]
async fn test_faucet_rate_limiting() {
    let store_builder = MakeMemoryStoreClient::default();
    let clock = store_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(store_builder, 4, 1).await.unwrap();
    let client = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(6))
        .await
        .unwrap();
    let client = Arc::new(Mutex::new(client));
    let root = MutationRoot {
        client,
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
}

#[test]
fn test_multiply() {
    let mul = MutationRoot::<(), ()>::multiply;
    assert_eq!(mul((1 << 127) + (1 << 63), 1 << 63), [1 << 62, 1 << 62, 0]);
    assert_eq!(mul(u128::MAX, u64::MAX), [u64::MAX - 1, u64::MAX, 1]);
}
