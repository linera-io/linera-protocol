// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_base::{
    execution::ChainStatus,
    messages::{ChainDescription, Epoch, Owner},
};

#[tokio::test]
async fn test_read_write() {
    let mut store = InMemoryStoreClient::default();
    let mut chain = store.read_chain_or_default(ChainId::root(1)).await.unwrap();
    chain.description = Some(ChainDescription::Root(1));
    chain
        .state
        .committees
        .insert(Epoch::from(0), Committee::make_simple(Vec::new()));
    chain.state.epoch = Some(Epoch::from(0));
    chain.state.status = Some(ChainStatus::Managing {
        subscribers: Vec::new(),
    });
    chain.state.manager = ChainManager::single(Owner(PublicKey::debug(2)));
    store.write_chain(chain).await.unwrap();
    store
        .clone()
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap();
}
