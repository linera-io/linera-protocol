// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_base::messages::{ChainDescription, Epoch, Owner};

#[tokio::test]
async fn test_read_write() {
    let mut store = InMemoryStoreClient::default();
    let mut chain = store.read_chain_or_default(ChainId::root(1)).await.unwrap();
    chain.system_state.description = Some(ChainDescription::Root(1));
    chain
        .system_state
        .committees
        .insert(Epoch::from(0), Committee::make_simple(Vec::new()));
    chain.system_state.epoch = Some(Epoch::from(0));
    chain.system_state.admin_id = Some(ChainId::root(0));
    chain.system_state.manager = ChainManager::single(Owner(PublicKey::debug(2)));
    store.write_chain(chain).await.unwrap();
    store
        .clone()
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap();
}
