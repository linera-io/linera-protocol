// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_base::base_types::ChainDescription;

#[tokio::test]
async fn test_read_write() {
    let mut store = InMemoryStoreClient::default();
    let mut chain = store.read_chain_or_default(ChainId::root(1)).await.unwrap();
    chain.description = Some(ChainDescription::Root(1));
    chain.state.committee = Some(Committee::make_simple(Vec::new()));
    chain.state.manager = ChainManager::single(PublicKey::debug(2));
    store.write_chain(chain).await.unwrap();
    store
        .clone()
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap();
}
