// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn test_read_write() {
    let mut store = InMemoryStoreClient::default();
    let mut chain = store.read_chain_or_default(&dbg_chain(1)).await.unwrap();
    chain.state.committee = Some(Committee::make_simple(Vec::new()));
    chain.state.manager = ChainManager::single(dbg_addr(2));
    store.write_chain(chain).await.unwrap();
    store
        .clone()
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
}
