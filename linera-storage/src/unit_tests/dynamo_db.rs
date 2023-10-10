// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{DynamoDbStoreClient, Store};
use linera_base::identifiers::ChainId;
use std::mem;

/// Tests if released guards don't use memory.
#[tokio::test]
async fn guards_dont_leak() -> Result<(), anyhow::Error> {
    let store = DynamoDbStoreClient::make_test_store(None).await;
    let chain_id = ChainId::root(1);
    // There should be no active guards when initialized
    assert_eq!(store.client.guards.active_guards(), 0);
    // One guard should be active after obtaining a chain
    let chain = store.load_chain(chain_id).await?;
    assert_eq!(store.client.guards.active_guards(), 1);
    // No guards should be active after dropping the chain
    mem::drop(chain);
    assert_eq!(store.client.guards.active_guards(), 0);
    Ok(())
}
