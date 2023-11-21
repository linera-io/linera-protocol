// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::RocksDbStorage;
use crate::Storage;
use linera_base::identifiers::ChainId;
use std::mem;

/// Tests if released guards don't use memory.
#[tokio::test]
async fn guards_dont_leak() -> Result<(), anyhow::Error> {
    let storage = RocksDbStorage::make_test_storage(None).await;
    let chain_id = ChainId::root(1);
    // There should be no active guards when initialized
    assert_eq!(storage.client.guards.active_guards(), 0);
    // One guard should be active after obtaining a chain
    let chain = storage.load_chain(chain_id).await?;
    assert_eq!(storage.client.guards.active_guards(), 1);
    // No guards should be active after dropping the chain
    mem::drop(chain);
    assert_eq!(storage.client.guards.active_guards(), 0);
    Ok(())
}
