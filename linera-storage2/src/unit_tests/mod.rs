// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{RocksdbStoreClient, Store};
use linera_base::{
    committee::Committee,
    crypto::KeyPair,
    messages::{ChainDescription, ChainId},
    system::Balance,
};
use std::collections::BTreeMap;

#[tokio::test]
async fn create_store_load_chain() -> Result<(), anyhow::Error> {
    // let localstack = LocalStackTestContext::new().await?;
    // let table = "linera".parse()?;
    // let store = DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table).await?;
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_path_buf();
    let store = RocksdbStoreClient::new(path).await?;

    let committee = Committee {
        validators: BTreeMap::new(),
        total_votes: 1,
        quorum_threshold: 2,
        validity_threshold: 3,
    };
    let admin_id = ChainId::root(1);
    let description = ChainDescription::Root(2);
    let chain_id = description.into();
    let key_pair = KeyPair::generate();
    let owner = key_pair.public().into();
    let balance = 100.into();

    let chain = store
        .create_chain(committee, admin_id, description, owner, balance)
        .await?;
    let _reloaded_chain = store.load_active_chain(chain_id).await?;

    Ok(())
}
