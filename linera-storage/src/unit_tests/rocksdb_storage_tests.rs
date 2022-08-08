// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_base::{
    chain::SYSTEM,
    messages::*,
    system::{SystemExecutionState, SystemOperation},
};

#[tokio::test]
async fn test_rocksdb_storage_for_chains() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf(), 1).unwrap();
    let id = ChainId::root(1);
    {
        let mut chain = client.read_chain_or_default(id).await.unwrap();
        assert_eq!(chain.next_block_height, BlockHeight(0));
        chain.next_block_height = BlockHeight(3);
        client.write_chain(chain).await.unwrap();
    }
    {
        let chain = client.read_chain_or_default(id).await.unwrap();
        assert_eq!(chain.next_block_height, BlockHeight(3));
    }
}

#[tokio::test]
async fn test_rocksdb_storage_for_certificates() {
    let dir = tempfile::TempDir::new().unwrap();
    // Repeat read/write to catch issues with opening the DB multiple times.
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf(), 1).unwrap();
    for i in 0..2 {
        let block = Block {
            epoch: Epoch::from(0),
            chain_id: ChainId::root(i),
            incoming_messages: Vec::new(),
            operations: vec![(SYSTEM, Operation::System(SystemOperation::CloseChain))],
            previous_block_hash: None,
            height: BlockHeight::default(),
        };
        let value = Value::ConfirmedBlock {
            block,
            effects: Vec::new(),
            state_hash: HashValue::new(&SystemExecutionState::new(ChainId::root(1))),
        };
        let certificate = Certificate::new(value, vec![]);
        client.write_certificate(certificate.clone()).await.unwrap();
        let read_certificate = client.read_certificate(certificate.hash).await.unwrap();
        assert_eq!(read_certificate.hash, certificate.hash);
    }
}

#[tokio::test]
async fn test_rocksdb_persistance_across_writes() {
    let dir = tempfile::TempDir::new().unwrap();
    let block = Block {
        epoch: Epoch::from(0),
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![(SYSTEM, Operation::System(SystemOperation::CloseChain))],
        previous_block_hash: None,
        height: BlockHeight::default(),
    };
    let value = Value::ConfirmedBlock {
        block,
        effects: Vec::new(),
        state_hash: HashValue::new(&SystemExecutionState::new(ChainId::root(1))),
    };
    let certificate = Certificate::new(value, vec![]);

    {
        let mut client1 = RocksdbStoreClient::new(dir.path().to_path_buf(), 1).unwrap();
        client1
            .write_certificate(certificate.clone())
            .await
            .unwrap();
    }

    {
        let mut client2 = RocksdbStoreClient::new(dir.path().to_path_buf(), 1).unwrap();
        let read_certificate = client2.read_certificate(certificate.hash).await.unwrap();
        assert_eq!(read_certificate.hash, certificate.hash);
    }
}
