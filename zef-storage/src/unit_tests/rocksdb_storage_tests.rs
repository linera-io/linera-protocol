// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_base::{base_types::*, messages::*};

#[tokio::test]
async fn test_rocksdb_storage_for_chains() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf());
    let id = ChainId(vec![BlockHeight(1)]);
    {
        let mut chain = client.read_chain_or_default(&id).await.unwrap();
        assert_eq!(chain.next_block_height, BlockHeight(0));
        chain.next_block_height = BlockHeight(3);
        client.write_chain(chain).await.unwrap();
    }
    {
        let chain = client.read_chain_or_default(&id).await.unwrap();
        assert_eq!(chain.next_block_height, BlockHeight(3));
    }
}

#[tokio::test]
async fn test_rocksdb_storage_for_certificates() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf());
    let block = Block {
        chain_id: ChainId::default(),
        incoming_messages: Vec::new(),
        operation: Operation::CloseChain,
        previous_block_hash: None,
        height: BlockHeight::default(),
    };
    let value = Value::Confirmed { block };
    let certificate = Certificate::new(value, vec![]);
    client.write_certificate(certificate.clone()).await.unwrap();
    let read_certificate = client.read_certificate(certificate.hash).await.unwrap();
    assert_eq!(read_certificate.hash, certificate.hash);
}
