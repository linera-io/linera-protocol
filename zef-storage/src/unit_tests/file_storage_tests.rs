// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_base::{base_types::*, chain::ExecutionState, messages::*};

#[tokio::test]
async fn test_file_storage_for_chains() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = FileStoreClient::new(dir.path().to_path_buf());
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
async fn test_file_storage_for_certificates() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = FileStoreClient::new(dir.path().to_path_buf());
    let block = Block {
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::CloseChain],
        height: BlockHeight::default(),
        previous_block_hash: None,
    };
    let value = Value::ConfirmedBlock {
        block,
        state_hash: HashValue::new(&ExecutionState::default()),
    };
    let certificate = Certificate::new(value, vec![]);
    client.write_certificate(certificate.clone()).await.unwrap();
    let read_certificate = client.read_certificate(certificate.hash).await.unwrap();
    assert_eq!(read_certificate.hash, certificate.hash);
}
