// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_base::{base_types::*, messages::*};

#[tokio::test]
async fn test_rocksdb_storage_for_chains() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf());
    let id = ChainId(vec![SequenceNumber(1)]);
    {
        let mut chain = client.read_chain_or_default(&id).await.unwrap();
        assert_eq!(chain.next_sequence_number, SequenceNumber(0));
        chain.next_sequence_number = SequenceNumber(3);
        client.write_chain(chain).await.unwrap();
    }
    {
        let chain = client.read_chain_or_default(&id).await.unwrap();
        assert_eq!(chain.next_sequence_number, SequenceNumber(3));
    }
}

#[tokio::test]
async fn test_rocksdb_storage_for_certificates() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf());
    let request = Request {
        chain_id: ChainId::default(),
        operation: Operation::CloseChain,
        sequence_number: SequenceNumber::default(),
        round: RoundNumber::default(),
    };
    let value = Value::Confirmed { request };
    let certificate = Certificate::new(value, vec![]);
    client.write_certificate(certificate.clone()).await.unwrap();
    let read_certificate = client.read_certificate(certificate.hash).await.unwrap();
    assert_eq!(read_certificate.hash, certificate.hash);
}
