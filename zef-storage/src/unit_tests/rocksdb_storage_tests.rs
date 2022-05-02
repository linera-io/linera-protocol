// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_base::{base_types::*, messages::*};

#[tokio::test]
async fn test_rocksdb_storage_for_accounts() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf());
    let id = AccountId(vec![SequenceNumber(1)]);
    {
        let mut account = client.read_account_or_default(&id).await.unwrap();
        assert_eq!(account.next_sequence_number, SequenceNumber(0));
        account.next_sequence_number = SequenceNumber(3);
        client.write_account(account).await.unwrap();
    }
    {
        let account = client.read_account_or_default(&id).await.unwrap();
        assert_eq!(account.next_sequence_number, SequenceNumber(3));
    }
}

#[tokio::test]
async fn test_rocksdb_storage_for_certificates() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = RocksdbStoreClient::new(dir.path().to_path_buf());
    let request = Request {
        account_id: AccountId::default(),
        operation: Operation::CloseAccount,
        sequence_number: SequenceNumber::default(),
        round: RoundNumber::default(),
    };
    let value = Value::Confirmed { request };
    let certificate = Certificate::new(value, vec![]);
    client.write_certificate(certificate.clone()).await.unwrap();
    let read_certificate = client.read_certificate(certificate.hash).await.unwrap();
    assert_eq!(read_certificate.hash, certificate.hash);
}
