// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_core::{base_types::*, messages::*};

#[tokio::test]
async fn test_file_storage_for_accounts() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = FileStoreClient::new(dir.path().to_path_buf());
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
async fn test_file_storage_for_certificates() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut client = FileStoreClient::new(dir.path().to_path_buf());
    let proposal = ConsensusProposal {
        instance_id: AccountId::default(),
        round: SequenceNumber::default(),
        decision: ConsensusDecision::Confirm,
    };
    let value = Value::PreCommit {
        proposal,
        requests: vec![],
    };
    let certificate = Certificate::new(value, vec![]);
    let certificate_clone = certificate.clone();
    let certificate_hash = certificate.hash;
    client.write_certificate(certificate).await.unwrap();
    let read_certificate = client.read_certificate(certificate_hash).await.unwrap();
    assert_eq!(read_certificate.hash, certificate_clone.hash);
}
