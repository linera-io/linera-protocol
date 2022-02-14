// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use zef_core::base_types::*;

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
