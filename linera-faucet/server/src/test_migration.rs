// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for JSON to SQLite migration.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::{ChainDescription, ChainOrigin, Epoch, InitialChainConfig, Timestamp},
        identifiers::AccountOwner,
        ownership::ChainOwnership,
    };
    use tempfile::tempdir;

    use crate::database::FaucetDatabase;

    #[tokio::test]
    async fn test_json_to_sqlite_migration() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("test_migration.json");
        let sqlite_path = temp_dir.path().join("test_migration.sqlite");

        // Create a legacy storage with some test data.
        let mut owner_to_chain = HashMap::new();

        let owner1: AccountOwner = CryptoHash::new(&TestString("owner1".into())).into();
        let owner2: AccountOwner = CryptoHash::new(&TestString("owner2".into())).into();

        let description1 = ChainDescription::new(
            ChainOrigin::Root(0),
            InitialChainConfig {
                ownership: ChainOwnership::single(owner1),
                epoch: Epoch::from(0),
                min_active_epoch: Epoch::from(0),
                max_active_epoch: Epoch::from(0),
                balance: Default::default(),
                application_permissions: Default::default(),
            },
            Timestamp::from(100),
        );

        let description2 = ChainDescription::new(
            ChainOrigin::Root(1),
            InitialChainConfig {
                ownership: ChainOwnership::single(owner2),
                epoch: Epoch::from(0),
                min_active_epoch: Epoch::from(0),
                max_active_epoch: Epoch::from(0),
                balance: Default::default(),
                application_permissions: Default::default(),
            },
            Timestamp::from(200),
        );

        owner_to_chain.insert(owner1, description1.clone());
        owner_to_chain.insert(owner2, description2.clone());

        // Write the legacy JSON file.
        let json_data = serde_json::to_vec_pretty(&owner_to_chain).unwrap();
        tokio::fs::write(&json_path, json_data).await.unwrap();

        // Create database and migrate.
        let db = FaucetDatabase::new(&sqlite_path).await.unwrap();
        db.migrate_from_json(&json_path).await.unwrap();

        // Verify the data was migrated correctly.
        let chain1 = db.get_chain(&owner1).await.unwrap().unwrap();
        assert_eq!(chain1.id(), description1.id());
        assert_eq!(chain1.timestamp(), description1.timestamp());

        let chain2 = db.get_chain(&owner2).await.unwrap().unwrap();
        assert_eq!(chain2.id(), description2.id());
        assert_eq!(chain2.timestamp(), description2.timestamp());

        // Verify the JSON file was renamed.
        assert!(tokio::fs::metadata(&json_path).await.is_err());
        assert!(
            tokio::fs::metadata(json_path.with_extension("json.migrated"))
                .await
                .is_ok()
        );

        // Verify database contents.
        assert_eq!(db.get_chain_count().await.unwrap(), 2);
        assert!(db.get_chain(&owner1).await.unwrap().is_some());
        assert!(db.get_chain(&owner2).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_json_extension_safety_check() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("dangerous.json");

        // Attempt to create database with .json extension should fail.
        let result = FaucetDatabase::new(&json_path).await;
        match result {
            Ok(_) => panic!("Expected error when creating database with .json extension"),
            Err(error) => {
                let error_message = error.to_string();
                assert!(error_message.contains("Database path cannot end with '.json' extension"));
                assert!(error_message.contains("dangerous.json"));
                assert!(error_message.contains("Use '.sqlite' or '.db' extension instead"));
            }
        }

        // But valid extensions should work fine.
        let sqlite_path = temp_dir.path().join("valid.sqlite");
        assert!(FaucetDatabase::new(&sqlite_path).await.is_ok());

        let db_path = temp_dir.path().join("valid.db");
        assert!(FaucetDatabase::new(&db_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_no_migration_if_db_not_empty() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("test_no_migration.json");
        let sqlite_path = temp_dir.path().join("test_no_migration.sqlite");

        // Create database and add a chain.
        let db = FaucetDatabase::new(&sqlite_path).await.unwrap();
        let owner: AccountOwner = CryptoHash::new(&TestString("existing".into())).into();
        let description = ChainDescription::new(
            ChainOrigin::Root(0),
            InitialChainConfig {
                ownership: ChainOwnership::single(owner),
                epoch: Epoch::from(0),
                min_active_epoch: Epoch::from(0),
                max_active_epoch: Epoch::from(0),
                balance: Default::default(),
                application_permissions: Default::default(),
            },
            Timestamp::from(100),
        );
        db.store_chains_batch(vec![(owner, description)])
            .await
            .unwrap();

        // Create a JSON file that should NOT be migrated.
        let mut owner_to_chain = HashMap::new();
        let new_owner: AccountOwner = CryptoHash::new(&TestString("new".into())).into();
        let new_description = ChainDescription::new(
            ChainOrigin::Root(1),
            InitialChainConfig {
                ownership: ChainOwnership::single(new_owner),
                epoch: Epoch::from(0),
                min_active_epoch: Epoch::from(0),
                max_active_epoch: Epoch::from(0),
                balance: Default::default(),
                application_permissions: Default::default(),
            },
            Timestamp::from(200),
        );
        owner_to_chain.insert(new_owner, new_description);
        let json_data = serde_json::to_vec_pretty(&owner_to_chain).unwrap();
        tokio::fs::write(&json_path, json_data).await.unwrap();

        // Attempt migration (should skip).
        db.migrate_from_json(&json_path).await.unwrap();

        // Verify the new chain was NOT added and the file not renamed.
        assert!(db.get_chain(&new_owner).await.unwrap().is_none());
        assert!(tokio::fs::metadata(&json_path).await.is_ok());
        assert_eq!(db.get_chain_count().await.unwrap(), 1);
    }
}
