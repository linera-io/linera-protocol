// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{InMemorySigner, ValidatorKeypair},
    data_types::Amount,
};
use linera_storage::{DbStorage, TestClock};
use linera_views::{
    random::generate_test_namespace,
    rocks_db::{PathWithGuard, RocksDbDatabase},
    store::TestKeyValueDatabase as _,
};
use rocksdb::{
    backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
    Env,
};
use tempfile::TempDir;

use crate::{
    chain_worker::ChainWorkerConfig,
    data_types::ChainInfoQuery,
    test_utils::{RocksDbStorageBuilder, TestBuilder},
    worker::WorkerState,
};

#[tokio::test]
async fn test_no_key_boots_on_cross_key_backup() {
    let mut builder = TestBuilder::new(
        RocksDbStorageBuilder::new().await,
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");
    let _client = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("root chain");
    let chain_id = builder.admin_chain_id();

    let source_storage = builder
        .validator_storages
        .values()
        .next()
        .expect("at least one validator")
        .clone();

    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");

    let restore_base = TempDir::new().expect("restore base");
    let namespace = generate_test_namespace();
    let restore_ns_path = restore_base.path().join(&namespace);
    std::fs::create_dir_all(&restore_ns_path).expect("create restore ns dir");

    {
        let opts = BackupEngineOptions::new(backup_dir.path()).expect("backup opts");
        let env = Env::new().expect("rocksdb env");
        let mut engine = BackupEngine::open(&opts, &env).expect("backup engine");
        engine
            .restore_from_latest_backup(
                &restore_ns_path,
                &restore_ns_path,
                &RestoreOptions::default(),
            )
            .expect("restore");
    }

    let mut restore_config = RocksDbDatabase::new_test_config()
        .await
        .expect("test config");
    restore_config.inner_config.path_with_guard =
        PathWithGuard::new(restore_base.path().to_path_buf());

    let restored_storage = DbStorage::<RocksDbDatabase, TestClock>::connect_for_testing(
        restore_config,
        &namespace,
        None,
        TestClock::new(),
    )
    .await
    .expect("connect for testing");

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    };
    let worker = WorkerState::new(restored_storage, config, None);
    let result = worker
        .handle_chain_info_query(ChainInfoQuery::new(chain_id))
        .await;
    assert!(result.is_ok(), "observer worker failed: {result:?}");
}

#[tokio::test]
async fn test_mismatched_key_boots_on_cross_key_backup() {
    let mut builder = TestBuilder::new(
        RocksDbStorageBuilder::new().await,
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");
    let _client = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("root chain");
    let chain_id = builder.admin_chain_id();

    let source_storage = builder
        .validator_storages
        .values()
        .next()
        .expect("at least one validator")
        .clone();

    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");

    let restore_base = TempDir::new().expect("restore base");
    let namespace = generate_test_namespace();
    let restore_ns_path = restore_base.path().join(&namespace);
    std::fs::create_dir_all(&restore_ns_path).expect("create restore ns dir");

    {
        let opts = BackupEngineOptions::new(backup_dir.path()).expect("backup opts");
        let env = Env::new().expect("rocksdb env");
        let mut engine = BackupEngine::open(&opts, &env).expect("backup engine");
        engine
            .restore_from_latest_backup(
                &restore_ns_path,
                &restore_ns_path,
                &RestoreOptions::default(),
            )
            .expect("restore");
    }

    let mut restore_config = RocksDbDatabase::new_test_config()
        .await
        .expect("test config");
    restore_config.inner_config.path_with_guard =
        PathWithGuard::new(restore_base.path().to_path_buf());

    let restored_storage = DbStorage::<RocksDbDatabase, TestClock>::connect_for_testing(
        restore_config,
        &namespace,
        None,
        TestClock::new(),
    )
    .await
    .expect("connect for testing");

    let mismatched_key = ValidatorKeypair::generate().secret_key;
    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(mismatched_key));
    let worker = WorkerState::new(restored_storage, config, None);
    let result = worker
        .handle_chain_info_query(ChainInfoQuery::new(chain_id))
        .await;
    assert!(result.is_ok(), "mismatched-key worker failed: {result:?}");
}
