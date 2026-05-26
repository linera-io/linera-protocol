// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![expect(clippy::large_futures)]
//! Phase 0a/0b: confirm that a `WorkerState` can boot from a backup produced by a
//! different validator and continue processing blocks.  Tests cover three backends:
//! `MemoryDatabase`, `RocksDbDatabase`, and `ScyllaDbDatabase`.

use linera_base::{
    crypto::{InMemorySigner, ValidatorKeypair, ValidatorSecretKey},
    data_types::Amount,
    identifiers::{Account, AccountOwner},
};
use linera_storage::{DbStorage, TestClock};
use linera_views::{memory::MemoryDatabase, random::generate_test_namespace};
use tempfile::TempDir;

use crate::{
    chain_worker::ChainWorkerConfig,
    test_utils::{ClientOutcomeResultExt as _, MemoryStorageBuilder, TestBuilder},
    worker::WorkerState,
};

// ── Memory backend helpers ────────────────────────────────────────────────────

/// Deserializes a memory backup from `dir` into a fresh namespace and returns
/// a `DbStorage` connected to it.
async fn restore_memory_backup(
    backup_dir: &TempDir,
    namespace: &str,
) -> DbStorage<MemoryDatabase, TestClock> {
    use std::collections::BTreeMap;

    use linera_views::{
        batch::{Batch, WriteOperation},
        store::{KeyValueDatabase as _, TestKeyValueDatabase as _, WritableKeyValueStore as _},
    };

    let encoded =
        std::fs::read(backup_dir.path().join("memory_backup.bcs")).expect("read memory backup");
    let snapshot: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>> =
        bcs::from_bytes(&encoded).expect("deserialize memory backup");

    let config = MemoryDatabase::new_test_config()
        .await
        .expect("memory test config");

    MemoryDatabase::create(&config, namespace)
        .await
        .expect("create memory namespace");
    {
        let db = MemoryDatabase::connect(&config, namespace)
            .await
            .expect("connect to memory");
        for (root_key, kv_pairs) in snapshot {
            let store = db.open_shared(&root_key).expect("open memory store");
            if !kv_pairs.is_empty() {
                let batch = Batch {
                    operations: kv_pairs
                        .into_iter()
                        .map(|(key, value)| WriteOperation::Put { key, value })
                        .collect(),
                };
                store.write_batch(batch).await.expect("write memory batch");
            }
        }
    }

    DbStorage::<MemoryDatabase, TestClock>::connect_for_testing(
        config,
        namespace,
        None,
        TestClock::new(),
    )
    .await
    .expect("connect memory storage for testing")
}

/// Sets up a 4-validator memory-backed network, commits block 0 (transfer),
/// takes a backup, commits block 1, and returns `(backup_dir, cert_for_block_1)`.
async fn memory_setup_backup_and_next_cert(
) -> (TempDir, linera_chain::types::ConfirmedBlockCertificate) {
    let mut builder = TestBuilder::new(
        MemoryStorageBuilder::default(),
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let source_storage = builder
        .validator_storages
        .values()
        .next()
        .expect("at least one validator")
        .clone();
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");

    let cert1 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    (backup_dir, cert1)
}

/// Sets up a 4-validator memory-backed network, commits block 0, takes a backup
/// of validator 0 together with its key, commits blocks 1 and 2, and returns
/// `(backup_dir, validator_secret, cert1, cert2)`.
async fn memory_setup_stale_backup_and_two_certs() -> (
    TempDir,
    ValidatorSecretKey,
    linera_chain::types::ConfirmedBlockCertificate,
    linera_chain::types::ConfirmedBlockCertificate,
) {
    let mut builder = TestBuilder::new(
        MemoryStorageBuilder::default(),
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let (backup_validator_pub_key, source_storage) = builder
        .validator_storages
        .iter()
        .next()
        .map(|(k, v)| (*k, v.clone()))
        .expect("at least one validator");
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");
    let validator_secret = builder
        .validator_key_pairs
        .get(&backup_validator_pub_key)
        .expect("validator key pair")
        .copy();

    let cert1 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    let cert2 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    (backup_dir, validator_secret, cert1, cert2)
}

// ── Memory tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_memory_no_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = memory_setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_memory_backup(&backup_dir, &namespace).await;

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    };
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("memory observer worker should apply the block");
}

#[tokio::test]
async fn test_memory_mismatched_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = memory_setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_memory_backup(&backup_dir, &namespace).await;

    let mismatched_key = ValidatorKeypair::generate().secret_key;
    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(mismatched_key));
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("memory mismatched-key worker should apply the block");
}

#[tokio::test]
async fn test_memory_stale_backup_catches_up_with_own_key() {
    let (backup_dir, validator_secret, cert1, cert2) =
        memory_setup_stale_backup_and_two_certs().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_memory_backup(&backup_dir, &namespace).await;

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(validator_secret));
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("memory stale-backup worker should apply block 1");

    restored_worker
        .fully_handle_certificate_with_notifications(cert2, &())
        .await
        .expect("memory stale-backup worker should apply block 2");
}

// ── RocksDB backend helpers ────────────────────────────────────────────────────

#[cfg(feature = "rocksdb")]
use crate::test_utils::RocksDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use linera_views::{
    rocks_db::{PathWithGuard, RocksDbDatabase},
    store::TestKeyValueDatabase as _,
};
#[cfg(feature = "rocksdb")]
use rocksdb::{
    backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
    Env,
};

/// Restores a RocksDB backup into `restore_base/<namespace>` and returns a fresh
/// `DbStorage` connected to it.
#[cfg(feature = "rocksdb")]
async fn restore_rocksdb_backup(
    backup_dir: &TempDir,
    namespace: &str,
) -> DbStorage<RocksDbDatabase, TestClock> {
    let restore_base = TempDir::new().expect("restore base");
    let restore_ns_path = restore_base.path().join(namespace);
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
    let mut config = RocksDbDatabase::new_test_config()
        .await
        .expect("test config");
    config.inner_config.path_with_guard = PathWithGuard::new(restore_base.keep());
    DbStorage::<RocksDbDatabase, TestClock>::connect_for_testing(
        config,
        namespace,
        None,
        TestClock::new(),
    )
    .await
    .expect("connect for testing")
}

#[cfg(feature = "rocksdb")]
async fn rocksdb_setup_backup_and_next_cert(
) -> (TempDir, linera_chain::types::ConfirmedBlockCertificate) {
    let mut builder = TestBuilder::new(
        RocksDbStorageBuilder::new().await,
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let source_storage = builder
        .validator_storages
        .values()
        .next()
        .expect("at least one validator")
        .clone();
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");

    let cert1 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    (backup_dir, cert1)
}

#[cfg(feature = "rocksdb")]
async fn rocksdb_setup_stale_backup_and_two_certs() -> (
    TempDir,
    ValidatorSecretKey,
    linera_chain::types::ConfirmedBlockCertificate,
    linera_chain::types::ConfirmedBlockCertificate,
) {
    let mut builder = TestBuilder::new(
        RocksDbStorageBuilder::new().await,
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let (backup_validator_pub_key, source_storage) = builder
        .validator_storages
        .iter()
        .next()
        .map(|(k, v)| (*k, v.clone()))
        .expect("at least one validator");
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");
    let validator_secret = builder
        .validator_key_pairs
        .get(&backup_validator_pub_key)
        .expect("validator key pair")
        .copy();

    let cert1 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    let cert2 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    (backup_dir, validator_secret, cert1, cert2)
}

// ── RocksDB tests ─────────────────────────────────────────────────────────────

/// A WorkerState with no key (observer mode) booted from another validator's backup can
/// apply the next committed block.
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_no_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = rocksdb_setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_rocksdb_backup(&backup_dir, &namespace).await;

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    };
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("observer worker should apply the block");
}

/// A WorkerState with a mismatched keypair booted from another validator's backup can
/// apply the next committed block.
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_mismatched_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = rocksdb_setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_rocksdb_backup(&backup_dir, &namespace).await;

    let mismatched_key = ValidatorKeypair::generate().secret_key;
    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(mismatched_key));
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("mismatched-key worker should apply the block");
}

/// A WorkerState restored from a stale backup using the original validator's own key can
/// sequentially apply the blocks it missed and catch up to the current chain tip.
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_stale_backup_catches_up_with_own_key() {
    let (backup_dir, validator_secret, cert1, cert2) =
        rocksdb_setup_stale_backup_and_two_certs().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_rocksdb_backup(&backup_dir, &namespace).await;

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(validator_secret));
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("stale-backup worker should apply block 1");

    restored_worker
        .fully_handle_certificate_with_notifications(cert2, &())
        .await
        .expect("stale-backup worker should apply block 2");
}

// ── ScyllaDB backend helpers ───────────────────────────────────────────────────

#[cfg(feature = "scylladb")]
use crate::test_utils::ScyllaDbStorageBuilder;
#[cfg(feature = "scylladb")]
use linera_views::{
    backends::scylla_db::ScyllaDbDatabaseInternal,
    scylla_db::ScyllaDbDatabase,
    store::{KeyValueDatabase as _, TestKeyValueDatabase as _, WritableKeyValueStore as _},
};

/// Deserializes a ScyllaDB backup from `dir` into a fresh namespace and returns
/// a `DbStorage` connected to it.
#[cfg(feature = "scylladb")]
async fn restore_scylladb_backup(
    backup_dir: &TempDir,
    namespace: &str,
) -> DbStorage<ScyllaDbDatabase, TestClock> {
    use std::collections::BTreeMap;

    use linera_views::batch::{Batch, WriteOperation};

    let encoded =
        std::fs::read(backup_dir.path().join("scylladb_backup.bcs")).expect("read scylladb backup");
    let snapshot: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>> =
        bcs::from_bytes(&encoded).expect("deserialize scylladb backup");

    let config = ScyllaDbDatabase::new_test_config()
        .await
        .expect("scylladb test config");
    let inner_config = config.inner_config.clone();

    ScyllaDbDatabaseInternal::create(&inner_config, namespace)
        .await
        .expect("create scylladb namespace");
    {
        let db = ScyllaDbDatabaseInternal::connect(&inner_config, namespace)
            .await
            .expect("connect to scylladb inner");
        for (big_root_key, kv_pairs) in snapshot {
            // big_root_key in the dump = [0] ++ actual_root_key (see get_big_root_key).
            // open_shared re-applies get_big_root_key, so we strip the leading byte.
            let actual_root_key = &big_root_key[1..];
            let store = db
                .open_shared(actual_root_key)
                .expect("open scylladb store");
            if !kv_pairs.is_empty() {
                let batch = Batch {
                    operations: kv_pairs
                        .into_iter()
                        .map(|(key, value)| WriteOperation::Put { key, value })
                        .collect(),
                };
                store
                    .write_batch(batch)
                    .await
                    .expect("write scylladb batch");
            }
        }
    }

    DbStorage::<ScyllaDbDatabase, TestClock>::connect_for_testing(
        config,
        namespace,
        None,
        TestClock::new(),
    )
    .await
    .expect("connect scylladb storage for testing")
}

#[cfg(feature = "scylladb")]
async fn scylladb_setup_backup_and_next_cert(
) -> (TempDir, linera_chain::types::ConfirmedBlockCertificate) {
    let mut builder = TestBuilder::new(
        ScyllaDbStorageBuilder::default(),
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let source_storage = builder
        .validator_storages
        .values()
        .next()
        .expect("at least one validator")
        .clone();
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage
        .backup_to(backup_dir.path())
        .expect("scylladb backup");

    let cert1 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    (backup_dir, cert1)
}

#[cfg(feature = "scylladb")]
async fn scylladb_setup_stale_backup_and_two_certs() -> (
    TempDir,
    ValidatorSecretKey,
    linera_chain::types::ConfirmedBlockCertificate,
    linera_chain::types::ConfirmedBlockCertificate,
) {
    let mut builder = TestBuilder::new(
        ScyllaDbStorageBuilder::default(),
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let (backup_validator_pub_key, source_storage) = builder
        .validator_storages
        .iter()
        .next()
        .map(|(k, v)| (*k, v.clone()))
        .expect("at least one validator");
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage
        .backup_to(backup_dir.path())
        .expect("scylladb backup");
    let validator_secret = builder
        .validator_key_pairs
        .get(&backup_validator_pub_key)
        .expect("validator key pair")
        .copy();

    let cert1 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    let cert2 = chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    (backup_dir, validator_secret, cert1, cert2)
}

// ── ScyllaDB tests ────────────────────────────────────────────────────────────

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_scylladb_no_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = scylladb_setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_scylladb_backup(&backup_dir, &namespace).await;

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    };
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("scylladb observer worker should apply the block");
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_scylladb_mismatched_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = scylladb_setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_scylladb_backup(&backup_dir, &namespace).await;

    let mismatched_key = ValidatorKeypair::generate().secret_key;
    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(mismatched_key));
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("scylladb mismatched-key worker should apply the block");
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_scylladb_stale_backup_catches_up_with_own_key() {
    let (backup_dir, validator_secret, cert1, cert2) =
        scylladb_setup_stale_backup_and_two_certs().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_scylladb_backup(&backup_dir, &namespace).await;

    let config = ChainWorkerConfig {
        allow_inactive_chains: true,
        ..ChainWorkerConfig::default()
    }
    .with_key_pair(Some(validator_secret));
    let restored_worker = WorkerState::new(restored_storage, config, None);

    restored_worker
        .fully_handle_certificate_with_notifications(cert1, &())
        .await
        .expect("scylladb stale-backup worker should apply block 1");

    restored_worker
        .fully_handle_certificate_with_notifications(cert2, &())
        .await
        .expect("scylladb stale-backup worker should apply block 2");
}
