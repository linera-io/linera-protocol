// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![expect(clippy::large_futures)]
//! Phase 0a: empirically confirm that a WorkerState can boot from a RocksDB backup
//! produced by a different validator and then continue processing blocks.

use linera_base::{
    crypto::{InMemorySigner, ValidatorKeypair, ValidatorSecretKey},
    data_types::Amount,
    identifiers::{Account, AccountOwner},
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
    test_utils::{ClientOutcomeResultExt as _, RocksDbStorageBuilder, TestBuilder},
    worker::WorkerState,
};

/// Restores a RocksDB backup into `restore_base/<namespace>` and returns a fresh
/// `DbStorage` connected to it.
async fn restore_backup(
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

/// Sets up a 4-validator test network, commits one block (a transfer from chain 0 to
/// chain 1), takes a RocksDB backup of one validator's storage, and returns the backup
/// directory together with the certificate for a second block ready to be fed to the
/// restored worker.
async fn setup_backup_and_next_cert() -> (TempDir, linera_chain::types::ConfirmedBlockCertificate) {
    let mut builder = TestBuilder::new(
        RocksDbStorageBuilder::new().await,
        4,
        0,
        InMemorySigner::new(None),
    )
    .await
    .expect("test builder");

    // Two chains so there is a real transfer target.
    let chain_a = builder
        .add_root_chain(0, Amount::from_tokens(10))
        .await
        .expect("chain a");
    let chain_b = builder
        .add_root_chain(1, Amount::ZERO)
        .await
        .expect("chain b");

    // Block 0: transfer — all 4 validators propose, vote, and commit this block.
    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // Back up one validator's storage after block 0 is committed.
    let source_storage = builder
        .validator_storages
        .values()
        .next()
        .expect("at least one validator")
        .clone();
    let backup_dir = TempDir::new().expect("backup dir");
    source_storage.backup_to(backup_dir.path()).expect("backup");

    // Block 1: second transfer — all 4 original validators commit this too.
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

/// A WorkerState with no key (observer mode) booted from another validator's backup can
/// apply the next committed block.
#[tokio::test]
async fn test_no_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_backup(&backup_dir, &namespace).await;

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
#[tokio::test]
async fn test_mismatched_key_boots_on_cross_key_backup() {
    let (backup_dir, cert1) = setup_backup_and_next_cert().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_backup(&backup_dir, &namespace).await;

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

/// Sets up a 4-validator test network, commits one block (a transfer from chain 0 to
/// chain 1), takes a RocksDB backup of validator 0's storage together with its secret key,
/// then commits two more blocks that the backup does not contain.
async fn setup_stale_backup_and_two_certs() -> (
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

    // Block 0: all 4 validators sign and commit this block.
    chain_a
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_b.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // Take the backup of validator 0 (keyed by its public key) together with its secret key.
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

    // Blocks 1 and 2: all 4 original validators commit; the backup does not contain these.
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

/// A WorkerState restored from a stale backup using the original validator's own key can
/// sequentially apply the blocks it missed and catch up to the current chain tip.
#[tokio::test]
async fn test_stale_backup_catches_up_with_own_key() {
    let (backup_dir, validator_secret, cert1, cert2) = setup_stale_backup_and_two_certs().await;

    let namespace = generate_test_namespace();
    let restored_storage = restore_backup(&backup_dir, &namespace).await;

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
