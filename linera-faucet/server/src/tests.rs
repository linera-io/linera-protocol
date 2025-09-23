// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{collections::VecDeque, sync::Arc};

use futures::lock::Mutex;
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, InMemorySigner, TestString},
    data_types::{Amount, Timestamp},
    identifiers::{AccountOwner, ChainId},
};
use linera_client::{chain_listener, wallet::Wallet};
use linera_core::{
    client::ChainClient,
    environment,
    test_utils::{MemoryStorageBuilder, StorageBuilder, TestBuilder},
};
use linera_execution::ResourceControlPolicy;
use tempfile::tempdir;
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;

use crate::database::FaucetDatabase;

struct ClientContext {
    client: ChainClient<environment::Test>,
    update_calls: usize,
}

impl chain_listener::ClientContext for ClientContext {
    type Environment = environment::Test;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn storage(&self) -> &environment::TestStorage {
        self.client.storage_client()
    }

    fn client(&self) -> &Arc<linera_core::client::Client<environment::Test>> {
        unimplemented!()
    }

    fn timing_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<(u64, linera_core::client::TimingType)>> {
        None
    }

    fn make_chain_client(&self, chain_id: ChainId) -> ChainClient<environment::Test> {
        assert_eq!(chain_id, self.client.chain_id());
        self.client.clone()
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        _: ChainId,
        _: Option<AccountOwner>,
        _: Timestamp,
    ) -> Result<(), linera_client::Error> {
        self.update_calls += 1;
        Ok(())
    }

    async fn update_wallet(
        &mut self,
        _: &ChainClient<environment::Test>,
    ) -> Result<(), linera_client::Error> {
        self.update_calls += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_faucet_rate_limiting() {
    let storage_builder = MemoryStorageBuilder::default();
    let keys = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys).await.unwrap();
    let client = builder
        .add_root_chain(1, Amount::from_tokens(6))
        .await
        .unwrap();
    let context = ClientContext {
        client: client.clone(),
        update_calls: 0,
    };
    let context = Arc::new(Mutex::new(context));
    let temp_dir = tempdir().unwrap();
    let faucet_storage = Arc::new(
        FaucetDatabase::new(&temp_dir.path().join("test_faucet_rate_limiting.sqlite"))
            .await
            .unwrap(),
    );

    // Set up the batching components
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    // Create the MutationRoot with the current structure
    let root = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage),
        pending_requests: Arc::clone(&pending_requests),
        request_notifier: Arc::clone(&request_notifier),
        storage: client.storage_client().clone(),
    };

    // Create the BatchProcessor configuration and instance
    let batch_config = super::BatchProcessorConfig {
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
        max_batch_size: 1,
    };

    let batch_processor = super::BatchProcessor::new(
        batch_config,
        Arc::clone(&context),
        client,
        Arc::clone(&faucet_storage),
        Arc::clone(&pending_requests),
        Arc::clone(&request_notifier),
    );

    // Start the batch processor in the background
    let cancellation_token = CancellationToken::new();
    let processor_task = {
        let mut batch_processor = batch_processor;
        let token = cancellation_token.clone();
        tokio::spawn(async move { batch_processor.run(token).await })
    };

    // The faucet is releasing one token every 1000 microseconds. So at 1000 one claim should
    // succeed. At 3000, two more should have been unlocked.

    // Test: at time 999, no claims should succeed due to rate limiting
    clock.set(Timestamp::from(999));
    let result1 = root.do_claim(AccountPublicKey::test_key(0).into()).await;
    assert!(
        result1.is_err(),
        "Claim should fail before rate limit allows"
    );

    // Test: at time 1000, first claim should succeed
    clock.set(Timestamp::from(1000));
    let result2 = root.do_claim(AccountPublicKey::test_key(1).into()).await;
    assert!(result2.is_ok(), "First claim should succeed at time 1000");

    // Test: immediate second claim should fail (rate limit)
    let result3 = root.do_claim(AccountPublicKey::test_key(2).into()).await;
    assert!(
        result3.is_err(),
        "Second immediate claim should fail due to rate limit"
    );

    // Test: at time 3000, more tokens should be available
    clock.set(Timestamp::from(3000));
    let result4 = root.do_claim(AccountPublicKey::test_key(3).into()).await;
    assert!(result4.is_ok(), "Third claim should succeed at time 3000");

    let result5 = root.do_claim(AccountPublicKey::test_key(4).into()).await;
    assert!(result5.is_ok(), "Fourth claim should succeed at time 3000");

    // Test: too many claims should eventually fail
    let result6 = root.do_claim(AccountPublicKey::test_key(5).into()).await;
    assert!(
        result6.is_err(),
        "Fifth claim should fail due to rate limit"
    );

    // Verify update_wallet calls (includes successful operations and final error case)
    let update_calls = context.lock().await.update_calls;
    assert_eq!(update_calls, 3);

    // Clean up
    cancellation_token.cancel();
    let _ = processor_task.await;
}

#[test]
fn test_multiply() {
    use super::multiply;

    assert_eq!(
        multiply((1 << 127) + (1 << 63), 1 << 63),
        [1 << 62, 1 << 62, 0]
    );
    assert_eq!(multiply(u128::MAX, u64::MAX), [u64::MAX - 1, u64::MAX, 1]);
}

#[tokio::test]
async fn test_batch_size_reduction_on_limit_errors() {
    // Test that the batch processor reduces batch size when hitting BlockTooLarge limit

    // Set up test environment
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().join("test_batch_reduction.sqlite");

    let storage_builder = MemoryStorageBuilder::default();
    let keys = InMemorySigner::new(None);

    // Create a restrictive policy that limits block size to trigger BlockTooLarge
    let restrictive_policy = ResourceControlPolicy {
        maximum_block_size: 800,
        ..ResourceControlPolicy::default()
    };

    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await
        .unwrap()
        .with_policy(restrictive_policy);

    let client = builder
        .add_root_chain(1, Amount::from_tokens(100))
        .await
        .unwrap();

    let context = Arc::new(Mutex::new(ClientContext {
        client: client.clone(),
        update_calls: 0,
    }));

    let faucet_storage = Arc::new(FaucetDatabase::new(&storage_path).await.unwrap());
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    // Create batch processor with initial batch size of 3 and disabled rate limiting
    let initial_batch_size = 3;
    let config = super::BatchProcessorConfig {
        amount: Amount::from_tokens(1),
        start_balance: Amount::from_tokens(100),
        start_timestamp: Timestamp::from(1000), // start > end disables rate limiting
        end_timestamp: Timestamp::from(999),
        max_batch_size: initial_batch_size,
    };

    let mut batch_processor = super::BatchProcessor::new(
        config,
        Arc::clone(&context),
        client,
        Arc::clone(&faucet_storage),
        Arc::clone(&pending_requests),
        Arc::clone(&request_notifier),
    );

    // Create 3 different owners for batch processing
    let owners = [
        CryptoHash::new(&TestString("owner1".into())).into(),
        CryptoHash::new(&TestString("owner2".into())).into(),
        CryptoHash::new(&TestString("owner3".into())).into(),
    ];

    // Create and queue 3 pending requests
    {
        let mut pending_requests_guard = pending_requests.lock().await;
        for owner in owners {
            let (tx, _rx) = oneshot::channel();
            pending_requests_guard.push_back(super::PendingRequest {
                owner,
                responder: tx,
            });
        }
    }

    // Execute the batch - this triggers BlockTooLarge error
    batch_processor
        .process_batch()
        .await
        .expect("Batch processing should succeed");

    // Now the batch size should be reduced.
    assert!(batch_processor.config.max_batch_size < initial_batch_size);
}

#[tokio::test]
async fn test_faucet_persistence() {
    // Test that the faucet correctly persists chain IDs and retrieves them after restart.
    // This ensures the database is working correctly across sessions.

    let storage_builder = MemoryStorageBuilder::default();
    let keys = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys).await.unwrap();
    let client = builder
        .add_root_chain(1, Amount::from_tokens(6))
        .await
        .unwrap();

    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().join("test_faucet_persistence.sqlite");

    // Create first faucet instance
    let faucet_storage = Arc::new(FaucetDatabase::new(&storage_path).await.unwrap());

    let context = ClientContext {
        client: client.clone(),
        update_calls: 0,
    };
    let context = Arc::new(Mutex::new(context));

    // Set up the first MutationRoot instance
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    let root = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage),
        pending_requests: Arc::clone(&pending_requests),
        request_notifier: Arc::clone(&request_notifier),
        storage: client.storage_client().clone(),
    };

    // Create the BatchProcessor configuration
    let batch_config = super::BatchProcessorConfig {
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
        max_batch_size: 1,
    };

    let batch_processor = super::BatchProcessor::new(
        batch_config,
        Arc::clone(&context),
        client.clone(),
        Arc::clone(&faucet_storage),
        Arc::clone(&pending_requests),
        Arc::clone(&request_notifier),
    );

    // Start the batch processor
    let cancellation_token = CancellationToken::new();
    let processor_task = {
        let mut batch_processor = batch_processor;
        let token = cancellation_token.clone();
        tokio::spawn(async move { batch_processor.run(token).await })
    };

    // Set time to allow claims
    clock.set(Timestamp::from(1000));

    // Make first claim with a specific owner
    let test_owner_1 = AccountPublicKey::test_key(42).into();
    let test_owner_2 = AccountPublicKey::test_key(43).into();

    // Claim chains for two different owners
    let chain_1 = root
        .do_claim(test_owner_1)
        .await
        .expect("First claim should succeed");

    clock.set(Timestamp::from(2000));
    let chain_2 = root
        .do_claim(test_owner_2)
        .await
        .expect("Second claim should succeed");

    // Verify that immediate re-claims return the same chains
    let chain_1_again = root
        .do_claim(test_owner_1)
        .await
        .expect("Re-claim should return existing chain");
    assert_eq!(
        chain_1.id(),
        chain_1_again.id(),
        "Should return same chain for same owner"
    );

    let chain_2_again = root
        .do_claim(test_owner_2)
        .await
        .expect("Re-claim should return existing chain");
    assert_eq!(
        chain_2.id(),
        chain_2_again.id(),
        "Should return same chain for same owner"
    );

    // Store the chain IDs for later comparison
    let chain_1_id = chain_1.id();
    let chain_2_id = chain_2.id();

    // Stop the batch processor
    cancellation_token.cancel();
    let _ = processor_task.await;

    // Drop the first faucet instance to simulate shutdown
    drop(root);
    drop(faucet_storage);

    // Create a new faucet instance with the same database path (simulating restart)
    let faucet_storage_2 = Arc::new(FaucetDatabase::new(&storage_path).await.unwrap());

    // Set up the new MutationRoot instance
    let pending_requests_2 = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier_2 = Arc::new(Notify::new());

    let root_2 = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage_2),
        pending_requests: Arc::clone(&pending_requests_2),
        request_notifier: Arc::clone(&request_notifier_2),
        storage: client.storage_client().clone(),
    };

    // Create new batch processor for the second instance
    let batch_config_2 = super::BatchProcessorConfig {
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
        max_batch_size: 1,
    };
    let batch_processor_2 = super::BatchProcessor::new(
        batch_config_2,
        Arc::clone(&context),
        client.clone(),
        Arc::clone(&faucet_storage_2),
        Arc::clone(&pending_requests_2),
        Arc::clone(&request_notifier_2),
    );

    // Start the new batch processor
    let cancellation_token_2 = CancellationToken::new();
    let processor_task_2 = {
        let mut batch_processor = batch_processor_2;
        let token = cancellation_token_2.clone();
        tokio::spawn(async move { batch_processor.run(token).await })
    };

    // Verify that the new instance returns the same chain IDs for the same owners
    let chain_1_after_restart = root_2
        .do_claim(test_owner_1)
        .await
        .expect("Should return existing chain after restart");
    assert_eq!(
        chain_1_id,
        chain_1_after_restart.id(),
        "Chain ID should be preserved after restart for owner 1"
    );

    let chain_2_after_restart = root_2
        .do_claim(test_owner_2)
        .await
        .expect("Should return existing chain after restart");
    assert_eq!(
        chain_2_id,
        chain_2_after_restart.id(),
        "Chain ID should be preserved after restart for owner 2"
    );

    // Verify that a new owner can still claim a new chain after restart
    clock.set(Timestamp::from(3000));
    let test_owner_3 = AccountPublicKey::test_key(44).into();
    let chain_3 = root_2
        .do_claim(test_owner_3)
        .await
        .expect("New owner should be able to claim after restart");

    // Verify the new chain is different from the existing ones
    assert_ne!(
        chain_3.id(),
        chain_1_id,
        "New chain should have different ID"
    );
    assert_ne!(
        chain_3.id(),
        chain_2_id,
        "New chain should have different ID"
    );

    // Clean up
    cancellation_token_2.cancel();
    let _ = processor_task_2.await;
}

#[tokio::test]
async fn test_blockchain_sync_after_database_deletion() {
    // Test that the faucet correctly syncs with blockchain after database deletion.
    // This verifies that the blockchain synchronization can restore chain mappings from
    // the blockchain history when the database is lost or corrupted.

    let storage_builder = MemoryStorageBuilder::default();
    let keys = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    clock.set(Timestamp::from(0));
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys).await.unwrap();
    let client = builder
        .add_root_chain(1, Amount::from_tokens(6))
        .await
        .unwrap();

    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().join("test_blockchain_sync.sqlite");

    // === PHASE 1: Create chains with first faucet instance ===
    let faucet_storage = Arc::new(FaucetDatabase::new(&storage_path).await.unwrap());
    let context = ClientContext {
        client: client.clone(),
        update_calls: 0,
    };
    let context = Arc::new(Mutex::new(context));

    // Set up the first MutationRoot instance
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());
    let root = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage),
        pending_requests: Arc::clone(&pending_requests),
        request_notifier: Arc::clone(&request_notifier),
        storage: client.storage_client().clone(),
    };

    // Create the BatchProcessor configuration
    let batch_config = super::BatchProcessorConfig {
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
        max_batch_size: 1,
    };
    let batch_processor = super::BatchProcessor::new(
        batch_config,
        Arc::clone(&context),
        client.clone(),
        Arc::clone(&faucet_storage),
        Arc::clone(&pending_requests),
        Arc::clone(&request_notifier),
    );

    // Start the batch processor
    let cancellation_token = CancellationToken::new();
    let processor_task = {
        let mut batch_processor = batch_processor;
        let token = cancellation_token.clone();
        tokio::spawn(async move { batch_processor.run(token).await })
    };

    // Set time to allow claims
    clock.set(Timestamp::from(1000));

    // Make claims with specific owners to create chain mappings
    let test_owner_1 = AccountPublicKey::test_key(100).into();
    let test_owner_2 = AccountPublicKey::test_key(101).into();

    // Claim chains for two different owners
    let chain_1 = root
        .do_claim(test_owner_1)
        .await
        .expect("First claim should succeed");

    clock.set(Timestamp::from(2000));
    let chain_2 = root
        .do_claim(test_owner_2)
        .await
        .expect("Second claim should succeed");

    // Store the chain IDs for later comparison
    let chain_1_id = chain_1.id();
    let chain_2_id = chain_2.id();

    // Verify initial state works correctly
    let chain_1_again = root
        .do_claim(test_owner_1)
        .await
        .expect("Re-claim should return existing chain");
    assert_eq!(
        chain_1_id,
        chain_1_again.id(),
        "Should return same chain for same owner initially"
    );

    // Stop the batch processor and clean up first instance
    cancellation_token.cancel();
    let _ = processor_task.await;
    drop(root);
    drop(faucet_storage);

    // === PHASE 2: Delete the database file (simulate data loss) ===
    std::fs::remove_file(&storage_path).expect("Should be able to delete database file");

    // === PHASE 3: Create new faucet instance (should sync from blockchain) ===
    let faucet_storage_2 = Arc::new(FaucetDatabase::new(&storage_path).await.unwrap());

    // CRITICAL: Trigger blockchain sync before using the faucet
    faucet_storage_2
        .sync_with_blockchain(&client)
        .await
        .expect("Blockchain sync should succeed");

    // Set up the new MutationRoot instance
    let pending_requests_2 = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier_2 = Arc::new(Notify::new());

    let root_2 = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage_2),
        pending_requests: Arc::clone(&pending_requests_2),
        request_notifier: Arc::clone(&request_notifier_2),
        storage: client.storage_client().clone(),
    };

    // Create new batch processor for the second instance
    let batch_config_2 = super::BatchProcessorConfig {
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
        max_batch_size: 1,
    };
    let batch_processor_2 = super::BatchProcessor::new(
        batch_config_2,
        Arc::clone(&context),
        client.clone(),
        Arc::clone(&faucet_storage_2),
        Arc::clone(&pending_requests_2),
        Arc::clone(&request_notifier_2),
    );

    // Start the new batch processor
    let cancellation_token_2 = CancellationToken::new();
    let processor_task_2 = {
        let mut batch_processor = batch_processor_2;
        let token = cancellation_token_2.clone();
        tokio::spawn(async move { batch_processor.run(token).await })
    };

    // === PHASE 4: Verify blockchain sync restored the correct mappings ===

    // Test that the blockchain sync correctly restored the chain mappings
    let chain_1_after_sync = root_2
        .do_claim(test_owner_1)
        .await
        .expect("Should return existing chain after blockchain sync");
    assert_eq!(
        chain_1_id,
        chain_1_after_sync.id(),
        "Chain ID should be correctly restored from blockchain for owner 1"
    );

    let chain_2_after_sync = root_2
        .do_claim(test_owner_2)
        .await
        .expect("Should return existing chain after blockchain sync");
    assert_eq!(
        chain_2_id,
        chain_2_after_sync.id(),
        "Chain ID should be correctly restored from blockchain for owner 2"
    );

    // === PHASE 5: Verify new claims still work correctly ===

    // Verify that a completely new owner can still claim a new chain
    clock.set(Timestamp::from(3000));
    let test_owner_3 = AccountPublicKey::test_key(102).into();
    let chain_3 = root_2
        .do_claim(test_owner_3)
        .await
        .expect("New owner should be able to claim after sync");

    // Verify the new chain is different from the existing ones
    assert_ne!(
        chain_3.id(),
        chain_1_id,
        "New chain should have different ID from synced chains"
    );
    assert_ne!(
        chain_3.id(),
        chain_2_id,
        "New chain should have different ID from synced chains"
    );

    // Verify that the new chain mapping is also persisted
    let chain_3_again = root_2
        .do_claim(test_owner_3)
        .await
        .expect("Re-claim should return the new chain");
    assert_eq!(
        chain_3.id(),
        chain_3_again.id(),
        "New chain should be persistent after sync"
    );

    // Clean up
    cancellation_token_2.cancel();
    let _ = processor_task_2.await;
}
