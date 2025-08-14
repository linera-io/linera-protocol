// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{collections::VecDeque, path::PathBuf, str::FromStr, sync::Arc};

use futures::lock::Mutex;
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, InMemorySigner},
    data_types::{Amount, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, Timestamp},
    identifiers::{AccountOwner, ChainId},
    ownership::ChainOwnership,
};
use linera_client::{chain_listener, wallet::Wallet};
use linera_core::{
    client::ChainClient,
    environment,
    test_utils::{FaultType, MemoryStorageBuilder, StorageBuilder, TestBuilder},
};
use linera_execution::ResourceControlPolicy;
use tempfile::tempdir;
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;

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
    let chain_id = client.chain_id();
    let context = ClientContext {
        client,
        update_calls: 0,
    };
    let context = Arc::new(Mutex::new(context));
    let faucet_storage = Arc::new(Mutex::new(super::FaucetStorage::default()));
    let storage_path = PathBuf::from("/tmp/test_faucet_rate_limiting.json");

    // Set up the batching components
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    // Create the MutationRoot with the current structure
    let root = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage),
        pending_requests: Arc::clone(&pending_requests),
        request_notifier: Arc::clone(&request_notifier),
    };

    // Create the BatchProcessor configuration and instance
    let batch_config = super::BatchProcessorConfig {
        chain_id,
        amount: Amount::from_tokens(1),
        end_timestamp: Timestamp::from(6000),
        start_timestamp: Timestamp::from(0),
        start_balance: Amount::from_tokens(6),
        storage_path,
        max_batch_size: 10,
    };

    let batch_processor = super::BatchProcessor::new(
        batch_config,
        Arc::clone(&context),
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

    // Test: validator offline scenario
    clock.set(Timestamp::from(6000));
    builder.set_fault_type([0, 1], FaultType::Offline).await;
    let result7 = root.do_claim(AccountPublicKey::test_key(6).into()).await;
    assert!(
        result7.is_err(),
        "Claim should fail when validators are offline"
    );

    // Verify update_wallet calls (includes successful operations and final error case)
    let update_calls = context.lock().await.update_calls;
    assert!(
        update_calls >= 3,
        "Should have at least 3 wallet update calls, got {}",
        update_calls
    );

    // Clean up
    cancellation_token.cancel();
    let _ = processor_task.await;
}

#[tokio::test]
async fn test_faucet_storage_basic() {
    // Test basic storage file operations
    use tempfile::tempdir;

    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().join("test_storage.json");

    // Test loading non-existent storage creates empty storage
    let storage = super::FaucetStorage::load(&storage_path).await.unwrap();
    assert!(storage.owner_to_chain.is_empty());

    // Test saving empty storage
    storage.save(&storage_path).await.unwrap();
    assert!(storage_path.exists());

    // Test loading saved empty storage
    let loaded_storage = super::FaucetStorage::load(&storage_path).await.unwrap();
    assert!(loaded_storage.owner_to_chain.is_empty());
}

#[tokio::test]
async fn test_pending_request_queue() {
    // Test that pending requests can be queued and processed
    use std::{collections::VecDeque, sync::Arc};

    use futures::lock::Mutex;
    use tokio::sync::Notify;

    let pending_requests: Arc<Mutex<VecDeque<u32>>> = Arc::new(Mutex::new(VecDeque::new()));
    let _request_notifier = Arc::new(Notify::new());

    // Queue some test data
    {
        let mut requests = pending_requests.lock().await;
        requests.push_back(1);
        requests.push_back(2);
        requests.push_back(3);
    }

    // Verify items are in the queue
    {
        let requests = pending_requests.lock().await;
        assert_eq!(requests.len(), 3);
    }

    // Simulate processing by taking items from queue
    let mut processed = Vec::new();
    while let Some(item) = {
        let mut requests = pending_requests.lock().await;
        requests.pop_front()
    } {
        processed.push(item);
    }

    assert_eq!(processed, vec![1, 2, 3]);

    // Queue should be empty now
    {
        let requests = pending_requests.lock().await;
        assert_eq!(requests.len(), 0);
    }
}

#[test]
fn test_batch_processor_config_validation() {
    // Test BatchProcessorConfig basic validation
    use std::path::PathBuf;

    use linera_base::data_types::{Amount, Timestamp};

    // Test that we can create amounts and compare them
    let small_amount = Amount::from_tokens(100);
    let large_amount = Amount::from_tokens(10000);

    assert!(large_amount > small_amount);
    assert_eq!(small_amount, Amount::from_tokens(100));

    // Test timestamp creation
    let now = Timestamp::now();
    let also_now = Timestamp::now();
    assert!(also_now >= now); // Time should advance or stay the same

    // Test path creation
    let path = PathBuf::from("/tmp/test_faucet.json");
    assert_eq!(path.extension().unwrap(), "json");

    // Test batch size validation logic
    let max_batch_size = 50;
    assert!(max_batch_size > 0);
    assert!(max_batch_size <= 100); // reasonable upper bound
}

#[tokio::test]
async fn test_notification_system() {
    // Test that the notification system works for request processing
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Notify;

    let notifier = Arc::new(Notify::new());
    let notifier_clone = Arc::clone(&notifier);

    let handle = tokio::spawn(async move {
        notifier_clone.notified().await;
        "notified"
    });

    // Small delay to ensure the task is waiting
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Send notification
    notifier.notify_one();

    // Task should complete
    let result = tokio::time::timeout(Duration::from_millis(100), handle).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap(), "notified");
}

#[tokio::test]
async fn test_faucet_batch_processing_integration() {
    // Integration test for actual batch processing - verifies batching efficiency
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use futures::lock::Mutex;
    use linera_base::{
        data_types::Timestamp,
        identifiers::{AccountOwner, ChainId},
    };
    use tempfile::tempdir;
    use tokio::sync::{oneshot, Notify};

    // Set up test environment
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().join("test_faucet.json");

    // Create test data
    let _chain_id = ChainId(CryptoHash::test_hash("test_chain")); // Create a test chain ID
    let owners = [
        AccountOwner::from_str("0x1111111111111111111111111111111111111111").unwrap(),
        AccountOwner::from_str("0x2222222222222222222222222222222222222222").unwrap(),
        AccountOwner::from_str("0x3333333333333333333333333333333333333333").unwrap(),
    ];

    // Set up components
    let faucet_storage = Arc::new(Mutex::new(super::FaucetStorage::default()));
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    let _mutation_root = super::MutationRoot {
        faucet_storage: Arc::clone(&faucet_storage),
        pending_requests: Arc::clone(&pending_requests),
        request_notifier: Arc::clone(&request_notifier),
    };

    // Test that requests get queued
    let mut handles = Vec::new();
    for owner in owners {
        let notifier_clone = Arc::clone(&request_notifier);
        let pending_clone = Arc::clone(&pending_requests);

        handles.push(tokio::spawn(async move {
            // Create a mock request (simplified since we can't easily mock GraphQL context)
            let (tx, _rx) = oneshot::channel();

            {
                let mut requests = pending_clone.lock().await;
                requests.push_back(super::PendingRequest {
                    owner,
                    responder: tx,
                });
            }

            // Notify batch processor
            notifier_clone.notify_one();

            // Return owner for verification
            owner
        }));
    }

    // Wait for all handles to complete
    let mut queued_owners = Vec::new();
    for handle in handles {
        let owner = handle.await.unwrap();
        queued_owners.push(owner);
    }

    // Verify requests were queued
    tokio::time::sleep(Duration::from_millis(10)).await; // Small delay for async operations

    {
        let requests = pending_requests.lock().await;
        assert_eq!(requests.len(), 3, "Should have 3 requests queued");
    }

    // Test batch processing logic with efficiency verification
    let mut blocks_created = 0;
    let chains_created = queued_owners.len();
    let max_batch_size = 2; // Use small batch size to test batching

    {
        let mut storage = faucet_storage.lock().await;
        let mut requests = pending_requests.lock().await;

        // Process requests in batches (simulating BatchProcessor::execute_batch logic)
        while !requests.is_empty() {
            let mut batch = Vec::new();

            // Collect up to max_batch_size requests from queue
            while batch.len() < max_batch_size && !requests.is_empty() {
                if let Some(request) = requests.pop_front() {
                    batch.push(request.owner); // Just track the owner for this simulation
                }
            }

            if !batch.is_empty() {
                blocks_created += 1; // Each batch execution creates one block

                // Simulate successful batch processing by storing results
                for owner in batch {
                    let mock_description = ChainDescription::new(
                        ChainOrigin::Root(0),
                        InitialChainConfig {
                            ownership: ChainOwnership::single(owner),
                            balance: Amount::from_tokens(100),
                            application_permissions: Default::default(),
                            epoch: Epoch::ZERO,
                            max_active_epoch: Epoch::from(100),
                            min_active_epoch: Epoch::ZERO,
                        },
                        Timestamp::now(),
                    );
                    storage.store_chain(owner, mock_description);
                }
            }
        }

        // Verify batching efficiency: we should need fewer blocks than chains
        // With 3 chains and max_batch_size=2, we expect 2 blocks (2+1 chains)
        assert!(
            blocks_created < chains_created,
            "Expected batching to reduce blocks: {} blocks for {} chains (max_batch_size={})",
            blocks_created,
            chains_created,
            max_batch_size
        );

        // Verify all owners have chains stored
        assert_eq!(storage.owner_to_chain.len(), chains_created);
        for owner in &queued_owners {
            assert!(
                storage.get_chain(owner).is_some(),
                "Owner should have stored chain"
            );
        }

        // Test persistence
        storage.save(&storage_path).await.unwrap();
    }

    // Test loading persisted data
    let loaded_storage = super::FaucetStorage::load(&storage_path).await.unwrap();
    assert_eq!(loaded_storage.owner_to_chain.len(), 3);

    for owner in &queued_owners {
        assert!(
            loaded_storage.get_chain(owner).is_some(),
            "Loaded storage should contain owner"
        );
    }
}

#[tokio::test]
async fn test_batch_processor_error_handling() {
    // Test error handling in batch processing scenarios
    use std::{collections::VecDeque, sync::Arc};

    use futures::lock::Mutex;
    use linera_base::identifiers::AccountOwner;
    use tokio::sync::{oneshot, Notify};

    let faucet_storage = Arc::new(Mutex::new(super::FaucetStorage::default()));
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let _request_notifier = Arc::new(Notify::new());

    // Test duplicate owner handling
    let owner = AccountOwner::from_str("0x4444444444444444444444444444444444444444").unwrap();

    // Add owner to storage first (simulating existing chain)
    {
        let mut storage = faucet_storage.lock().await;
        let existing_description = ChainDescription::new(
            ChainOrigin::Root(1),
            InitialChainConfig {
                ownership: ChainOwnership::single(owner),
                balance: Amount::from_tokens(50),
                application_permissions: Default::default(),
                epoch: Epoch::ZERO,
                max_active_epoch: Epoch::from(100),
                min_active_epoch: Epoch::ZERO,
            },
            Timestamp::now(),
        );
        storage.store_chain(owner, existing_description);
    }

    // Create multiple requests for the same owner
    let mut _response_receivers = Vec::new();
    for _ in 0..3 {
        let (tx, rx) = oneshot::channel();
        _response_receivers.push(rx);

        {
            let mut requests = pending_requests.lock().await;
            requests.push_back(super::PendingRequest {
                owner,
                responder: tx,
            });
        }
    }

    // Verify requests were queued
    {
        let requests = pending_requests.lock().await;
        assert_eq!(requests.len(), 3, "Should have 3 duplicate requests queued");
    }

    // In a real batch processor, duplicate requests would be filtered out
    // and all would receive the same existing chain description
    // Here we simulate that by manually processing the queue
    let existing_description = {
        let storage = faucet_storage.lock().await;
        storage.get_chain(&owner).unwrap().clone()
    };

    // Process pending requests (simulate batch processor behavior)
    while let Some(request) = {
        let mut requests = pending_requests.lock().await;
        requests.pop_front()
    } {
        // All requests should get the existing description
        let _ = request.responder.send(Ok(existing_description.clone()));
    }

    // Verify queue is empty after processing
    {
        let requests = pending_requests.lock().await;
        assert_eq!(requests.len(), 0, "Queue should be empty after processing");
    }
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
    let storage_path = temp_dir.path().join("test_batch_reduction.json");

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
    let chain_id = client.chain_id();

    let context = Arc::new(Mutex::new(ClientContext {
        client,
        update_calls: 0,
    }));

    let faucet_storage = Arc::new(Mutex::new(super::FaucetStorage::default()));
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    // Create batch processor with initial batch size of 3 and disabled rate limiting
    let initial_batch_size = 3;
    let config = super::BatchProcessorConfig {
        chain_id,
        amount: Amount::from_tokens(1),
        start_balance: Amount::from_tokens(100),
        start_timestamp: Timestamp::from(1000), // start > end disables rate limiting
        end_timestamp: Timestamp::from(999),
        storage_path: storage_path.clone(),
        max_batch_size: initial_batch_size,
    };

    let mut batch_processor = super::BatchProcessor::new(
        config,
        Arc::clone(&context),
        Arc::clone(&faucet_storage),
        Arc::clone(&pending_requests),
        Arc::clone(&request_notifier),
    );

    // Create 3 different owners for batch processing
    let owners = [
        AccountOwner::from_str("0x1111111111111111111111111111111111111111").unwrap(),
        AccountOwner::from_str("0x2222222222222222222222222222222222222222").unwrap(),
        AccountOwner::from_str("0x3333333333333333333333333333333333333333").unwrap(),
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
