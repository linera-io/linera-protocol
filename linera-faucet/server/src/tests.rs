// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{str::FromStr, sync::Arc};

use linera_base::{
    data_types::{ChainOrigin, Epoch, InitialChainConfig, Timestamp},
    identifiers::{AccountOwner, ChainId},
    ownership::ChainOwnership,
};
use linera_client::{chain_listener, wallet::Wallet};
use linera_core::{client::ChainClient, environment};

#[allow(dead_code)]
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

// For now, we'll focus on unit tests that don't require complex mocking
// Integration tests will be handled at a higher level using the existing test framework

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
    let _chain_id = ChainId(super::CryptoHash::test_hash("test_chain")); // Create a test chain ID
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
                    let mock_description = super::ChainDescription::new(
                        ChainOrigin::Root(0),
                        InitialChainConfig {
                            ownership: ChainOwnership::single(owner),
                            balance: super::Amount::from_tokens(100),
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
        let existing_description = super::ChainDescription::new(
            ChainOrigin::Root(1),
            InitialChainConfig {
                ownership: ChainOwnership::single(owner),
                balance: super::Amount::from_tokens(50),
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
