// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use futures::lock::Mutex;
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, InMemorySigner, TestString},
    data_types::{Amount, Epoch, Timestamp},
    identifiers::{AccountOwner, ChainId},
};
use linera_client::chain_listener;
use linera_core::{
    client::ChainClient,
    environment,
    test_utils::{MemoryStorageBuilder, StorageBuilder, TestBuilder},
};
use linera_execution::ResourceControlPolicy;
use linera_storage::TestClock;
use tempfile::TempDir;
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    database::FaucetDatabase, BatchProcessor, BatchProcessorConfig, MutationRoot, PendingRequest,
};

struct ClientContext {
    client: ChainClient<environment::Test>,
    update_calls: usize,
}

impl chain_listener::ClientContext for ClientContext {
    type Environment = environment::Test;

    fn wallet(&self) -> &environment::TestWallet {
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

    async fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainClient<environment::Test>, linera_client::Error> {
        assert_eq!(chain_id, self.client.chain_id());
        Ok(self.client.clone())
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        _: ChainId,
        _: Option<AccountOwner>,
        _: Timestamp,
        _: Epoch,
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

/// Holds all components needed by faucet tests after common setup.
struct FaucetTestEnv {
    root: MutationRoot<environment::TestStorage>,
    context: Arc<Mutex<ClientContext>>,
    client: ChainClient<environment::Test>,
    clock: TestClock,
    faucet_storage: Arc<FaucetDatabase>,
    pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
    request_notifier: Arc<Notify>,
    storage_path: PathBuf,
    _temp_dir: TempDir,
}

struct FaucetTestConfig {
    initial_tokens: u128,
    initial_claim_amount: Amount,
    daily_claim_amount: Amount,
    batch_config: BatchProcessorConfig,
}

impl FaucetTestConfig {
    fn new(initial_tokens: u128) -> Self {
        Self {
            initial_tokens,
            initial_claim_amount: Amount::from_tokens(1),
            daily_claim_amount: Amount::ZERO,
            batch_config: BatchProcessorConfig {
                end_timestamp: Timestamp::from(0),
                start_timestamp: Timestamp::from(0),
                start_balance: Amount::from_tokens(initial_tokens),
                max_batch_size: 1,
            },
        }
    }
}

impl FaucetTestEnv {
    async fn new(config: FaucetTestConfig) -> anyhow::Result<Self> {
        let storage_builder = MemoryStorageBuilder::default();
        let keys = InMemorySigner::new(None);
        let clock = storage_builder.clock().clone();
        clock.set(Timestamp::from(0));
        let mut builder = TestBuilder::new(storage_builder, 4, 1, keys).await?;
        let client = builder
            .add_root_chain(1, Amount::from_tokens(config.initial_tokens))
            .await?;

        let context = Arc::new(Mutex::new(ClientContext {
            client: client.clone(),
            update_calls: 0,
        }));

        let temp_dir = tempfile::tempdir()?;
        let storage_path = temp_dir.path().join("faucet.sqlite");
        let faucet_storage = Arc::new(FaucetDatabase::new(&storage_path).await?);

        let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
        let request_notifier = Arc::new(Notify::new());

        let root = MutationRoot {
            faucet_storage: Arc::clone(&faucet_storage),
            pending_requests: Arc::clone(&pending_requests),
            request_notifier: Arc::clone(&request_notifier),
            storage: client.storage_client().clone(),
            initial_claim_amount: config.initial_claim_amount,
            daily_claim_amount: config.daily_claim_amount,
        };

        Ok(Self {
            root,
            context,
            client,
            clock,
            faucet_storage,
            pending_requests,
            request_notifier,
            storage_path,
            _temp_dir: temp_dir,
        })
    }

    /// Spawns the batch processor and returns a handle to stop it.
    fn spawn_processor(&self, batch_config: BatchProcessorConfig) -> BatchProcessorHandle {
        let batch_processor = BatchProcessor::new(
            batch_config,
            Arc::clone(&self.context),
            self.client.clone(),
            Arc::clone(&self.faucet_storage),
            Arc::clone(&self.pending_requests),
            Arc::clone(&self.request_notifier),
        );

        let cancellation_token = CancellationToken::new();
        let task = {
            let mut batch_processor = batch_processor;
            let token = cancellation_token.clone();
            tokio::spawn(async move { batch_processor.run(token).await })
        };

        BatchProcessorHandle {
            cancellation_token,
            task,
        }
    }

    /// Creates a new `MutationRoot` and batch processor handle using the given database,
    /// reusing the existing context and shared state infrastructure.
    fn new_faucet_instance(
        &self,
        faucet_storage: Arc<FaucetDatabase>,
        batch_config: BatchProcessorConfig,
    ) -> (MutationRoot<environment::TestStorage>, BatchProcessorHandle) {
        let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
        let request_notifier = Arc::new(Notify::new());

        let root = MutationRoot {
            faucet_storage: Arc::clone(&faucet_storage),
            pending_requests: Arc::clone(&pending_requests),
            request_notifier: Arc::clone(&request_notifier),
            storage: self.client.storage_client().clone(),
            initial_claim_amount: self.root.initial_claim_amount,
            daily_claim_amount: self.root.daily_claim_amount,
        };

        let batch_processor = BatchProcessor::new(
            batch_config,
            Arc::clone(&self.context),
            self.client.clone(),
            Arc::clone(&faucet_storage),
            Arc::clone(&pending_requests),
            Arc::clone(&request_notifier),
        );

        let cancellation_token = CancellationToken::new();
        let task = {
            let mut batch_processor = batch_processor;
            let token = cancellation_token.clone();
            tokio::spawn(async move { batch_processor.run(token).await })
        };

        (
            root,
            BatchProcessorHandle {
                cancellation_token,
                task,
            },
        )
    }
}

struct BatchProcessorHandle {
    cancellation_token: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl BatchProcessorHandle {
    async fn stop(self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.task.await?;
        Ok(())
    }
}

#[tokio::test]
async fn test_faucet_rate_limiting() -> anyhow::Result<()> {
    let mut config = FaucetTestConfig::new(6);
    config.batch_config.end_timestamp = Timestamp::from(6000);
    let batch_config = config.batch_config.clone();
    let env = FaucetTestEnv::new(config).await?;
    let handle = env.spawn_processor(batch_config);

    // The faucet is releasing one token every 1000 microseconds. So at 1000 one claim should
    // succeed. At 3000, two more should have been unlocked.

    // Test: at time 999, no claims should succeed due to rate limiting
    env.clock.set(Timestamp::from(999));
    let result1 = env
        .root
        .do_claim(AccountPublicKey::test_key(0).into())
        .await;
    assert!(
        result1.is_err(),
        "Claim should fail before rate limit allows"
    );

    // Test: at time 1000, first claim should succeed
    env.clock.set(Timestamp::from(1000));
    let result2 = env
        .root
        .do_claim(AccountPublicKey::test_key(1).into())
        .await;
    assert!(result2.is_ok(), "First claim should succeed at time 1000");

    // Test: immediate second claim should fail (rate limit)
    let result3 = env
        .root
        .do_claim(AccountPublicKey::test_key(2).into())
        .await;
    assert!(
        result3.is_err(),
        "Second immediate claim should fail due to rate limit"
    );

    // Test: at time 3000, more tokens should be available
    env.clock.set(Timestamp::from(3000));
    let result4 = env
        .root
        .do_claim(AccountPublicKey::test_key(3).into())
        .await;
    assert!(result4.is_ok(), "Third claim should succeed at time 3000");

    let result5 = env
        .root
        .do_claim(AccountPublicKey::test_key(4).into())
        .await;
    assert!(result5.is_ok(), "Fourth claim should succeed at time 3000");

    // Test: too many claims should eventually fail
    let result6 = env
        .root
        .do_claim(AccountPublicKey::test_key(5).into())
        .await;
    assert!(
        result6.is_err(),
        "Fifth claim should fail due to rate limit"
    );

    // Verify update_wallet calls (includes successful operations and final error case)
    let update_calls = env.context.lock().await.update_calls;
    assert_eq!(update_calls, 3);

    handle.stop().await
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

#[test_log::test(tokio::test)]
async fn test_batch_size_reduction_on_limit_errors() -> anyhow::Result<()> {
    // Test that the batch processor reduces batch size when hitting BlockTooLarge limit

    // Set up test environment
    let temp_dir = tempfile::tempdir()?;
    let storage_path = temp_dir.path().join("test_batch_reduction.sqlite");

    let storage_builder = MemoryStorageBuilder::default();
    let keys = InMemorySigner::new(None);

    // Create a restrictive policy that limits block size to trigger BlockTooLarge
    let restrictive_policy = ResourceControlPolicy {
        maximum_block_size: 800,
        ..ResourceControlPolicy::default()
    };

    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(restrictive_policy);

    let client = builder.add_root_chain(1, Amount::from_tokens(100)).await?;

    let context = Arc::new(Mutex::new(ClientContext {
        client: client.clone(),
        update_calls: 0,
    }));

    let faucet_storage = Arc::new(FaucetDatabase::new(&storage_path).await?);
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let request_notifier = Arc::new(Notify::new());

    // Create batch processor with initial batch size of 3 and disabled rate limiting
    let initial_batch_size = 3;
    let config = BatchProcessorConfig {
        start_balance: Amount::from_tokens(100),
        start_timestamp: Timestamp::from(0),
        end_timestamp: Timestamp::from(0), // All tokens are unlocked: no rate limiting.
        max_batch_size: initial_batch_size,
    };

    let mut batch_processor = BatchProcessor::new(
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
            pending_requests_guard.push_back(PendingRequest {
                owner,
                target_chain_id: None,
                amount: Amount::from_tokens(1),
                daily_period: 0,
                responder: tx,
                #[cfg(with_metrics)]
                queued_at: std::time::Instant::now(),
            });
        }
    }

    // Execute the batch - this triggers BlockTooLarge error
    batch_processor.process_batch().await?;

    // Now the batch size should be reduced.
    assert!(batch_processor.config.max_batch_size < initial_batch_size);
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_faucet_persistence() -> anyhow::Result<()> {
    // Test that the faucet correctly persists chain IDs and retrieves them after restart.

    let mut config = FaucetTestConfig::new(6);
    config.batch_config.end_timestamp = Timestamp::from(6000);
    let batch_config = config.batch_config.clone();
    let env = FaucetTestEnv::new(config).await?;
    let handle = env.spawn_processor(batch_config.clone());

    // Set time to allow claims
    env.clock.set(Timestamp::from(1000));

    // Make first claim with a specific owner
    let test_owner_1 = AccountPublicKey::test_key(42).into();
    let test_owner_2 = AccountPublicKey::test_key(43).into();

    // Claim chains for two different owners
    let chain_1 = env
        .root
        .do_claim(test_owner_1)
        .await
        .expect("First claim should succeed");

    env.clock.set(Timestamp::from(2000));
    let chain_2 = env
        .root
        .do_claim(test_owner_2)
        .await
        .expect("Second claim should succeed");

    // Verify that immediate re-claims return the same chains
    let chain_1_again = env
        .root
        .do_claim(test_owner_1)
        .await
        .expect("Re-claim should return existing chain");
    assert_eq!(
        chain_1.id(),
        chain_1_again.id(),
        "Should return same chain for same owner"
    );

    let chain_2_again = env
        .root
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
    handle.stop().await?;

    // Create a new faucet instance with the same database path (simulating restart)
    let faucet_storage_2 = Arc::new(FaucetDatabase::new(&env.storage_path).await?);

    // Test the chain_id query API through the database for owners that have claimed chains
    let queried_chain_1 = faucet_storage_2
        .get_chain_id(&test_owner_1)
        .await?
        .expect("Owner 1 should have a chain ID");
    assert_eq!(
        chain_1_id, queried_chain_1,
        "Query should return correct chain ID for owner 1"
    );

    let queried_chain_2 = faucet_storage_2
        .get_chain_id(&test_owner_2)
        .await?
        .expect("Owner 2 should have a chain ID");
    assert_eq!(
        chain_2_id, queried_chain_2,
        "Query should return correct chain ID for owner 2"
    );

    // Test the chain_id query for an owner that hasn't claimed a chain yet
    let test_owner_new = AccountPublicKey::test_key(99).into();
    let result_new = faucet_storage_2.get_chain_id(&test_owner_new).await?;
    assert!(
        result_new.is_none(),
        "Query should return None for owner that hasn't claimed a chain"
    );

    // Restart with a new MutationRoot and batch processor
    let (root_2, handle_2) = env.new_faucet_instance(faucet_storage_2, batch_config);

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
    env.clock.set(Timestamp::from(3000));
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

    handle_2.stop().await
}

#[test_log::test(tokio::test)]
async fn test_blockchain_sync_after_database_deletion() -> anyhow::Result<()> {
    // Test that the faucet correctly syncs with blockchain after database deletion.

    let mut config = FaucetTestConfig::new(6);
    config.batch_config.end_timestamp = Timestamp::from(6000);
    let batch_config = config.batch_config.clone();
    let env = FaucetTestEnv::new(config).await?;
    let handle = env.spawn_processor(batch_config.clone());

    // Set time to allow claims
    env.clock.set(Timestamp::from(1000));

    // Make claims with specific owners to create chain mappings
    let test_owner_1 = AccountPublicKey::test_key(100).into();
    let test_owner_2 = AccountPublicKey::test_key(101).into();

    // Claim chains for two different owners
    let chain_1 = env
        .root
        .do_claim(test_owner_1)
        .await
        .expect("First claim should succeed");

    env.clock.set(Timestamp::from(2000));
    let chain_2 = env
        .root
        .do_claim(test_owner_2)
        .await
        .expect("Second claim should succeed");

    // Store the chain IDs for later comparison
    let chain_1_id = chain_1.id();
    let chain_2_id = chain_2.id();

    // Verify initial state works correctly
    let chain_1_again = env
        .root
        .do_claim(test_owner_1)
        .await
        .expect("Re-claim should return existing chain");
    assert_eq!(
        chain_1_id,
        chain_1_again.id(),
        "Should return same chain for same owner initially"
    );

    // Stop the batch processor
    handle.stop().await?;

    // === PHASE 2: Delete the database file (simulate data loss) ===
    std::fs::remove_file(&env.storage_path)?;

    // === PHASE 3: Create new faucet instance (should sync from blockchain) ===
    let faucet_storage_2 = Arc::new(FaucetDatabase::new(&env.storage_path).await?);

    // CRITICAL: Trigger blockchain sync before using the faucet
    faucet_storage_2.sync_with_blockchain(&env.client).await?;

    // Restart with a new MutationRoot and batch processor
    let (root_2, handle_2) = env.new_faucet_instance(faucet_storage_2, batch_config);

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
    env.clock.set(Timestamp::from(3000));
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

    handle_2.stop().await
}

#[test_log::test(tokio::test)]
async fn test_daily_claim_flow() -> anyhow::Result<()> {
    // Test the full daily claim flow: create a chain, then make a daily claim.

    let daily_amount = Amount::from_millis(500);
    let mut config = FaucetTestConfig::new(100);
    config.batch_config.max_batch_size = 10;
    config.daily_claim_amount = daily_amount;
    let batch_config = config.batch_config.clone();
    let env = FaucetTestEnv::new(config).await?;
    let handle = env.spawn_processor(batch_config);

    let test_owner = AccountPublicKey::test_key(200).into();

    // Step 1: Daily claim should fail before initial claim.
    let daily_before_initial = env.root.do_daily_claim(test_owner).await;
    assert!(
        daily_before_initial.is_err(),
        "Daily claim should fail without an initial chain claim"
    );

    // Step 2: Do the initial claim to create a chain.
    let description = env
        .root
        .do_claim(test_owner)
        .await
        .expect("Initial claim should succeed");
    let chain_id = description.id();

    // Step 3: Daily claim should fail in period 0 (same period as initial claim).
    let daily_same_period = env.root.do_daily_claim(test_owner).await;
    assert!(
        daily_same_period.is_err(),
        "Daily claim should fail in the same period as initial claim"
    );

    // Step 4: Advance clock by 25 hours to enter period 1.
    let twenty_five_hours = 25 * 60 * 60 * 1_000_000u64;
    env.clock.set(Timestamp::from(twenty_five_hours));

    // Step 5: Daily claim should now succeed.
    let outcome = env
        .root
        .do_daily_claim(test_owner)
        .await
        .expect("Daily claim should succeed after 25 hours");
    assert_eq!(outcome.chain_id, chain_id);
    assert_eq!(outcome.amount, daily_amount);

    // Step 6: Second daily claim in the same period should fail.
    let daily_duplicate = env.root.do_daily_claim(test_owner).await;
    assert!(
        daily_duplicate.is_err(),
        "Second daily claim in same period should fail"
    );

    // Step 7: Advance clock by another 24 hours to enter period 2.
    let forty_nine_hours = 49 * 60 * 60 * 1_000_000u64;
    env.clock.set(Timestamp::from(forty_nine_hours));

    let outcome_2 = env
        .root
        .do_daily_claim(test_owner)
        .await
        .expect("Daily claim should succeed in period 2");
    assert_eq!(outcome_2.chain_id, chain_id);
    assert_eq!(outcome_2.amount, daily_amount);

    handle.stop().await
}
