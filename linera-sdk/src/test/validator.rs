// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A minimal validator implementation suited for tests.
//!
//! The [`TestValidator`] is a minimal validator with a single shard. Micro-chains can be added to
//! it, and blocks can be added to each microchain individually.

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dashmap::DashMap;
use futures::FutureExt;
use linera_base::{
    crypto::KeyPair,
    data_types::Timestamp,
    identifiers::{ApplicationId, BytecodeId, ChainDescription, ChainId},
};
use linera_core::worker::WorkerState;
use linera_execution::{
    committee::{Committee, ValidatorName},
    WasmRuntime,
};
use linera_storage::{MemoryStorage, Storage, WallClock};
use linera_views::memory::{MemoryStoreConfig, TEST_MEMORY_MAX_STREAM_QUERIES};
use serde::Serialize;
use tokio::sync::{Mutex, MutexGuard};

use super::ActiveChain;
use crate::ContractAbi;

/// A minimal validator implementation suited for tests.
///
/// ```rust
/// # use linera_sdk::test::*;
/// # use linera_base::identifiers::ChainId;
/// # tokio_test::block_on(async {
/// let validator = TestValidator::default();
/// assert_eq!(validator.new_chain().await.id(), ChainId::root(0));
/// # });
/// ```
pub struct TestValidator {
    key_pair: KeyPair,
    committee: Committee,
    worker: Arc<Mutex<WorkerState<MemoryStorage<WallClock>>>>,
    root_chain_counter: Arc<AtomicU32>,
    chains: Arc<DashMap<ChainId, ActiveChain>>,
}

impl Default for TestValidator {
    fn default() -> Self {
        let key_pair = KeyPair::generate();
        let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
        let store_config = MemoryStoreConfig::new(TEST_MEMORY_MAX_STREAM_QUERIES);
        let namespace = "validator";
        let wasm_runtime = Some(WasmRuntime::default());
        let storage = MemoryStorage::new(store_config, namespace, wasm_runtime)
            .now_or_never()
            .expect("execution of MemoryStorage::new should not await anything")
            .expect("storage");

        let worker = WorkerState::new(
            "Single validator node".to_string(),
            Some(key_pair.copy()),
            storage,
        );

        TestValidator {
            key_pair,
            committee,
            worker: Arc::new(Mutex::new(worker)),
            root_chain_counter: Arc::default(),
            chains: Arc::default(),
        }
    }
}

impl Clone for TestValidator {
    fn clone(&self) -> Self {
        TestValidator {
            key_pair: self.key_pair.copy(),
            committee: self.committee.clone(),
            worker: self.worker.clone(),
            root_chain_counter: self.root_chain_counter.clone(),
            chains: self.chains.clone(),
        }
    }
}

impl TestValidator {
    /// Creates a new [`TestValidator`] with a single microchain with the bytecode of the crate
    /// calling this method published on it.
    ///
    /// Returns the new [`TestValidator`] and the [`BytecodeId`] of the published bytecode.
    pub async fn with_current_bytecode<Abi, Parameters, InstantiationArgument>() -> (
        TestValidator,
        BytecodeId<Abi, Parameters, InstantiationArgument>,
    ) {
        let validator = TestValidator::default();
        let publisher = validator.new_chain().await;

        let bytecode_id = publisher.publish_current_bytecode().await;

        (validator, bytecode_id)
    }

    /// Creates a new [`TestValidator`] with the application of the crate calling this method
    /// created on a chain.
    ///
    /// The bytecode is first published on one microchain, then the application is created on
    /// another microchain.
    ///
    /// Returns the new [`TestValidator`], the [`ApplicationId`] of the created application, and
    /// the chain on which it was created.
    pub async fn with_current_application<Abi, Parameters, InstantiationArgument>(
        parameters: Parameters,
        instantiation_argument: InstantiationArgument,
    ) -> (TestValidator, ApplicationId<Abi>, ActiveChain)
    where
        Abi: ContractAbi,
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    {
        let (validator, bytecode_id) =
            TestValidator::with_current_bytecode::<Abi, Parameters, InstantiationArgument>().await;

        let mut creator = validator.new_chain().await;

        let application_id = creator
            .create_application(bytecode_id, parameters, instantiation_argument, vec![])
            .await;

        (validator, application_id, creator)
    }

    /// Returns the locked [`WorkerState`] of this validator.
    pub(crate) async fn worker(&self) -> MutexGuard<WorkerState<MemoryStorage<WallClock>>> {
        self.worker.lock().await
    }

    /// Returns the keys this test validator uses for signing certificates.
    pub fn key_pair(&self) -> &KeyPair {
        &self.key_pair
    }

    /// Returns the committee that this test validator is part of.
    ///
    /// The committee contains only this validator.
    pub fn committee(&self) -> &Committee {
        &self.committee
    }

    /// Creates a new microchain and returns the [`ActiveChain`] that can be used to add blocks to
    /// it.
    pub async fn new_chain(&self) -> ActiveChain {
        let key_pair = KeyPair::generate();
        let description =
            ChainDescription::Root(self.root_chain_counter.fetch_add(1, Ordering::AcqRel));

        self.worker()
            .await
            .storage_client()
            .create_chain(
                self.committee.clone(),
                ChainId::root(0),
                description,
                key_pair.public(),
                0.into(),
                Timestamp::from(0),
            )
            .await
            .expect("Failed to create chain");

        let chain = ActiveChain::new(key_pair, description, self.clone());

        self.chains.insert(description.into(), chain.clone());

        chain
    }

    /// Returns the [`ActiveChain`] reference to the microchain identified by `chain_id`.
    pub fn get_chain(&self, chain_id: &ChainId) -> ActiveChain {
        self.chains.get(chain_id).expect("Chain not found").clone()
    }
}
