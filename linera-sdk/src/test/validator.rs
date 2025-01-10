// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A minimal validator implementation suited for tests.
//!
//! The [`TestValidator`] is a minimal validator with a single shard. Micro-chains can be added to
//! it, and blocks can be added to each microchain individually.

use std::{num::NonZeroUsize, sync::Arc};

use dashmap::DashMap;
use futures::FutureExt as _;
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{Amount, ApplicationPermissions, Timestamp},
    identifiers::{ApplicationId, BytecodeId, ChainDescription, ChainId, MessageId},
    ownership::ChainOwnership,
};
use linera_core::worker::WorkerState;
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{OpenChainConfig, SystemOperation, OPEN_CHAIN_MESSAGE_INDEX},
    WasmRuntime,
};
use linera_storage::{DbStorage, Storage, TestClock};
use linera_views::memory::MemoryStore;
use serde::Serialize;

use super::ActiveChain;
use crate::ContractAbi;

/// A minimal validator implementation suited for tests.
///
/// ```rust
/// # use linera_sdk::test::*;
/// # use linera_base::{data_types::BlockHeight, identifiers::ChainId};
/// # tokio_test::block_on(async {
/// let validator = TestValidator::new().await;
/// assert_eq!(
///     validator.new_chain().await.get_tip_height().await,
///     BlockHeight(0)
/// );
/// # });
/// ```
pub struct TestValidator {
    key_pair: KeyPair,
    committee: Committee,
    storage: DbStorage<MemoryStore, TestClock>,
    worker: WorkerState<DbStorage<MemoryStore, TestClock>>,
    clock: TestClock,
    chains: Arc<DashMap<ChainId, ActiveChain>>,
}

impl Clone for TestValidator {
    fn clone(&self) -> Self {
        TestValidator {
            key_pair: self.key_pair.copy(),
            committee: self.committee.clone(),
            storage: self.storage.clone(),
            worker: self.worker.clone(),
            clock: self.clock.clone(),
            chains: self.chains.clone(),
        }
    }
}

impl TestValidator {
    /// Creates a new [`TestValidator`].
    pub async fn new() -> Self {
        let key_pair = KeyPair::generate();
        let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
        let wasm_runtime = Some(WasmRuntime::default());
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(wasm_runtime)
            .now_or_never()
            .expect("execution of DbStorage::new should not await anything");
        let clock = storage.clock().clone();
        let worker = WorkerState::new(
            "Single validator node".to_string(),
            Some(key_pair.copy()),
            storage.clone(),
            NonZeroUsize::new(20).expect("Chain worker limit should not be zero"),
        );

        let validator = TestValidator {
            key_pair,
            committee,
            storage,
            worker,
            clock,
            chains: Arc::default(),
        };

        validator.create_admin_chain().await;
        validator
    }

    /// Creates a new [`TestValidator`] with a single microchain with the bytecode of the crate
    /// calling this method published on it.
    ///
    /// Returns the new [`TestValidator`] and the [`BytecodeId`] of the published bytecode.
    pub async fn with_current_bytecode<Abi, Parameters, InstantiationArgument>() -> (
        TestValidator,
        BytecodeId<Abi, Parameters, InstantiationArgument>,
    ) {
        let validator = TestValidator::new().await;
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

    /// Returns this validator's storage.
    pub(crate) fn storage(&self) -> &DbStorage<MemoryStore, TestClock> {
        &self.storage
    }

    /// Returns the locked [`WorkerState`] of this validator.
    pub(crate) fn worker(&self) -> WorkerState<DbStorage<MemoryStore, TestClock>> {
        self.worker.clone()
    }

    /// Returns the [`TestClock`] of this validator.
    pub fn clock(&self) -> &TestClock {
        &self.clock
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
        let description = self
            .request_new_chain_from_admin_chain(key_pair.public())
            .await;
        let chain = ActiveChain::new(key_pair, description, self.clone());

        chain.handle_received_messages().await;

        self.chains.insert(description.into(), chain.clone());

        chain
    }

    /// Adds a block to the admin chain to create a new chain.
    ///
    /// Returns the [`ChainDescription`] of the new chain.
    async fn request_new_chain_from_admin_chain(&self, public_key: PublicKey) -> ChainDescription {
        let admin_id = ChainId::root(0);
        let admin_chain = self
            .chains
            .get(&admin_id)
            .expect("Admin chain should be created when the `TestValidator` is constructed");

        let new_chain_config = OpenChainConfig {
            ownership: ChainOwnership::single(public_key),
            committees: [(Epoch::ZERO, self.committee.clone())]
                .into_iter()
                .collect(),
            admin_id,
            epoch: Epoch::ZERO,
            balance: Amount::ZERO,
            application_permissions: ApplicationPermissions::default(),
        };

        let certificate = admin_chain
            .add_block(|block| {
                block.with_system_operation(SystemOperation::OpenChain(new_chain_config));
            })
            .await;
        let executed_block = certificate.inner().executed_block();

        ChainDescription::Child(MessageId {
            chain_id: executed_block.block.chain_id,
            height: executed_block.block.height,
            index: OPEN_CHAIN_MESSAGE_INDEX,
        })
    }

    /// Returns the [`ActiveChain`] reference to the microchain identified by `chain_id`.
    pub fn get_chain(&self, chain_id: &ChainId) -> ActiveChain {
        self.chains.get(chain_id).expect("Chain not found").clone()
    }

    /// Creates the root admin microchain and returns the [`ActiveChain`] map with it.
    async fn create_admin_chain(&self) {
        let key_pair = KeyPair::generate();
        let description = ChainDescription::Root(0);

        self.worker()
            .storage_client()
            .create_chain(
                self.committee.clone(),
                ChainId::root(0),
                description,
                key_pair.public(),
                Amount::MAX,
                Timestamp::from(0),
            )
            .await
            .expect("Failed to create root admin chain");

        let chain = ActiveChain::new(key_pair, description, self.clone());

        self.chains.insert(description.into(), chain);
    }
}
