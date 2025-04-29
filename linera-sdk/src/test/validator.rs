// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A minimal validator implementation suited for tests.
//!
//! The [`TestValidator`] is a minimal validator with a single shard. Micro-chains can be added to
//! it, and blocks can be added to each microchain individually.

use std::{num::NonZeroUsize, sync::Arc};

use dashmap::DashMap;
use futures::{
    lock::{MappedMutexGuard, Mutex, MutexGuard},
    FutureExt as _,
};
use linera_base::{
    crypto::{AccountSecretKey, ValidatorKeypair, ValidatorSecretKey},
    data_types::{
        Amount, ApplicationPermissions, Blob, BlobContent, ChainDescription, ChainOrigin, Epoch,
        InitialChainConfig, Timestamp,
    },
    identifiers::{AccountOwner, ApplicationId, ChainId, ModuleId},
    ownership::ChainOwnership,
};
use linera_core::worker::WorkerState;
use linera_execution::{
    committee::Committee,
    system::{AdminOperation, OpenChainConfig, SystemOperation},
    ResourceControlPolicy, WasmRuntime,
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
    validator_secret: ValidatorSecretKey,
    account_secret: AccountSecretKey,
    committee: Arc<Mutex<(Epoch, Committee)>>,
    storage: DbStorage<MemoryStore, TestClock>,
    worker: WorkerState<DbStorage<MemoryStore, TestClock>>,
    clock: TestClock,
    admin_chain_id: ChainId,
    chains: Arc<DashMap<ChainId, ActiveChain>>,
}

impl Clone for TestValidator {
    fn clone(&self) -> Self {
        TestValidator {
            admin_chain_id: self.admin_chain_id,
            validator_secret: self.validator_secret.copy(),
            account_secret: self.account_secret.copy(),
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
        let validator_keypair = ValidatorKeypair::generate();
        let account_secret = AccountSecretKey::generate();
        let epoch = Epoch::ZERO;
        let committee = Committee::make_simple(vec![(
            validator_keypair.public_key,
            account_secret.public(),
        )]);
        let wasm_runtime = Some(WasmRuntime::default());
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(wasm_runtime)
            .now_or_never()
            .expect("execution of DbStorage::new should not await anything");
        let clock = storage.clock().clone();
        let worker = WorkerState::new(
            "Single validator node".to_string(),
            Some(validator_keypair.secret_key.copy()),
            storage.clone(),
            NonZeroUsize::new(40).expect("Chain worker limit should not be zero"),
        );

        // Create an admin chain.
        let key_pair = AccountSecretKey::generate();

        let new_chain_config = InitialChainConfig {
            ownership: ChainOwnership::single(key_pair.public().into()),
            committees: [(
                epoch,
                bcs::to_bytes(&committee).expect("Serializing a committee should not fail!"),
            )]
            .into_iter()
            .collect(),
            admin_id: None,
            epoch,
            balance: Amount::from_tokens(1_000_000),
            application_permissions: ApplicationPermissions::default(),
        };

        let origin = ChainOrigin::Root(0);
        let description = ChainDescription::new(origin, new_chain_config, Timestamp::from(0));
        let admin_chain_id = description.id();

        worker
            .storage_client()
            .create_chain(description.clone())
            .await
            .expect("Failed to create root admin chain");

        let validator = TestValidator {
            validator_secret: validator_keypair.secret_key,
            account_secret,
            committee: Arc::new(Mutex::new((epoch, committee))),
            storage,
            worker,
            clock,
            admin_chain_id,
            chains: Arc::default(),
        };

        let chain = ActiveChain::new(key_pair, description.clone(), validator.clone());

        validator.chains.insert(description.id(), chain);

        validator
    }

    /// Creates a new [`TestValidator`] with a single microchain with the bytecode of the crate
    /// calling this method published on it.
    ///
    /// Returns the new [`TestValidator`] and the [`ModuleId`] of the published module.
    pub async fn with_current_module<Abi, Parameters, InstantiationArgument>() -> (
        TestValidator,
        ModuleId<Abi, Parameters, InstantiationArgument>,
    ) {
        let validator = TestValidator::new().await;
        let publisher = validator.new_chain().await;

        let module_id = publisher.publish_current_module().await;

        (validator, module_id)
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
        let (validator, module_id) =
            TestValidator::with_current_module::<Abi, Parameters, InstantiationArgument>().await;

        let mut creator = validator.new_chain().await;

        let application_id = creator
            .create_application(module_id, parameters, instantiation_argument, vec![])
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
    pub fn key_pair(&self) -> &ValidatorSecretKey {
        &self.validator_secret
    }

    /// Returns the ID of the admin chain.
    pub fn admin_chain_id(&self) -> ChainId {
        self.admin_chain_id
    }

    /// Returns the latest committee that this test validator is part of.
    ///
    /// The committee contains only this validator.
    pub async fn committee(&self) -> MappedMutexGuard<'_, (Epoch, Committee), Committee> {
        MutexGuard::map(self.committee.lock().await, |(_epoch, committee)| committee)
    }

    /// Updates the admin chain, creating a new epoch with an updated
    /// [`ResourceControlPolicy`].
    pub async fn change_resource_control_policy(
        &mut self,
        adjustment: impl FnOnce(&mut ResourceControlPolicy),
    ) {
        let (epoch, committee) = {
            let (ref mut epoch, ref mut committee) = &mut *self.committee.lock().await;

            epoch
                .try_add_assign_one()
                .expect("Reached the limit of epochs");

            adjustment(committee.policy_mut());

            (*epoch, committee.clone())
        };

        let admin_chain = self.get_chain(&self.admin_chain_id);

        let committee_blob = Blob::new(BlobContent::new_committee(
            bcs::to_bytes(&committee).unwrap(),
        ));
        let blob_hash = committee_blob.id().hash;
        self.storage
            .write_blob(&committee_blob)
            .await
            .expect("Should write committee blob");

        admin_chain
            .add_block(|block| {
                block.with_system_operation(SystemOperation::Admin(
                    AdminOperation::CreateCommittee { epoch, blob_hash },
                ));
            })
            .await;

        for entry in self.chains.iter() {
            let chain = entry.value();

            if chain.id() != self.admin_chain_id {
                chain
                    .add_block(|block| {
                        block.with_system_operation(SystemOperation::ProcessNewEpoch(epoch));
                    })
                    .await;
            }
        }
    }

    /// Creates a new microchain and returns the [`ActiveChain`] that can be used to add blocks to
    /// it with the given key pair.
    pub async fn new_chain_with_keypair(&self, key_pair: AccountSecretKey) -> ActiveChain {
        let description = self
            .request_new_chain_from_admin_chain(key_pair.public().into())
            .await;
        let chain = ActiveChain::new(key_pair, description.clone(), self.clone());

        chain.handle_received_messages().await;

        self.chains.insert(description.id(), chain.clone());

        chain
    }

    /// Creates a new microchain and returns the [`ActiveChain`] that can be used to add blocks to
    /// it.
    pub async fn new_chain(&self) -> ActiveChain {
        let key_pair = AccountSecretKey::generate();
        self.new_chain_with_keypair(key_pair).await
    }

    /// Adds an existing [`ActiveChain`].
    pub fn add_chain(&self, chain: ActiveChain) {
        self.chains.insert(chain.id(), chain);
    }

    /// Adds a block to the admin chain to create a new chain.
    ///
    /// Returns the [`ChainDescription`] of the new chain.
    async fn request_new_chain_from_admin_chain(&self, owner: AccountOwner) -> ChainDescription {
        let admin_id = self.admin_chain_id;
        let admin_chain = self
            .chains
            .get(&admin_id)
            .expect("Admin chain should be created when the `TestValidator` is constructed");

        let (epoch, committee) = self.committee.lock().await.clone();

        let open_chain_config = OpenChainConfig {
            ownership: ChainOwnership::single(owner),
            balance: Amount::ZERO,
            application_permissions: ApplicationPermissions::default(),
        };
        let new_chain_config = open_chain_config.init_chain_config(
            epoch,
            Some(admin_id),
            [(
                epoch,
                bcs::to_bytes(&committee).expect("Serializing a committee should not fail!"),
            )]
            .into_iter()
            .collect(),
        );

        let certificate = admin_chain
            .add_block(|block| {
                block.with_system_operation(SystemOperation::OpenChain(open_chain_config));
            })
            .await;
        let block = certificate.inner().block();

        let origin = ChainOrigin::Child {
            parent: block.header.chain_id,
            block_height: block.header.height,
            chain_index: 0,
        };

        ChainDescription::new(origin, new_chain_config, Timestamp::from(0))
    }

    /// Returns the [`ActiveChain`] reference to the microchain identified by `chain_id`.
    pub fn get_chain(&self, chain_id: &ChainId) -> ActiveChain {
        self.chains.get(chain_id).expect("Chain not found").clone()
    }
}
