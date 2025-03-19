// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A reference to a single microchain inside a [`TestValidator`].
//!
//! This allows manipulating a test microchain.

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use cargo_toml::Manifest;
use linera_base::{
    crypto::{AccountPublicKey, AccountSecretKey},
    data_types::{
        Amount, Blob, BlockHeight, Bytecode, CompressedBytecode, UserApplicationDescription,
    },
    identifiers::{AccountOwner, ApplicationId, ChainDescription, ChainId, ModuleId},
    vm::VmRuntime,
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{data_types::ChainInfoQuery, worker::WorkerError};
use linera_execution::{
    committee::Epoch,
    system::{SystemOperation, SystemQuery, SystemResponse},
    Operation, Query, QueryOutcome, QueryResponse,
};
use linera_storage::Storage as _;
use serde::Serialize;
use tokio::{fs, sync::Mutex};

use super::{BlockBuilder, TestValidator};
use crate::{ContractAbi, ServiceAbi};

/// A reference to a single microchain inside a [`TestValidator`].
pub struct ActiveChain {
    key_pair: AccountSecretKey,
    description: ChainDescription,
    tip: Arc<Mutex<Option<ConfirmedBlockCertificate>>>,
    validator: TestValidator,
}

impl Clone for ActiveChain {
    fn clone(&self) -> Self {
        ActiveChain {
            key_pair: self.key_pair.copy(),
            description: self.description,
            tip: self.tip.clone(),
            validator: self.validator.clone(),
        }
    }
}

impl ActiveChain {
    /// Creates a new [`ActiveChain`] instance referencing a new empty microchain in the
    /// `validator`.
    ///
    /// The microchain has a single owner that uses the `key_pair` to produce blocks. The
    /// `description` is used as the identifier of the microchain.
    pub fn new(
        key_pair: AccountSecretKey,
        description: ChainDescription,
        validator: TestValidator,
    ) -> Self {
        ActiveChain {
            key_pair,
            description,
            tip: Arc::default(),
            validator,
        }
    }

    /// Returns the [`ChainId`] of this microchain.
    pub fn id(&self) -> ChainId {
        self.description.into()
    }

    /// Returns the [`AccountPublicKey`] of the active owner of this microchain.
    pub fn public_key(&self) -> AccountPublicKey {
        self.key_pair.public()
    }

    /// Returns the [`AccountSecretKey`] of the active owner of this microchain.
    pub fn key_pair(&self) -> &AccountSecretKey {
        &self.key_pair
    }

    /// Sets the [`AccountSecretKey`] to use for signing new blocks.
    pub fn set_key_pair(&mut self, key_pair: AccountSecretKey) {
        self.key_pair = key_pair
    }

    /// Returns the current [`Epoch`] the chain is in.
    pub async fn epoch(&self) -> Epoch {
        self.validator
            .worker()
            .chain_state_view(self.id())
            .await
            .expect("Failed to load chain")
            .execution_state
            .system
            .epoch
            .get()
            .expect("Active chains should be in an epoch")
    }

    /// Reads the current shared balance available to all of the owners of this microchain.
    pub async fn chain_balance(&self) -> Amount {
        let query = Query::System(SystemQuery);

        let QueryOutcome { response, .. } = self
            .validator
            .worker()
            .query_application(self.id(), query)
            .await
            .expect("Failed to query chain's balance");

        let QueryResponse::System(SystemResponse { balance, .. }) = response else {
            panic!("Unexpected response from system application");
        };

        balance
    }

    /// Reads the current account balance on this microchain of an [`AccountOwner`].
    pub async fn owner_balance(&self, owner: &AccountOwner) -> Option<Amount> {
        let chain_state = self
            .validator
            .worker()
            .chain_state_view(self.id())
            .await
            .expect("Failed to read chain state");

        chain_state
            .execution_state
            .system
            .balances
            .get(owner)
            .await
            .expect("Failed to read owner balance")
    }

    /// Reads the current account balance on this microchain of all [`AccountOwner`]s.
    pub async fn owner_balances(
        &self,
        owners: impl IntoIterator<Item = AccountOwner>,
    ) -> HashMap<AccountOwner, Option<Amount>> {
        let chain_state = self
            .validator
            .worker()
            .chain_state_view(self.id())
            .await
            .expect("Failed to read chain state");

        let mut balances = HashMap::new();

        for owner in owners {
            let balance = chain_state
                .execution_state
                .system
                .balances
                .get(&owner)
                .await
                .expect("Failed to read an owner's balance");

            balances.insert(owner, balance);
        }

        balances
    }

    /// Reads a list of [`AccountOwner`]s that have a non-zero balance on this microchain.
    pub async fn accounts(&self) -> Vec<AccountOwner> {
        let chain_state = self
            .validator
            .worker()
            .chain_state_view(self.id())
            .await
            .expect("Failed to read chain state");

        chain_state
            .execution_state
            .system
            .balances
            .indices()
            .await
            .expect("Failed to list accounts on the chain")
    }

    /// Reads all the non-zero account balances on this microchain.
    pub async fn all_owner_balances(&self) -> HashMap<AccountOwner, Amount> {
        self.owner_balances(self.accounts().await)
            .await
            .into_iter()
            .map(|(owner, balance)| {
                (
                    owner,
                    balance.expect("`accounts` should only return accounts with non-zero balance"),
                )
            })
            .collect()
    }

    /// Adds a block to this microchain.
    ///
    /// The `block_builder` parameter is a closure that should use the [`BlockBuilder`] parameter
    /// to provide the block's contents.
    pub async fn add_block(
        &self,
        block_builder: impl FnOnce(&mut BlockBuilder),
    ) -> ConfirmedBlockCertificate {
        self.try_add_block(block_builder)
            .await
            .expect("Failed to execute block.")
    }

    /// Adds a block to this microchain, passing the blobs to be used during certificate handling.
    ///
    /// The `block_builder` parameter is a closure that should use the [`BlockBuilder`] parameter
    /// to provide the block's contents.
    pub async fn add_block_with_blobs(
        &self,
        block_builder: impl FnOnce(&mut BlockBuilder),
        blobs: Vec<Blob>,
    ) -> ConfirmedBlockCertificate {
        self.try_add_block_with_blobs(block_builder, blobs)
            .await
            .expect("Failed to execute block.")
    }

    /// Tries to add a block to this microchain.
    ///
    /// The `block_builder` parameter is a closure that should use the [`BlockBuilder`] parameter
    /// to provide the block's contents.
    pub async fn try_add_block(
        &self,
        block_builder: impl FnOnce(&mut BlockBuilder),
    ) -> anyhow::Result<ConfirmedBlockCertificate> {
        self.try_add_block_with_blobs(block_builder, vec![]).await
    }

    /// Tries to add a block to this microchain, writing some `blobs` to storage if needed.
    ///
    /// The `block_builder` parameter is a closure that should use the [`BlockBuilder`] parameter
    /// to provide the block's contents.
    ///
    /// The blobs are either all written to storage, if executing the block fails due to a missing
    /// blob, or none are written to storage if executing the block succeeds without the blobs.
    async fn try_add_block_with_blobs(
        &self,
        block_builder: impl FnOnce(&mut BlockBuilder),
        blobs: Vec<Blob>,
    ) -> anyhow::Result<ConfirmedBlockCertificate> {
        let mut tip = self.tip.lock().await;
        let mut block = BlockBuilder::new(
            self.description.into(),
            self.key_pair.public().into(),
            self.epoch().await,
            tip.as_ref(),
            self.validator.clone(),
        );

        block_builder(&mut block);

        // TODO(#2066): Remove boxing once call-stack is shallower
        let certificate = Box::pin(block.try_sign(&blobs)).await?;

        let result = self
            .validator
            .worker()
            .fully_handle_certificate_with_notifications(certificate.clone(), &())
            .await;
        if let Err(WorkerError::BlobsNotFound(_)) = &result {
            self.validator.storage().maybe_write_blobs(&blobs).await?;
            self.validator
                .worker()
                .fully_handle_certificate_with_notifications(certificate.clone(), &())
                .await
                .expect("Rejected certificate");
        } else {
            result.expect("Rejected certificate");
        }

        *tip = Some(certificate.clone());

        Ok(certificate)
    }

    /// Receives all queued messages in all inboxes of this microchain.
    ///
    /// Adds a block to this microchain that receives all queued messages in the microchains
    /// inboxes.
    pub async fn handle_received_messages(&self) {
        let chain_id = self.id();
        let (information, _) = self
            .validator
            .worker()
            .handle_chain_info_query(ChainInfoQuery::new(chain_id).with_pending_message_bundles())
            .await
            .expect("Failed to query chain's pending messages");
        let messages = information.info.requested_pending_message_bundles;

        self.add_block(|block| {
            block.with_incoming_bundles(messages);
        })
        .await;
    }

    /// Publishes the module in the crate calling this method to this microchain.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecode files
    /// to be published on this chain. Returns the module ID to reference the published module.
    pub async fn publish_current_module<Abi, Parameters, InstantiationArgument>(
        &self,
    ) -> ModuleId<Abi, Parameters, InstantiationArgument> {
        self.publish_bytecode_files_in(".").await
    }

    /// Publishes the bytecode files in the crate at `repository_path`.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecode files
    /// to be published on this chain. Returns the module ID to reference the published module.
    pub async fn publish_bytecode_files_in<Abi, Parameters, InstantiationArgument>(
        &self,
        repository_path: impl AsRef<Path>,
    ) -> ModuleId<Abi, Parameters, InstantiationArgument> {
        let repository_path = fs::canonicalize(repository_path)
            .await
            .expect("Failed to obtain absolute application repository path");
        Self::build_bytecode_files_in(&repository_path).await;
        let (contract, service) = self.find_bytecode_files_in(&repository_path).await;
        let contract_blob = Blob::new_contract_bytecode(contract);
        let service_blob = Blob::new_service_bytecode(service);
        let contract_blob_hash = contract_blob.id().hash;
        let service_blob_hash = service_blob.id().hash;
        let vm_runtime = VmRuntime::Wasm;

        let module_id = ModuleId::new(contract_blob_hash, service_blob_hash, vm_runtime);

        let certificate = self
            .add_block_with_blobs(
                |block| {
                    block.with_system_operation(SystemOperation::PublishModule { module_id });
                },
                vec![contract_blob, service_blob],
            )
            .await;

        let executed_block = certificate.inner().block();
        assert_eq!(executed_block.messages().len(), 1);
        assert_eq!(executed_block.messages()[0].len(), 0);

        module_id.with_abi()
    }

    /// Compiles the crate in the `repository` path.
    async fn build_bytecode_files_in(repository: &Path) {
        let output = std::process::Command::new("cargo")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .current_dir(repository)
            .output()
            .expect("Failed to build Wasm binaries");

        if !output.status.success() {
            panic!(
                "Failed to build bytecode binaries.\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    /// Searches the Cargo manifest of the crate calling this method for binaries to use as the
    /// contract and service bytecode files.
    ///
    /// Returns a tuple with the loaded contract and service [`CompressedBytecode`]s,
    /// ready to be published.
    async fn find_bytecode_files_in(
        &self,
        repository: &Path,
    ) -> (CompressedBytecode, CompressedBytecode) {
        let manifest_path = repository.join("Cargo.toml");
        let cargo_manifest =
            Manifest::from_path(manifest_path).expect("Failed to load Cargo.toml manifest");

        let binaries = cargo_manifest
            .bin
            .into_iter()
            .filter_map(|binary| binary.name)
            .filter(|name| name.ends_with("service") || name.ends_with("contract"))
            .collect::<Vec<_>>();

        assert_eq!(
            binaries.len(),
            2,
            "Could not figure out contract and service bytecode binaries.\
            Please specify them manually using `publish_module`."
        );

        let (contract_binary, service_binary) = if binaries[0].ends_with("contract") {
            (&binaries[0], &binaries[1])
        } else {
            (&binaries[1], &binaries[0])
        };

        let base_path = self
            .find_output_directory_of(repository)
            .await
            .expect("Failed to look for output binaries");
        let contract_path = base_path.join(format!("{}.wasm", contract_binary));
        let service_path = base_path.join(format!("{}.wasm", service_binary));

        let contract = Bytecode::load_from_file(contract_path)
            .await
            .expect("Failed to load contract bytecode from file");
        let service = Bytecode::load_from_file(service_path)
            .await
            .expect("Failed to load service bytecode from file");

        tokio::task::spawn_blocking(move || (contract.compress(), service.compress()))
            .await
            .expect("Failed to compress bytecode files")
    }

    /// Searches for the directory where the built WebAssembly binaries should be.
    ///
    /// Assumes that the binaries will be built and placed inside a
    /// `target/wasm32-unknown-unknown/release` sub-directory. However, since the crate with the
    /// binaries could be part of a workspace, that output sub-directory must be searched in parent
    /// directories as well.
    async fn find_output_directory_of(&self, repository: &Path) -> Result<PathBuf, io::Error> {
        let output_sub_directory = Path::new("target/wasm32-unknown-unknown/release");
        let mut current_directory = repository;
        let mut output_path = current_directory.join(output_sub_directory);

        while !fs::try_exists(&output_path).await? {
            current_directory = current_directory.parent().unwrap_or_else(|| {
                panic!(
                    "Failed to find Wasm binary output directory in {}",
                    repository.display()
                )
            });

            output_path = current_directory.join(output_sub_directory);
        }

        Ok(output_path)
    }

    /// Returns the height of the tip of this microchain.
    pub async fn get_tip_height(&self) -> BlockHeight {
        self.tip
            .lock()
            .await
            .as_ref()
            .expect("Block was not successfully added")
            .inner()
            .block()
            .header
            .height
    }

    /// Creates an application on this microchain, using the module referenced by `module_id`.
    ///
    /// Returns the [`ApplicationId`] of the created application.
    ///
    /// If necessary, this microchain will subscribe to the microchain that published the
    /// module to use, and fetch it.
    ///
    /// The application is instantiated using the instantiation parameters, which consist of the
    /// global static `parameters`, the one time `instantiation_argument` and the
    /// `required_application_ids` of the applications that the new application will depend on.
    pub async fn create_application<Abi, Parameters, InstantiationArgument>(
        &mut self,
        module_id: ModuleId<Abi, Parameters, InstantiationArgument>,
        parameters: Parameters,
        instantiation_argument: InstantiationArgument,
        required_application_ids: Vec<ApplicationId>,
    ) -> ApplicationId<Abi>
    where
        Abi: ContractAbi,
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    {
        let parameters = serde_json::to_vec(&parameters).unwrap();
        let instantiation_argument = serde_json::to_vec(&instantiation_argument).unwrap();

        let creation_certificate = self
            .add_block(|block| {
                block.with_system_operation(SystemOperation::CreateApplication {
                    module_id: module_id.forget_abi(),
                    parameters: parameters.clone(),
                    instantiation_argument,
                    required_application_ids: required_application_ids.clone(),
                });
            })
            .await;

        let block = creation_certificate.inner().block();
        assert_eq!(block.messages().len(), 1);
        assert!(block.messages()[0].is_empty());

        let description = UserApplicationDescription {
            module_id: module_id.forget_abi(),
            creator_chain_id: block.header.chain_id,
            block_height: block.header.height,
            application_index: 0,
            parameters,
            required_application_ids,
        };

        ApplicationId::<()>::from(&description).with_abi()
    }

    /// Returns whether this chain has been closed.
    pub async fn is_closed(&self) -> bool {
        self.validator
            .worker()
            .chain_state_view(self.id())
            .await
            .expect("Failed to load chain")
            .is_closed()
    }

    /// Executes a `query` on an `application`'s state on this microchain.
    ///
    /// Returns the deserialized response from the `application`.
    pub async fn query<Abi>(
        &self,
        application_id: ApplicationId<Abi>,
        query: Abi::Query,
    ) -> QueryOutcome<Abi::QueryResponse>
    where
        Abi: ServiceAbi,
    {
        let query_bytes = serde_json::to_vec(&query).expect("Failed to serialize query");

        let QueryOutcome {
            response,
            operations,
        } = self
            .validator
            .worker()
            .query_application(
                self.id(),
                Query::User {
                    application_id: application_id.forget_abi(),
                    bytes: query_bytes,
                },
            )
            .await
            .expect("Failed to query application");

        let deserialized_response = match response {
            QueryResponse::User(bytes) => {
                serde_json::from_slice(&bytes).expect("Failed to deserialize query response")
            }
            QueryResponse::System(_) => {
                unreachable!("User query returned a system response")
            }
        };

        QueryOutcome {
            response: deserialized_response,
            operations,
        }
    }

    /// Executes a GraphQL `query` on an `application`'s state on this microchain.
    ///
    /// Returns the deserialized GraphQL JSON response from the `application`.
    pub async fn graphql_query<Abi>(
        &self,
        application_id: ApplicationId<Abi>,
        query: impl Into<async_graphql::Request>,
    ) -> QueryOutcome<serde_json::Value>
    where
        Abi: ServiceAbi<Query = async_graphql::Request, QueryResponse = async_graphql::Response>,
    {
        let query = query.into();
        let query_str = query.query.clone();
        let QueryOutcome {
            response,
            operations,
        } = self.query(application_id, query).await;
        if !response.errors.is_empty() {
            panic!(
                "GraphQL query:\n{}\nyielded errors:\n{:#?}",
                query_str, response.errors
            );
        }
        let json_response = response
            .data
            .into_json()
            .expect("Unexpected non-JSON query response");

        QueryOutcome {
            response: json_response,
            operations,
        }
    }

    /// Executes a GraphQL `mutation` on an `application` and proposes a block with the resulting
    /// scheduled operations.
    ///
    /// Returns the certificate of the new block.
    pub async fn graphql_mutation<Abi>(
        &self,
        application_id: ApplicationId<Abi>,
        query: impl Into<async_graphql::Request>,
    ) -> ConfirmedBlockCertificate
    where
        Abi: ServiceAbi<Query = async_graphql::Request, QueryResponse = async_graphql::Response>,
    {
        let QueryOutcome { operations, .. } = self.graphql_query(application_id, query).await;

        self.add_block(|block| {
            for operation in operations {
                match operation {
                    Operation::User {
                        application_id,
                        bytes,
                    } => {
                        block.with_raw_operation(application_id, bytes);
                    }
                    Operation::System(system_operation) => {
                        block.with_system_operation(system_operation);
                    }
                }
            }
        })
        .await
    }
}
