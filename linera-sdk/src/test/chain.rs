// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A reference to a single microchain inside a [`TestValidator`].
//!
//! This allows manipulating a test microchain.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use cargo_toml::Manifest;
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{Blob, BlockHeight, Bytecode, CompressedBytecode},
    identifiers::{ApplicationId, BytecodeId, ChainDescription, ChainId, MessageId},
};
use linera_chain::{types::ConfirmedBlockCertificate, ChainError, ChainExecutionContext};
use linera_core::{data_types::ChainInfoQuery, worker::WorkerError};
use linera_execution::{
    system::{SystemExecutionError, SystemOperation, CREATE_APPLICATION_MESSAGE_INDEX},
    ExecutionError, Query, Response,
};
use linera_storage::Storage as _;
use serde::Serialize;
use tokio::{fs, sync::Mutex};

use super::{BlockBuilder, TestValidator};
use crate::{ContractAbi, ServiceAbi};

/// A reference to a single microchain inside a [`TestValidator`].
pub struct ActiveChain {
    key_pair: KeyPair,
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
    pub fn new(key_pair: KeyPair, description: ChainDescription, validator: TestValidator) -> Self {
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

    /// Returns the [`PublicKey`] of the active owner of this microchain.
    pub fn public_key(&self) -> PublicKey {
        self.key_pair.public()
    }

    /// Returns the [`KeyPair`] of the active owner of this microchain.
    pub fn key_pair(&self) -> &KeyPair {
        &self.key_pair
    }

    /// Sets the [`KeyPair`] to use for signing new blocks.
    pub fn set_key_pair(&mut self, key_pair: KeyPair) {
        self.key_pair = key_pair
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

    async fn try_add_block_with_blobs(
        &self,
        block_builder: impl FnOnce(&mut BlockBuilder),
        blobs: Vec<Blob>,
    ) -> anyhow::Result<ConfirmedBlockCertificate> {
        let mut tip = self.tip.lock().await;
        let mut block = BlockBuilder::new(
            self.description.into(),
            self.key_pair.public().into(),
            tip.as_ref(),
            self.validator.clone(),
        );

        block_builder(&mut block);

        // TODO(#2066): Remove boxing once call-stack is shallower
        let certificate = Box::pin(block.try_sign()).await?;

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

    /// Publishes the bytecodes in the crate calling this method to this microchain.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecodes
    /// to be published on this chain. Returns the bytecode ID to reference the published bytecode.
    pub async fn publish_current_bytecode<Abi, Parameters, InstantiationArgument>(
        &self,
    ) -> BytecodeId<Abi, Parameters, InstantiationArgument> {
        self.publish_bytecodes_in(".").await
    }

    /// Publishes the bytecodes in the crate at `repository_path`.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecodes
    /// to be published on this chain. Returns the bytecode ID to reference the published bytecode.
    pub async fn publish_bytecodes_in<Abi, Parameters, InstantiationArgument>(
        &self,
        repository_path: impl AsRef<Path>,
    ) -> BytecodeId<Abi, Parameters, InstantiationArgument> {
        let repository_path = fs::canonicalize(repository_path)
            .await
            .expect("Failed to obtain absolute application repository path");
        Self::build_bytecodes_in(&repository_path).await;
        let (contract, service) = self.find_bytecodes_in(&repository_path).await;
        let contract_blob = Blob::new_contract_bytecode(contract);
        let service_blob = Blob::new_service_bytecode(service);
        let contract_blob_hash = contract_blob.id().hash;
        let service_blob_hash = service_blob.id().hash;

        let bytecode_id = BytecodeId::new(contract_blob_hash, service_blob_hash);

        let certificate = self
            .add_block_with_blobs(
                |block| {
                    block.with_system_operation(SystemOperation::PublishBytecode { bytecode_id });
                },
                vec![contract_blob, service_blob],
            )
            .await;

        let executed_block = certificate.inner().block();
        assert_eq!(executed_block.messages().len(), 1);
        assert_eq!(executed_block.messages()[0].len(), 0);

        bytecode_id.with_abi()
    }

    /// Compiles the crate in the `repository` path.
    async fn build_bytecodes_in(repository: &Path) {
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
    /// contract and service bytecodes.
    ///
    /// Returns a tuple with the loaded contract and service [`CompressedBytecode`]s,
    /// ready to be published.
    async fn find_bytecodes_in(
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
            Please specify them manually using `publish_bytecode`."
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
            .expect("Failed to compress bytecodes")
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

    /// Creates an application on this microchain, using the bytecode referenced by `bytecode_id`.
    ///
    /// Returns the [`ApplicationId`] of the created application.
    ///
    /// If necessary, this microchain will subscribe to the microchain that published the
    /// bytecode to use, and fetch it.
    ///
    /// The application is instantiated using the instantiation parameters, which consist of the
    /// global static `parameters`, the one time `instantiation_argument` and the
    /// `required_application_ids` of the applications that the new application will depend on.
    pub async fn create_application<Abi, Parameters, InstantiationArgument>(
        &mut self,
        bytecode_id: BytecodeId<Abi, Parameters, InstantiationArgument>,
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

        for &dependency in &required_application_ids {
            self.register_application(dependency).await;
        }

        let creation_certificate = self
            .add_block(|block| {
                block.with_system_operation(SystemOperation::CreateApplication {
                    bytecode_id: bytecode_id.forget_abi(),
                    parameters,
                    instantiation_argument,
                    required_application_ids,
                });
            })
            .await;

        let block = creation_certificate.inner().block();
        assert_eq!(block.messages().len(), 1);
        let creation = MessageId {
            chain_id: block.header.chain_id,
            height: block.header.height,
            index: CREATE_APPLICATION_MESSAGE_INDEX,
        };

        ApplicationId {
            bytecode_id: bytecode_id.just_abi(),
            creation,
        }
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

    /// Registers on this chain an application created on another chain.
    pub async fn register_application<Abi>(&self, application_id: ApplicationId<Abi>) {
        if self.needs_application_description(application_id).await {
            let source_chain = self.validator.get_chain(&application_id.creation.chain_id);

            let request_certificate = self
                .add_block(|block| {
                    block.with_request_for_application(application_id);
                })
                .await;

            let register_certificate = source_chain
                .add_block(|block| {
                    block.with_messages_from(&request_certificate);
                })
                .await;

            let final_certificate = self
                .add_block(|block| {
                    block.with_messages_from(&register_certificate);
                })
                .await;

            assert_eq!(final_certificate.outgoing_message_count(), 0);
        }
    }

    /// Checks if the `application_id` is missing from this microchain.
    async fn needs_application_description<Abi>(&self, application_id: ApplicationId<Abi>) -> bool {
        let description_result = self
            .validator
            .worker()
            .describe_application(self.id(), application_id.forget_abi())
            .await;

        match description_result {
            Ok(_) => false,
            Err(WorkerError::ChainError(boxed_chain_error))
                if matches!(
                    &*boxed_chain_error,
                    ChainError::ExecutionError(
                        execution_error,
                        ChainExecutionContext::DescribeApplication,
                    ) if matches!(
                        **execution_error,
                        ExecutionError::SystemError(SystemExecutionError::UnknownApplicationId(_))
                    )
                ) =>
            {
                true
            }
            Err(_) => panic!("Failed to check known bytecode locations"),
        }
    }

    /// Executes a `query` on an `application`'s state on this microchain.
    ///
    /// Returns the deserialized response from the `application`.
    pub async fn query<Abi>(
        &self,
        application_id: ApplicationId<Abi>,
        query: Abi::Query,
    ) -> Abi::QueryResponse
    where
        Abi: ServiceAbi,
    {
        let query_bytes = serde_json::to_vec(&query).expect("Failed to serialize query");

        let response = self
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

        match response {
            Response::User(bytes) => {
                serde_json::from_slice(&bytes).expect("Failed to deserialize query response")
            }
            Response::System(_) => unreachable!("User query returned a system response"),
        }
    }

    /// Executes a GraphQL `query` on an `application`'s state on this microchain.
    ///
    /// Returns the deserialized GraphQL JSON response from the `application`.
    pub async fn graphql_query<Abi>(
        &self,
        application_id: ApplicationId<Abi>,
        query: impl Into<async_graphql::Request>,
    ) -> serde_json::Value
    where
        Abi: ServiceAbi<Query = async_graphql::Request, QueryResponse = async_graphql::Response>,
    {
        let query = query.into();
        let query_str = query.query.clone();
        let response = self.query(application_id, query).await;
        if !response.errors.is_empty() {
            panic!(
                "GraphQL query:\n{}\nyielded errors:\n{:#?}",
                query_str, response.errors
            );
        }
        response
            .data
            .into_json()
            .expect("Unexpected non-JSON query response")
    }
}
