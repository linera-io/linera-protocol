// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A reference to a single microchain inside a [`TestValidator`].
//!
//! This allows manipulating a test microchain.

use super::{BlockBuilder, TestValidator};
use crate::{ContractAbi, ServiceAbi};
use cargo_toml::Manifest;
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::BlockHeight,
    identifiers::{ApplicationId, BytecodeId, ChainDescription, ChainId, EffectId},
};
use linera_chain::data_types::Certificate;
use linera_core::{data_types::ChainInfoQuery, worker::ValidatorWorker};
use linera_execution::{
    system::{SystemChannel, SystemEffect, SystemExecutionError, SystemOperation},
    Bytecode, Effect, Query, Response,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, sync::Mutex};

/// A reference to a single microchain inside a [`TestValidator`].
pub struct ActiveChain {
    key_pair: KeyPair,
    description: ChainDescription,
    tip: Arc<Mutex<Option<Certificate>>>,
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

    /// Returns the [`PublicKey`] of the owner of this microchain.
    pub fn public_key(&self) -> PublicKey {
        self.key_pair.public()
    }

    /// Adds a block to this microchain.
    ///
    /// The `block_builder` parameter is a closure that should use the [`BlockBuilder`] parameter
    /// to provide the block's contents.
    pub async fn add_block(&self, block_builder: impl FnOnce(&mut BlockBuilder)) -> Vec<EffectId> {
        let mut tip = self.tip.lock().await;
        let mut block = BlockBuilder::new(
            self.description.into(),
            self.key_pair.public().into(),
            tip.as_ref(),
            self.validator.clone(),
        );

        block_builder(&mut block);

        let (certificate, effect_ids) = block.sign().await;

        self.validator
            .worker()
            .await
            .fully_handle_certificate(certificate.clone(), vec![])
            .await
            .expect("Rejected certificate");

        *tip = Some(certificate);

        effect_ids
    }

    /// Receives all queued messages in all inboxes of this microchain.
    ///
    /// Adds a block to this microchain that receives all queued messages in the microchains
    /// inboxes.
    pub async fn handle_received_effects(&self) {
        let chain_id = self.id();
        let (information, _) = self
            .validator
            .worker()
            .await
            .handle_chain_info_query(ChainInfoQuery::new(chain_id).with_pending_messages())
            .await
            .expect("Failed to query chain's pending messages");
        let messages = information.info.requested_pending_messages;

        self.add_block(|block| {
            block.with_raw_messages(messages);
        })
        .await;
    }

    /// Publishes the bytecodes in the crate calling this method to this microchain.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecodes
    /// to be published on this chain. Returns the bytecode ID to reference the published bytecode.
    pub async fn publish_current_bytecode<Abi>(&self) -> BytecodeId<Abi> {
        self.publish_bytecodes_in(".").await
    }

    /// Publishes the bytecodes in the crate at `repository_path`.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecodes
    /// to be published on this chain. Returns the bytecode ID to reference the published bytecode.
    pub async fn publish_bytecodes_in<Abi>(
        &self,
        repository_path: impl AsRef<Path>,
    ) -> BytecodeId<Abi> {
        let repository_path = fs::canonicalize(repository_path)
            .await
            .expect("Failed to obtain absolute application repository path");
        Self::build_bytecodes_in(&repository_path).await;
        let (contract, service) = self.find_bytecodes_in(&repository_path).await;

        let publish_effects = self
            .add_block(|block| {
                block.with_system_operation(SystemOperation::PublishBytecode { contract, service });
            })
            .await;

        assert_eq!(publish_effects.len(), 1);

        self.add_block(|block| {
            block.with_incoming_message(publish_effects[0]);
        })
        .await;

        BytecodeId::new(publish_effects[0]).with_abi()
    }

    /// Compiles the crate in the `repository` path.
    async fn build_bytecodes_in(repository: &Path) {
        let output = std::process::Command::new("cargo")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .current_dir(repository)
            .output()
            .expect("Failed to build WASM binaries");

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
    /// Returns a tuple with the loaded contract and service [`Bytecode`]s.
    async fn find_bytecodes_in(&self, repository: &Path) -> (Bytecode, Bytecode) {
        let manifest_path = repository.join("Cargo.toml");
        let cargo_manifest =
            Manifest::from_path(manifest_path).expect("Failed to load Cargo.toml manifest");

        let binaries: Vec<_> = cargo_manifest
            .bin
            .into_iter()
            .filter_map(|binary| binary.name)
            .filter(|name| name.ends_with("service") || name.ends_with("contract"))
            .collect();

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

        let base_path = PathBuf::from("../target/wasm32-unknown-unknown/release");
        let contract_path = base_path.join(format!("{}.wasm", contract_binary));
        let service_path = base_path.join(format!("{}.wasm", service_binary));

        (
            Bytecode::load_from_file(contract_path)
                .await
                .expect("Failed to load contract bytecode from file"),
            Bytecode::load_from_file(service_path)
                .await
                .expect("Failed to load service bytecode from file"),
        )
    }

    /// Returns the height of the tip of this microchain.
    pub async fn get_tip_height(&self) -> BlockHeight {
        self.tip
            .lock()
            .await
            .as_ref()
            .expect("Block was not successfully added")
            .value()
            .height()
    }

    /// Subscribes this microchain to the bytecodes published on the `publisher_id` microchain.
    pub async fn subscribe_to_published_bytecodes_from(&mut self, publisher_id: ChainId) {
        let publisher = self.validator.get_chain(&publisher_id);

        let request_effects = self
            .add_block(|block| {
                block.with_system_operation(SystemOperation::Subscribe {
                    chain_id: publisher.id(),
                    channel: SystemChannel::PublishedBytecodes,
                });
            })
            .await;

        assert_eq!(request_effects.len(), 1);

        let accept_effects = publisher
            .add_block(|block| {
                block.with_incoming_message(request_effects[0]);
            })
            .await;

        assert_eq!(accept_effects.len(), 1);

        self.add_block(|block| {
            block.with_incoming_message(accept_effects[0]);
        })
        .await;
    }

    /// Creates an application on this microchain, using the bytecode referenced by `bytecode_id`.
    ///
    /// Returns the [`ApplicationId`] of the created application.
    ///
    /// If necessary, this microchain will subscribe to the microchain that published the
    /// bytecode to use, and fetch it.
    ///
    /// The application is initialized using the initialization parameters, which consist of the
    /// global static `parameters`, the one time `initialization_argument` and the
    /// `required_application_ids` of the applications that the new application will depend on.
    pub async fn create_application<A>(
        &mut self,
        bytecode_id: BytecodeId<A>,
        parameters: A::Parameters,
        initialization_argument: A::InitializationArgument,
        required_application_ids: Vec<ApplicationId>,
    ) -> ApplicationId<A>
    where
        A: ContractAbi,
    {
        let bytecode_location_effect = if self.needs_bytecode_location(bytecode_id).await {
            self.subscribe_to_published_bytecodes_from(bytecode_id.effect_id.chain_id)
                .await;
            Some(self.find_bytecode_location(bytecode_id).await)
        } else {
            None
        };

        let parameters = serde_json::to_vec(&parameters).unwrap();
        let initialization_argument = serde_json::to_vec(&initialization_argument).unwrap();

        for &dependency in &required_application_ids {
            self.register_application(dependency).await;
        }

        let creation_effects = self
            .add_block(|block| {
                if let Some(effect_id) = bytecode_location_effect {
                    block.with_incoming_message(effect_id);
                }

                block.with_system_operation(SystemOperation::CreateApplication {
                    bytecode_id: bytecode_id.forget_abi(),
                    parameters,
                    initialization_argument,
                    required_application_ids,
                });
            })
            .await;

        assert_eq!(creation_effects.len(), 1);

        ApplicationId {
            bytecode_id,
            creation: creation_effects[0],
        }
    }

    /// Checks if the `bytecode_id` is missing from this microchain.
    async fn needs_bytecode_location<Abi>(&self, bytecode_id: BytecodeId<Abi>) -> bool {
        let applications = self
            .validator
            .worker()
            .await
            .load_application_registry(self.id())
            .await
            .expect("Failed to load application registry");

        applications
            .bytecode_locations_for([bytecode_id.forget_abi()])
            .await
            .expect("Failed to check known bytecode locations")
            .is_empty()
    }

    /// Finds the effect that sends the message with the bytecode location of `bytecode_id`.
    async fn find_bytecode_location<Abi>(&self, bytecode_id: BytecodeId<Abi>) -> EffectId {
        for height in bytecode_id.effect_id.height.0.. {
            let certificate = self
                .validator
                .worker()
                .await
                .read_certificate(bytecode_id.effect_id.chain_id, height.into())
                .await
                .expect("Failed to load certificate to search for bytecode location")
                .expect("Bytecode location not found");

            let effect_index = certificate.value().effects().iter().position(|effect| {
                matches!(
                    &effect.effect,
                    Effect::System(SystemEffect::BytecodeLocations { locations })
                        if locations.iter().any(|(id, _)| id == &bytecode_id.forget_abi())
                )
            });

            if let Some(index) = effect_index {
                return EffectId {
                    chain_id: bytecode_id.effect_id.chain_id,
                    height: BlockHeight(height),
                    index: index.try_into().expect(
                        "Incompatible `EffectId` index types in \
                        `linera-sdk` and `linera-execution`",
                    ),
                };
            }
        }

        panic!("Bytecode not found in the chain it was supposed to be published on");
    }

    /// Registers on this chain an application created on another chain.
    pub async fn register_application<Abi>(&self, application_id: ApplicationId<Abi>) {
        if self.needs_application_description(application_id).await {
            let source_chain = self.validator.get_chain(&application_id.creation.chain_id);

            let request_effects = self
                .add_block(|block| {
                    block.with_request_for_application(application_id);
                })
                .await;

            assert_eq!(request_effects.len(), 1);

            let register_effects = source_chain
                .add_block(|block| {
                    block.with_incoming_message(request_effects[0]);
                })
                .await;

            assert_eq!(register_effects.len(), 1);

            let final_effects = self
                .add_block(|block| {
                    block.with_incoming_message(register_effects[0]);
                })
                .await;

            assert_eq!(final_effects.len(), 0);
        }
    }

    /// Checks if the `application_id` is missing from this microchain.
    async fn needs_application_description<Abi>(&self, application_id: ApplicationId<Abi>) -> bool {
        let applications = self
            .validator
            .worker()
            .await
            .load_application_registry(self.id())
            .await
            .expect("Failed to load application registry");

        match applications
            .describe_application(application_id.forget_abi())
            .await
        {
            Ok(_) => false,
            Err(SystemExecutionError::UnknownApplicationId(_)) => true,
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
            .await
            .query_application(
                self.id(),
                &Query::User {
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
}
