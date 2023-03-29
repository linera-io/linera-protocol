// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A reference to a single micro-chain inside a [`TestValidator`].
//!
//! This allows manipulating a test micro-chain.

use super::{BlockBuilder, TestValidator};
use cargo_toml::Manifest;
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainDescription, ChainId, EffectId},
};
use linera_chain::data_types::Certificate;
use linera_execution::{
    system::{SystemChannel, SystemOperation},
    Bytecode,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

/// A reference to a single micro-chain inside a [`TestValidator`].
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
    /// Creates a new [`ActiveChain`] instance referencing a new empty micro-chain in the
    /// `validator`.
    ///
    /// The micro-chain has a single owner that uses the `key_pair` to produce blocks. The
    /// `description` is used as the identifier of the micro-chain.
    pub fn new(key_pair: KeyPair, description: ChainDescription, validator: TestValidator) -> Self {
        ActiveChain {
            key_pair,
            description,
            tip: Arc::default(),
            validator,
        }
    }

    /// Returns the [`ChainId`] of this micro-chain.
    pub fn id(&self) -> ChainId {
        self.description.into()
    }

    /// Returns the [`PublicKey`] of the owner of this micro-chain.
    pub fn public_key(&self) -> PublicKey {
        self.key_pair.public()
    }

    /// Adds a block to this micro-chain.
    ///
    /// The `block_builder` parameter is a closure that should use the [`BlockBuilder`] parameter
    /// to provide the block's contents.
    pub async fn add_block(&self, block_builder: impl FnOnce(&mut BlockBuilder)) {
        let mut tip = self.tip.lock().await;
        let mut block = BlockBuilder::new(
            self.description.into(),
            self.key_pair.public().into(),
            tip.as_ref(),
            self.validator.clone(),
        );

        block_builder(&mut block);

        let certificate = block.sign().await;

        self.validator
            .worker()
            .await
            .fully_handle_certificate(certificate.clone(), vec![])
            .await
            .expect("Rejected certificate");

        *tip = Some(certificate);
    }

    /// Publishes the bytecodes in the crate calling this method to this micro-chain.
    ///
    /// Searches the Cargo manifest for binaries that end with `contract` and `service`, builds
    /// them for WebAssembly and uses the generated binaries as the contract and service bytecodes
    /// to be published on this chain. Returns the bytecode ID to reference the published bytecode.
    pub async fn publish_current_bytecode(&self) -> BytecodeId {
        Self::build_bytecodes();
        let (contract, service) = self.find_current_bytecodes().await;

        self.add_block(|block| {
            block.with_system_operation(SystemOperation::PublishBytecode { contract, service });
        })
        .await;

        let publish_effect_id = EffectId {
            chain_id: self.description.into(),
            height: self.tip_height().await,
            index: 0,
        };

        self.add_block(|block| {
            block.with_incoming_message(publish_effect_id);
        })
        .await;

        BytecodeId(publish_effect_id)
    }

    /// Compiles the crate calling this method to generate the WebAssembly binaries.
    fn build_bytecodes() {
        let output = std::process::Command::new("cargo")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
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
    async fn find_current_bytecodes(&self) -> (Bytecode, Bytecode) {
        let mut cargo_manifest =
            Manifest::from_path("Cargo.toml").expect("Failed to load Cargo.toml manifest");

        cargo_manifest
            .complete_from_path(Path::new("."))
            .expect("Failed to populate manifest with information inferred from the repository");

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

    /// Returns the height of the tip of this micro-chain.
    async fn tip_height(&self) -> BlockHeight {
        self.tip
            .lock()
            .await
            .as_ref()
            .expect("Block was not successfully added")
            .value
            .block()
            .height
    }

    /// Subscribes this micro-chain to the bytecodes published on the `publisher_id` micro-chain.
    pub async fn subscribe_to_published_bytecodes_from(&mut self, publisher_id: ChainId) {
        let publisher = self.validator.get_chain(&publisher_id);

        self.add_block(|block| {
            block.with_system_operation(SystemOperation::Subscribe {
                chain_id: publisher.id(),
                channel: SystemChannel::PublishedBytecodes,
            });
        })
        .await;

        let effect_id = EffectId {
            chain_id: self.description.into(),
            height: self.tip_height().await,
            index: 0,
        };

        publisher
            .add_block(|block| {
                block.with_incoming_message(effect_id);
            })
            .await;

        let effect_id = EffectId {
            chain_id: publisher.id(),
            height: publisher.tip_height().await,
            index: 0,
        };

        self.add_block(|block| {
            block.with_incoming_message(effect_id);
        })
        .await;
    }
}
