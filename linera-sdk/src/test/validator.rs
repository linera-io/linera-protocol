// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A minimal validator implementation suited for tests.
//!
//! The [`TestValidator`] is a minimal validator with a single shard. Micro-chains can be added to
//! it, and blocks can be added to each micro-chain individually.

use super::ActiveChain;
use dashmap::DashMap;
use linera_base::{
    crypto::KeyPair,
    data_types::Timestamp,
    identifiers::{ChainDescription, ChainId},
};
use linera_core::worker::WorkerState;
use linera_execution::{
    committee::{Committee, ValidatorName},
    WasmRuntime,
};
use linera_storage::{MemoryStoreClient, Store};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{Mutex, MutexGuard};

/// A minimal validator implementation suited for tests.
pub struct TestValidator {
    key_pair: KeyPair,
    committee: Committee,
    worker: Arc<Mutex<WorkerState<MemoryStoreClient>>>,
    root_chain_counter: Arc<AtomicUsize>,
    chains: Arc<DashMap<ChainId, ActiveChain>>,
}

impl Default for TestValidator {
    fn default() -> Self {
        let key_pair = KeyPair::generate();
        let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
        let client = MemoryStoreClient::new(Some(WasmRuntime::default()));

        let worker = WorkerState::new(
            "Single validator node".to_string(),
            Some(key_pair.copy()),
            client,
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
    /// Returns the locked [`WorkerState`] of this validator.
    pub(crate) async fn worker(&self) -> MutexGuard<WorkerState<MemoryStoreClient>> {
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

    /// Creates a new micro-chain and returns the [`ActiveChain`] that can be used to add blocks to
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
}
