// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A minimal validator implementation suited for tests.
//!
//! The [`TestValidator`] is a minimal validator with a single shard. Micro-chains can be added to
//! it, and blocks can be added to each micro-chain individually.

use linera_base::crypto::KeyPair;
use linera_core::worker::WorkerState;
use linera_execution::{
    committee::{Committee, ValidatorName},
    WasmRuntime,
};
use linera_storage::MemoryStoreClient;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::{Mutex, MutexGuard};

/// A minimal validator implementation suited for tests.
pub struct TestValidator {
    key_pair: KeyPair,
    committee: Committee,
    worker: Arc<Mutex<WorkerState<MemoryStoreClient>>>,
    root_chain_counter: Arc<AtomicUsize>,
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
}
