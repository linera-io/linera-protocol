// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides the executables needed to operate a Linera service, including a placeholder wallet acting as a GraphQL service for user interfaces.

pub mod chain_listener;
pub mod cli_wrappers;
pub mod config;
pub mod faucet;
pub mod grpc_proxy;
pub mod node_service;
pub mod project;
pub mod prometheus_server;
pub mod storage;
pub mod util;

#[cfg(with_testing)]
use {
    linera_base::sync::Lazy,
    tokio::sync::Mutex,
};

/// A static lock to prevent integration tests from running in parallel.
#[cfg(with_testing)]
pub static INTEGRATION_TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
