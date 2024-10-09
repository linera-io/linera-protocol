// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides the executables needed to operate a Linera service, including a placeholder wallet acting as a GraphQL service for user interfaces.

#![deny(clippy::large_futures)]

pub mod cli_wrappers;
pub mod faucet;
pub mod node_service;
pub mod project;
#[cfg(with_metrics)]
pub mod prometheus_server;
pub mod util;
pub mod wallet;
