// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]
#![deny(missing_docs)]

//! This module provides the executables needed to operate a Linera service, including a placeholder wallet acting as a GraphQL service for user interfaces.

pub mod cli;
pub mod cli_wrappers;
/// Configuration types for the service binaries.
pub mod config;
/// The controller that orchestrates worker services.
pub mod controller;
/// The GraphQL node service exposing wallet and chain state.
pub mod node_service;
/// Helpers for creating and building application projects.
pub mod project;
/// Tracking of GraphQL subscriptions by query.
pub mod query_subscription;
/// Storage backend selection for the service binaries.
pub mod storage;
pub mod task_processor;
/// Assorted helper utilities for the service binaries.
pub mod util;

pub use linera_wallet_json::PersistentWallet as Wallet;
