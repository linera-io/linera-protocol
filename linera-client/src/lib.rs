// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a convenient library for writing a Linera client application.

#![recursion_limit = "512"]
#![deny(missing_docs)]
#![allow(async_fn_in_trait)]

/// Listens for notifications on the chains tracked by a client and reacts to them.
pub mod chain_listener;
/// The context bundling the wallet, storage, and configuration a client operates with.
pub mod client_context;
pub use client_context::ClientContext;
/// Prometheus metrics collected by the client.
#[cfg(not(web))]
pub mod client_metrics;
/// Command-line options shared by the client binaries.
pub mod client_options;
pub use client_options::Options;
/// Configuration types for wallets, committees, and validator servers.
pub mod config;
mod error;
/// Assorted parsing and command-line helper utilities.
pub mod util;

/// Tooling for running throughput benchmarks against a network.
#[cfg(not(web))]
pub mod benchmark;

#[cfg(test)]
mod unit_tests;

pub use error::Error;
