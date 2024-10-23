// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a convenient library for writing a Linera client application.

#![recursion_limit = "256"]
#![deny(clippy::large_futures)]

pub mod chain_listener;
pub mod client_context;
pub mod client_options;
pub mod config;
mod error;
pub mod persistent;
pub mod storage;
pub mod util;
pub mod wallet;

#[cfg(test)]
mod unit_tests;

pub use error::Error;
