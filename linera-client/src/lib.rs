// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a convenient library for writing a Linera client application.

#![recursion_limit = "256"]
#![deny(clippy::large_futures)]
#![allow(async_fn_in_trait)]

pub mod chain_listener;
pub mod client_context;
#[cfg(not(web))]
pub mod client_metrics;
pub mod client_options;
pub mod config;
mod error;
pub mod util;
pub mod wallet;

#[cfg(not(web))]
pub mod benchmark;

#[cfg(test)]
mod unit_tests;

pub use error::Error;
