// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A worker to handle a single chain.

mod config;
mod state;

#[cfg(test)]
pub(crate) use self::state::CrossChainUpdateHelper;
pub use self::{config::ChainWorkerConfig, state::ChainWorkerState};
