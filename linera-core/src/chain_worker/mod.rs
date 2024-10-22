// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A worker to handle a single chain.

mod actor;
mod config;
mod delivery_notifier;
mod state;

pub(super) use self::delivery_notifier::DeliveryNotifier;
#[cfg(test)]
pub(crate) use self::state::CrossChainUpdateHelper;
pub use self::{
    actor::{ChainWorkerActor, ChainWorkerRequest},
    config::ChainWorkerConfig,
    state::ChainWorkerState,
};
