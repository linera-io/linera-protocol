// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A worker to handle a single chain.

mod config;
mod delivery_notifier;
pub(crate) mod handle;
pub(crate) mod state;

pub(super) use self::delivery_notifier::DeliveryNotifier;
#[cfg(test)]
pub(crate) use self::state::CrossChainUpdateHelper;
pub(crate) use self::{
    config::ChainWorkerConfig,
    handle::ChainHandle,
    state::{BlockOutcome, EventSubscriptionsResult},
};
