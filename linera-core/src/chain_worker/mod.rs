// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A worker to handle a single chain.

use std::{future::Future, pin::Pin};

mod actor;
mod config;
mod delivery_notifier;
mod state;

/// A boxed future that is `Send` on native and `!Send` on wasm.
///
/// On native targets, futures must be `Send` because they are spawned on multi-threaded
/// runtimes. On wasm, `Send` is not required (single-threaded) and some storage contexts
/// do not implement `Sync`, making `&self` futures non-`Send`.
#[cfg(not(web))]
type BoxedFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// See non-web variant for documentation.
#[cfg(web)]
type BoxedFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

pub(super) use self::delivery_notifier::DeliveryNotifier;
#[cfg(test)]
pub(crate) use self::state::CrossChainUpdateHelper;
pub(crate) use self::{
    actor::{
        chain_actor_channel, ChainActorEndpoint, ChainActorReceivers, ChainWorkerActor,
        ChainWorkerRequest, EventSubscriptionsResult,
    },
    config::ChainWorkerConfig,
    state::BlockOutcome,
};
