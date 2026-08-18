// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Protocol vocabulary and quorum properties of the microchain consensus specification.
//!
//! This is the base of the specification's dependency graph: [`objects`] fixes what the
//! protocol's messages *are*, and [`quorum`] establishes what a quorum of signatures buys us.
//! Nothing here depends on the chain manager, so the manager's voting, locking, commit, safety
//! and round-advancement results ([`crate::manager::proof`]) may all cite these freely.
//!
//! The `linera-spec` crate gives the full reading order and the conventions governing statement
//! names, proof obligations and dependency edges.

pub mod objects;
pub mod quorum;
