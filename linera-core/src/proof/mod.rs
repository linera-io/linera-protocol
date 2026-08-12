// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The progress and liveness half of the microchain consensus correctness specification.
//!
//! The safety half is in [`linera_chain::manager::proof`] and does not depend on anything here.
//! This half does the reverse: it cites the chain crate's voting, locking and pacemaker results
//! throughout, and adds the assumptions that mention time and availability.
//!
//! | module | contents |
//! |---|---|
//! | [`assumptions`] | synchrony, availability, leader fairness, timeout growth, reachability |
//! | [`availability`] | what a certified block guarantees to everyone else, and what a crash costs |
//! | [`progress`] | the individual steps a correct driver can force after GST |
//! | [`liveness`] | the liveness theorems, and what they exclude |
//! | [`notifications`] | what a notification tells a client, and why no proof relies on one |
//!
//! The `linera-spec` crate gives the reading order across both crates.
//!
//! # Why liveness lives in this crate
//!
//! A Linera validator does not propose blocks and does not advance rounds on its own. Both are
//! done by a [`ChainClient`](crate::client::ChainClient) run by a chain owner — which is why
//! [`assumptions::ActiveCorrectDriver`] is an assumption rather than a lemma, and why the
//! progress lemmas cite `linera_core::client` rather than `linera_chain::manager`. Placing them
//! here keeps every statement next to the code that discharges it.

pub mod assumptions;
pub mod availability;
pub mod liveness;
pub mod notifications;
pub mod progress;
