// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The consensus correctness argument for the chain manager, from the system model to the
//! safety theorem.
//!
//! This is the chain-crate half of the specification; the progress and liveness half lives in
//! `linera_core::proof`, which cites into here. [`crate::spec`] gives the reading order across
//! both and explains the conventions.
//!
//! # Layout
//!
//! | module | contents |
//! |---|---|
//! | [`model`] | system model, and the assumptions safety rests on |
//! | [`voting`] | voting rules: what a correct validator's state must be for it to sign |
//! | [`rounds`] | the round register: what it means, and why it only grows |
//! | [`locking`] | locking invariants, proved by induction over an instance's transitions |
//! | [`commit`] | the commit rule, and what a node does when a block commits |
//! | [`safety`] | the safety theorem: at most one block per chain and height |
//! | [`pacemaker`] | view changes: how a height leaves a round, for the progress argument |
//!
//! The single most important statement is [`safety::CommitAgreement`]; the single most
//! substantial proof is [`safety::LockPreservation`].
//!
//! # What is proved, in one paragraph
//!
//! A correct validator will not cast a validation vote for a block it has not been shown a
//! justification for, and will not confirm a block that a quorum has not validated in the very
//! round it confirms in ([`voting`]). Its lock and its cast-vote rounds only ever move up
//! ([`rounds`], [`locking`]), so it votes at most once per round and per kind. Any two quorums
//! share a correct validator ([`crate::data_types::proof::quorum`]), so those per-validator
//! limits become per-round limits on the certificates that can exist. Finally, a validator
//! abandons a block it confirmed only in exchange for a certificate strictly above its own
//! confirmation round, which lets an induction over rounds show that once a block is committed,
//! no later round validates anything else ([`safety::LockPreservation`]) — hence at most one
//! block per height is ever committed ([`safety::CommitAgreement`]).

pub mod commit;
pub mod locking;
pub mod model;
pub mod pacemaker;
pub mod rounds;
pub mod safety;
pub mod voting;
