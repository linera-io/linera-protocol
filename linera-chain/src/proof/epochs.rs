// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committees, epochs, and how a node comes to trust one.
//!
//! A committee is identified by an [`Epoch`], and epochs are managed on the admin chain: created by
//! [`AdminOperation::CreateCommittee`], retired by [`AdminOperation::RemoveCommittee`]. Every
//! certificate is judged by the committee for the epoch its block declares, so before anything can
//! be verified a node must know which committee that is.
//!
//! This module is about where that knowledge comes from. The results here are what let the rest of
//! the specification induct on epochs at all.
//!
//! [`Epoch`]: linera_base::data_types::Epoch
//! [`AdminOperation::CreateCommittee`]: linera_execution::system::AdminOperation::CreateCommittee
//! [`AdminOperation::RemoveCommittee`]: linera_execution::system::AdminOperation::RemoveCommittee

use crate::manager::proof::model::CorrectValidator;

/// **Theorem (No committee certifies its own introduction).** For every epoch, a node can learn
/// that epoch's committee only from evidence certified under a *strictly earlier* epoch, or — for
/// epoch zero alone — from network configuration that is not a certificate at all. The relation
/// "the committee for `e` was learned from a certificate in epoch `e'`" is therefore well founded,
/// with `e' < e`.
///
/// This is what makes induction on the epoch legitimate, and it is the property a reconfiguration
/// scheme is easiest to get wrong. Were a committee able to authenticate the evidence that
/// introduces it — a per-chain genesis configuration naming a committee would do it — an attacker
/// who could produce such a configuration could mint a committee out of nothing and have it vouch
/// for itself. Nothing here would detect that, because every signature would check out.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | `ExecutionRuntimeContext::get_committee_hashes`, reached from `ChainWorkerState::committee_for_epoch` before `certificate.check` |
/// | reads | for epoch `0`, `NetworkDescription::genesis_committee_blob_hash`; otherwise the admin chain's [`EPOCH_STREAM_NAME`] event at index `epoch` |
/// | writes | nothing |
/// | failure | `ExecutionError::EventsNotFound` when the epoch's event is absent, so the certificate is not verified rather than verified optimistically |
///
/// *Proof.* By descent on the epoch.
///
/// *The base is not a certificate.* Epoch `0` resolves to
/// `NetworkDescription::genesis_committee_blob_hash`. A [`NetworkDescription`] is configuration a
/// node is given, network-wide and identical for every chain. No chain can introduce one, and in
/// particular creating a chain cannot introduce a committee — the failure mode this theorem exists
/// to exclude.
///
/// *Every step descends.* For `epoch > 0` the committee hash is read from the event at index
/// `epoch` of the admin chain's epoch stream, written by [`AdminOperation::CreateCommittee`]. To
/// hold that event a node must have processed the admin-chain block containing it, which by
/// [`TipAdvancesOnlyOnValidCertificate`] required a valid certificate for that block, judged
/// against the committee for the epoch that block *declares*. A block declares the epoch its chain
/// was in before executing, and a block advances the epoch at most once
/// ([`ChainError::MultipleEpochAdvances`]), so the block introducing epoch `e` declares at most
/// `e - 1`.
///
/// *There is no other route.* `committee_for_epoch` is the sole path from an epoch to a committee
/// on the verification path, and it consults exactly these two sources. When the event is missing
/// it fails with `EventsNotFound`; nothing falls back to a committee carried by the certificate
/// being checked, or by the block, or by the chain being created. ∎
///
/// **A missing epoch is recoverable, which is why the strictness costs nothing.** Refusing to
/// verify a certificate whose epoch a node has not yet learned would be an availability problem if
/// there were no way to catch up. There is: `EventsNotFound` on the admin chain's stream is a class
/// in `linera_core::proof::availability::MissingDependenciesAreRecoverable`, and
/// `send_confirmed_certificate` answers it by pushing the admin chain to that validator before
/// retrying. So the node learns the epoch and then verifies, in that order — which is the whole
/// point.
///
/// [`EPOCH_STREAM_NAME`]: linera_execution::system::EPOCH_STREAM_NAME
/// [`NetworkDescription`]: linera_base::data_types::NetworkDescription
/// [`AdminOperation::CreateCommittee`]: linera_execution::system::AdminOperation::CreateCommittee
/// [`TipAdvancesOnlyOnValidCertificate`]: crate::manager::proof::commit::TipAdvancesOnlyOnValidCertificate
/// [`ChainError::MultipleEpochAdvances`]: crate::ChainError::MultipleEpochAdvances
pub trait CommitteeKnowledgeIsWellFounded: CorrectValidator {}
