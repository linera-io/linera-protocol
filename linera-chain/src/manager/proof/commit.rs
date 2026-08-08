// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The commit rule: what it takes for a block to become final, and what a node does about it.
//!
//! The protocol has a two-step commit rule outside the fast round — validate, then confirm — and
//! a one-step rule inside it. This module pins down both, and connects "a confirmed block
//! certificate exists" to the observable state a node reaches.

use crate::{
    data_types::proof::quorum::CertificateCarriesCorrectVote,
    manager::proof::voting::ConfirmationNeedsValidatedCertificate,
};

/// **Definition (Committed block).** A block `B` at height `h` of a chain is *committed* when a
/// [`ConfirmedBlockCertificate`] for `B`, valid for the committee of its epoch, exists — that
/// is, when a quorum has cast confirmation votes for `B`.
///
/// The commit rule is therefore:
///
/// | round | rule |
/// |---|---|
/// | [`Round::Fast`] | a quorum of confirmation votes on a super owner's fast proposal, with no validation step |
/// | any other round `r` | a quorum of validation votes for `B` in round `r`, then a quorum of confirmation votes for `B` in round `r` |
///
/// Commitment is a property of the *world*, not of any one node: a block can be committed before
/// any particular correct validator learns of it. The node-local reflection of it is
/// [`TipAdvancesOnlyOnValidCertificate`].
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
pub trait CommittedBlock {}

/// **Lemma (A commit outside the fast round rests on a validated block certificate).** If a
/// [`ConfirmedBlockCertificate`] for `B` is certified in a round `r` other than
/// [`Round::Fast`](linera_base::data_types::Round::Fast), then a valid
/// [`ValidatedBlockCertificate`] for `B` in the *same* round `r` exists.
///
/// *Proof.* By [`CertificateCarriesCorrectVote`] some correct validator cast a confirmation vote
/// for `B` in round `r`. By [`ConfirmationNeedsValidatedCertificate`], since `r` is not the fast
/// round, a valid [`ValidatedBlockCertificate`] for `B` in round `r` existed when it did so. ∎
///
/// This is what lets the safety argument reason exclusively about *validated* certificates:
/// every commit above the fast round is backed by one, in its own round.
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
pub trait CommitRestsOnValidation:
    CertificateCarriesCorrectVote + ConfirmationNeedsValidatedCertificate
{
}

/// **Lemma (The tip advances only on a verified certificate).** A correct validator's
/// [`ChainTipState::next_block_height`] passes from `h` to `h + 1`, and its
/// [`block_hashes`](crate::ChainStateView) records a hash at `h`, only for a block carried by a
/// [`ConfirmedBlockCertificate`] that has passed [`check`] against the committee of the block's
/// epoch.
///
/// *Proof.* The tip register is advanced in one place in `linera_chain::chain`, at the end of
/// the private `ChainStateView::apply_confirmed_block` (`tip.next_block_height.
/// try_add_assign_one()`), reached only from `ChainWorkerState::execute_contiguous_block` and
/// `execute_block_with_checkpoint_restore`. Both are reached only through
/// `ChainWorkerState::process_confirmed_block`, whose every non-early-return path first
/// evaluates `certificate.check(&committee)?` with `committee` fetched for `block.header.epoch`.
/// The early returns — the `tip.next_block_height > height` skip and the `Preprocess` dispatch —
/// do not advance the tip. ∎
///
/// Two paths deserve explicit mention because they weaken the *precondition* without weakening
/// this lemma. `pre_checkpoint_block_trust` lets a hash recorded by an earlier checkpoint
/// certificate bypass the already-processed skip; and a checkpoint block may install a state
/// snapshot rather than replay its ancestors. Neither bypasses `certificate.check`, so a block
/// entering the tip is always quorum-certified; what they bypass is the *re-execution* of
/// ancestors, which is a matter of state-transition correctness rather than of agreement.
///
/// [`ChainTipState::next_block_height`]: crate::ChainTipState::next_block_height
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`check`]: crate::types::ConfirmedBlockCertificate::check
pub trait TipAdvancesOnlyOnValidCertificate {}
