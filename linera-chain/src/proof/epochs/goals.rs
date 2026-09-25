// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What committee revocation is meant to achieve, and does not yet.
//!
//! Every statement in this module is a **Goal**: a property the protocol is intended to have and
//! the implementation does not have. Goals carry no proof, and unlike an
//! [`Assumption`](crate::manager::proof::model) they cannot be discharged by a deployment — only by
//! building the mechanism they describe.
//!
//! **Nothing outside this module may name a goal as a supertrait.** A goal may depend on proved
//! statements, which reads as "once achieved, this would rest on those"; the reverse would let a
//! proof rest on something known to be false. The dependency edge runs one way, out of this module
//! only.
//!
//! Each goal states what should hold, why it does not today in terms of named code, and what would
//! discharge it. The third part is not optional: a goal nobody can say how to discharge is not
//! understood well enough to state.
//!
//! The mechanisms sketched below — commitments, staged settlement, appeals — are a design under
//! discussion, not a decided one. What is settled is the shape of the problem: [`MaxByzantineWeight`]
//! is assumed of every non-revoked committee, so the assumption accumulates until committees can be
//! retired, and today they cannot be.
//!
//! [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight

/// **Goal (Revoking an epoch never strands a chain).** A chain that has not migrated off an epoch
/// by the time that epoch is revoked must still be able to produce its next block.
///
/// *Not currently achieved.* The block that migrates a chain from epoch `e` declares `e` in its
/// header, so `certificate.check` runs against `committee_for_epoch(e)`: every route out of `e` is
/// itself a block in `e`. Revoke `e` and the chain is wedged permanently, with no recovery path.
/// Migration is also single-step — `check_next_epoch` requires exactly `current + 1`, and
/// [`ChainError::MultipleEpochAdvances`] caps a block at one advance — so a chain `k` epochs behind
/// needs `k` blocks, each requiring its then-current committee to still be live. Nothing gates
/// revocation on this: `AdminOperation::RemoveCommittee` checks that revocations are sequential and
/// that the epoch is below the *admin chain's own*, never where any other chain stands.
///
/// *What would discharge it.* A settled epoch leaves every chain with a definite last block in that
/// epoch, provable from the admin chain ([`RevokedEpochRecordBecomesDefinite`]). A chain that failed
/// to migrate then counts as migrated at that block, and the next committee may finalize its
/// successor directly, with no signature from the revoked committee. For epochs further out the
/// no-skipping rule is refined rather than dropped: a block may skip an epoch above its parent's
/// exactly when that epoch is settled and its record contains no block on this chain — which
/// absence from every commitment and appeal proves. A chain frozen at birth is revived the same way,
/// its first block grounded by the creator block.
///
/// [`ChainError::MultipleEpochAdvances`]: crate::ChainError::MultipleEpochAdvances
pub trait RevocationNeverStrandsAChain {}

/// **Goal (A revoked epoch's record becomes definite and public).** After a revoked epoch is
/// settled, whether a given block of that epoch was finalized must be decidable from the admin
/// chain alone, identically by everyone, without holding any epoch-`e` certificate.
///
/// *Not currently achieved.* Revocation is a single event with no accompanying record:
/// `Storage::is_epoch_revoked` tests for one event and nothing describes what the committee
/// finalized. A block's status remains a matter of who happens to hold its certificate.
///
/// *What would discharge it.* Each validator of a revoked epoch publishes a **commitment** on the
/// admin chain: by chain id, the last block in that epoch for which it signed a confirmation vote,
/// plus any superseded confirmation votes with the justifications they cited. Only confirmation
/// votes need committing — validation votes finalize nothing alone, and equivocation among them is
/// already attributable through the justification chains inside certificates — and since each
/// confirmed block names its parent, a committed block accounts transitively for every vote below
/// it.
///
/// The current committee accepts a commitment only if every entry is *grounded*: the committed
/// block's parent carries a quorum, and each listed vote carries the justification it cited, with
/// the justifications mutually consistent. Grounding the committed block, and not only the
/// superseded votes, is what keeps faults attributable — otherwise a quorum of commitments could
/// between them hold a quorum of confirmation votes for one block while each claimed a later vote
/// for a conflicting one, with nothing to check the claim against.
///
/// Settlement is then staged, each boundary an event on the admin chain so that all nodes agree
/// where they stand: revocation freezes signing; a grace period admits further commitments beyond
/// the first quorum; an appeal period admits certificates the commitments do not already imply;
/// after settlement a block is finalized exactly when a commitment or appeal implies it, directly or
/// as an ancestor.
pub trait RevokedEpochRecordBecomesDefinite {}

/// **Goal (Nothing new is finalized in a frozen epoch).** Once an epoch is revoked, the set of
/// blocks finalized in it must not grow.
///
/// *Not currently achieved.* Revocation withdraws [`MaxByzantineWeight`] for that committee, but
/// nothing stops the committee signing, and nothing marks the point after which its signatures
/// should carry no weight. A revoked committee that keeps signing is indistinguishable, to a node
/// that has not yet learned of the revocation, from one that has not.
///
/// *What would discharge it.* Validators stop signing in a revoked epoch except for *late votes* —
/// a confirmation vote for a block the validator has seen finalized by certificate but never voted
/// on itself, adopting that certificate's round and justification. A late vote duplicates a payload
/// that already carries a quorum, so it can neither finalize anything new nor create a conflict the
/// certificate did not already contain; its purpose is to let a commitment cover the chain's real
/// tip. Since a quorum of the committee has honestly stopped signing, no new certificate can form,
/// and the finalized set is fixed from the freeze onward — settlement only makes it public.
///
/// [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight
pub trait FrozenEpochFinalizesNothingNew {}

/// **Goal (A committee stays accountable until its record is settled).** A validator must remain
/// punishable for its conduct in an epoch until that epoch's record is definite — not merely until
/// the epoch is revoked.
///
/// *Not currently achieved.* There is no process for validators on their way out. Revocation ends a
/// committee's authority with nothing tying its obligations to what it signed, and no point at
/// which its stake or contractual duties can be released against a verified record.
///
/// *What would discharge it.* A confirmation vote in the epoch that a validator's commitment
/// neither covers nor lists counts as double-signing, so publishing a commitment is what converts
/// past conduct into a checked claim. Stake is released only at settlement, and only if the
/// commitment was accepted, which makes publishing one economically enforced rather than optional;
/// under a central authority it is a contractual duty instead. Signatures made before the freeze
/// stay backed by the bond throughout, so the three transitions are distinct: signing stops at
/// revocation, free circulation of that epoch's certificates ends with the grace period, and only
/// from settlement is the epoch anchored by the admin chain's verified record rather than by the
/// old signatures. The accountability window must outlast the appeal period.
///
/// This is the converse of [`AccountableSafety`], which convicts on evidence anyone can carry: here
/// the evidence is assembled on the admin chain while the committee is still bonded, precisely so
/// that it survives the committee it describes.
///
/// [`AccountableSafety`]: crate::justification::proof::AccountableSafety
pub trait CommitteeAccountableUntilSettlement {}

/// **Goal (An artifact from a revoked epoch keeps a provable validity path).** A message, blob or
/// event produced under an epoch must remain provably valid to a consumer block after that epoch is
/// revoked.
///
/// *Not currently achieved.* Re-certification is the mechanism, and it has gaps. It runs along
/// prev-hash chains — `ChainWorkerState::select_message_bundles` accepts a lapsed-epoch bundle when
/// a later bundle in the same batch is in a trusted epoch — and along the `previous_message_blocks`
/// and `previous_event_blocks` maps. But those chains only cover counterparties that are addressed
/// *again*: an artifact whose consumer is never addressed again has no link back to a live
/// committee. Checkpointing severs event chains outright, clearing `previous_event_blocks`.
///
/// *What would discharge it.* Appeals. Anyone holding a certificate the commitments do not already
/// imply may register it on the admin chain during the appeal period; since fewer than a third of
/// the still-bonded committee will double-sign, a certificate that verifies is genuine, and the
/// block becomes part of the settled record. Validators appeal automatically for certificates in
/// their own storage that would otherwise be orphaned — which matters most for blocks whose
/// messages were already delivered elsewhere, since delivery implies a quorum verified the
/// certificate, and dropping the block while the deliveries survive downstream would tear a hole in
/// the ledger. Client appeals are a backstop for certificates that never circulated at all.
pub trait OldArtifactsKeepAProvablePath {}
