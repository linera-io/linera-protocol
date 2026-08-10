// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! System model and the assumptions the safety argument rests on.
//!
//! The definitions here fix what a consensus instance is and what it means for two blocks to
//! conflict; the assumptions are the things the implementation does *not* prove and that a
//! deployment must supply. Everything in this module is a leaf of the dependency graph.
//!
//! The assumptions needed only for progress — synchrony, availability, leader fairness, an
//! active driver — are stated separately in `linera_core::proof::assumptions`, because their
//! evidence lives in that crate. Safety depends on none of them.

/// **Definition (Consensus instance).** The protocol decides one block at a time, per chain.
/// A *consensus instance* is a pair `(chain, height)`, and its state is one [`ChainManager`],
/// reachable as [`ChainStateView::manager`], whose height is
/// [`ChainTipState::next_block_height`].
///
/// [`ChainManager::reset`] destroys an instance and creates the next one: it calls `clear()` on
/// every view, re-derives the leader distributions from the new ownership, and sets
/// [`ChainManager::current_round`] to [`ChainOwnership::first_round`]. It is called from exactly
/// two places in `linera_chain::chain`: `initialize_if_needed`, when the chain becomes active at
/// height `0`, and `reset_chain_manager`, immediately after a confirmed block at height `h` has
/// been executed, for height `h + 1`.
///
/// Consequently **every invariant in [`crate::manager::proof::locking`] is scoped to a single
/// instance**: it holds from the moment the instance is created until it is reset, and says
/// nothing across a reset. That is sound because a reset happens only once the height's block is
/// committed, so a later instance decides a different height. Restoring state *across* a reset
/// is the one exception, handled by [`SafetyStateRecovery`].
///
/// [`ChainManager`]: crate::manager::ChainManager
/// [`ChainManager::reset`]: crate::manager::ChainManager::reset
/// [`ChainManager::current_round`]: crate::manager::ChainManager::current_round
/// [`ChainStateView::manager`]: crate::ChainStateView::manager
/// [`ChainTipState::next_block_height`]: crate::ChainTipState::next_block_height
/// [`ChainOwnership::first_round`]: linera_base::ownership::ChainOwnership::first_round
/// [`SafetyStateRecovery`]: crate::manager::proof::locking::SafetyStateRecovery
pub trait ConsensusInstance {}

/// **Definition (Round order).** Rounds are [`Round`] values, totally ordered by the derived
/// [`Ord`] on the enum, which orders first by variant and then by the contained `u32`:
///
/// ```text
/// Round::Fast  <  Round::MultiLeader(0) < Round::MultiLeader(1) < …
///              <  Round::SingleLeader(0) < Round::SingleLeader(1) < …
///              <  Round::Validator(0)  < Round::Validator(1)  < …
/// ```
///
/// In particular [`Round::Fast`] is the global minimum, which several arguments use directly:
/// a guard of the form `x.round() < Round::Fast` is unsatisfiable.
///
/// The successor function is [`ChainOwnership::next_round`], which is *not* the successor of
/// this order — it skips the multi-leader rounds a chain is not configured for and saturates
/// into [`Round::Validator`]. It is monotone, which is all the pacemaker results need.
///
/// [`Round`]: linera_base::data_types::Round
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`Round::Validator`]: linera_base::data_types::Round::Validator
/// [`ChainOwnership::next_round`]: linera_base::ownership::ChainOwnership::next_round
pub trait RoundOrder {}

/// **Definition (Correct validator).** A validator is *correct* in an execution if every
/// signature it produces was produced by an unmodified build of this code, driven through the
/// public entry points of `linera_core::worker::WorkerState`, with a private key no other party
/// holds. A validator that is not correct is *faulty*, and may sign anything at any time,
/// including contradictory statements.
///
/// This is what licenses the arguments in [`crate::manager::proof::voting`]: for a correct
/// validator, "it signed a validation vote for `B` in round `r`" implies its
/// [`ChainManager`](crate::manager::ChainManager) state satisfied the guards on the path that
/// produces such a vote, because that path is the only one that can produce it
/// ([`VoteConstructionSites`]).
///
/// Note this is a statement about *signing*, not about liveness: a correct validator may be
/// slow, unreachable, or permanently crashed without becoming faulty.
///
/// [`VoteConstructionSites`]: crate::manager::proof::voting::VoteConstructionSites
pub trait CorrectValidator {}

/// **Definition (Conflicting blocks).** Two [`Block`]s *conflict* when they have the same
/// [`chain_id`] and [`height`] but different hashes. Certificates certify [`ConfirmedBlock`] and
/// [`ValidatedBlock`] values, each of which wraps a [`Block`] and hashes to that block's hash,
/// so "certificates for conflicting blocks" is well defined.
///
/// Note that a [`Block`] is a [`ProposedBlock`] *together with* its
/// [`BlockExecutionOutcome`]. Two blocks with the same proposal but different outcomes therefore
/// conflict. This is deliberate: they lead to different chain states, so agreement must exclude
/// them, and the exclusion is discharged by [`DeterministicExecution`].
///
/// The struct carries a third field, [`Block::owner_authorization`], which is deliberately
/// *outside* the block's identity: it is excluded from [`Block::hash`], and `Block`'s `PartialEq`
/// and `Hash` impls exclude it too, so the same block re-proposed in a later round under a
/// different but equally valid authorization is the same block. Because equality and hashing
/// agree, "distinct [`Block`]s have distinct hashes" in [`UnforgeableSignatures`] remains exact,
/// and no statement below needs to mention the field.
///
/// Ancestry needs no separate definition here: [`ChainTipState::verify_block_chaining`] requires
/// a proposal's height to equal the tip's next height and its `previous_block_hash` to equal the
/// tip's block hash, so the committed blocks of a chain form a hash-linked list, one per height.
///
/// [`Block`]: crate::block::Block
/// [`chain_id`]: crate::block::BlockHeader::chain_id
/// [`height`]: crate::block::BlockHeader::height
/// [`ConfirmedBlock`]: crate::block::ConfirmedBlock
/// [`ValidatedBlock`]: crate::block::ValidatedBlock
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`ChainTipState::verify_block_chaining`]: crate::ChainTipState::verify_block_chaining
/// [`Block::owner_authorization`]: crate::block::Block::owner_authorization
/// [`Block::hash`]: crate::block::Block::hash
/// [`UnforgeableSignatures`]: self::UnforgeableSignatures
pub trait ConflictingBlocks {}

/// **Assumption (Maximum Byzantine weight).** In the committee governing a chain's epoch, the
/// total [`Committee::weight`] of faulty validators is strictly less than
/// [`Committee::validity_threshold`], i.e. at most `f⁺ − 1` where `f⁺ = ⌈N/3⌉`.
///
/// Equivalently, faulty weight is strictly below one third of the total. This is the only fault
/// bound assumed anywhere in the specification.
///
/// [`Committee::weight`]: linera_execution::committee::Committee::weight
/// [`Committee::validity_threshold`]: linera_execution::committee::Committee::validity_threshold
pub trait MaxByzantineWeight {}

/// **Assumption (Epoch agreement).** All correct validators evaluate a given consensus instance
/// against the same [`Committee`].
///
/// This is what makes [`Intersection`] applicable to two certificates for the same height: two
/// quorums of *different* committees need not intersect at all. It is close to enforced rather
/// than assumed. A proposal is rejected unless `check_block_epoch` finds
/// [`ProposedBlock::epoch`] equal to the chain's current epoch, and
/// `ChainWorkerState::process_validated_block` and `process_confirmed_block` apply the same
/// check to the certified block before verifying signatures against that epoch's committee. The
/// chain's current epoch at height `h` is a function of the committed blocks below `h`, which
/// [`UniqueChain`] shows is unique — so the assumption is discharged for height `h` by the
/// agreement result at heights below `h`, and the induction in [`UniqueChain`] is what makes
/// that non-circular.
///
/// What remains genuinely assumed is that the committee for an epoch is itself agreed, which
/// holds because it is published by a committed block on the admin chain.
///
/// [`Committee`]: linera_execution::committee::Committee
/// [`ProposedBlock::epoch`]: crate::data_types::ProposedBlock::epoch
/// [`Intersection`]: crate::data_types::proof::quorum::Intersection
/// [`UniqueChain`]: crate::manager::proof::safety::UniqueChain
pub trait EpochAgreement {}

/// **Assumption (Cryptographic soundness).** [`ValidatorSignature`] is existentially unforgeable:
/// no party without a validator's secret key produces a signature that
/// [`ValidatorSignature::check`] accepts for that validator's public key. [`CryptoHash`] is
/// collision resistant, so distinct values — in particular distinct [`Block`]s and distinct
/// [`VoteValue`]s — have distinct hashes.
///
/// Collision resistance is what lets the specification move between "the certificate's
/// `value_hash`" and "the block", and between block equality and hash equality.
///
/// [`ValidatorSignature`]: linera_base::crypto::ValidatorSignature
/// [`ValidatorSignature::check`]: linera_base::crypto::ValidatorSignature::check
/// [`CryptoHash`]: linera_base::crypto::CryptoHash
/// [`Block`]: crate::block::Block
/// [`VoteValue`]: crate::data_types::VoteValue
pub trait UnforgeableSignatures {}

/// **Assumption (Durable persistence).** A correct validator persists its
/// [`ChainManager`](crate::manager::ChainManager) state before releasing a vote to the network,
/// and that state survives a crash.
///
/// The implementation is structured to make this hold: every path in
/// `linera_core::chain_worker::state` that mutates the manager calls `self.save()` before
/// returning the chain info response that carries the vote — `try_handle_block_proposal` after
/// `create_vote`, `process_validated_block` after `create_final_vote`,
/// `vote_for_leader_timeout` after `create_timeout_vote`, and `vote_for_fallback` after
/// `vote_fallback`. The response projection is [`ChainManagerInfo`].
///
/// Without this, a crash could lose the record of a vote and let the validator vote again,
/// breaking [`OneValidationVotePerRound`] and [`OneConfirmationVotePerRound`], which are the
/// only places where the assumption is consumed. A validator that violates it is faulty in the
/// sense of [`CorrectValidator`], and is counted against [`MaxByzantineWeight`].
///
/// [`OneValidationVotePerRound`]: crate::manager::proof::locking::OneValidationVotePerRound
/// [`OneConfirmationVotePerRound`]: crate::manager::proof::locking::OneConfirmationVotePerRound
/// [`ChainManagerInfo`]: crate::manager::ChainManagerInfo
pub trait DurablePersistence {}

/// **Assumption (Serialized instance state).** The transitions of one consensus instance are
/// mutually exclusive and each runs to completion: no two of them interleave their reads and
/// writes of the same [`ChainManager`](crate::manager::ChainManager).
///
/// This is enforced, not hoped for: `linera_core::worker::WorkerState` routes every mutating
/// request for a chain through `chain_write`, which holds a per-chain write lock for the whole
/// transition, and the manager is `!Sync`-by-construction behind that guard. The specification
/// relies on it whenever it reasons about "the state immediately before" a vote — for instance
/// in [`UnlockingJustification`], where the guard evaluated by
/// [`ChainManager::check_proposed_block`] must still describe the state when
/// [`ChainManager::create_vote`] runs a few statements later.
///
/// [`UnlockingJustification`]: crate::manager::proof::safety::UnlockingJustification
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
pub trait SerializedChainState {}

/// **Assumption (Deterministic execution).** For a fixed chain state at a height, a fixed
/// [`ProposedBlock`], a fixed set of published blobs and a fixed multi-leader round argument,
/// block execution returns a unique [`BlockExecutionOutcome`].
///
/// This is what makes [`ConflictingBlocks`] a property of the *proposal* in the cases where the
/// protocol re-executes rather than re-uses a certified outcome. Two correct validators handed
/// the same proposal at the same height therefore compute the same [`Block`].
///
/// **Residual obligation.** The round argument is
/// [`Round::multi_leader`](linera_base::data_types::Round::multi_leader), so an outcome may in
/// principle depend on the round — the round is readable as an oracle
/// ([`OracleResponse::Round`]). This matters in exactly one place,
/// [`FastRetryPreservesBlock`], where a block confirmed in [`Round::Fast`] is re-executed in a
/// later round. The gap is closed there by the fast round's own restriction
/// (`WorkerError::FastBlockUsingOracles` rejects a fast block that recorded any oracle
/// response), plus determinism: an execution that never queried the round oracle cannot diverge
/// on the round's value. Note that the restriction is checked only when the *proposal's* round
/// is fast, so the retry itself is not re-checked; the argument leans on determinism of the
/// execution engine rather than on a runtime check at the retry.
///
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`Block`]: crate::block::Block
/// [`OracleResponse::Round`]: linera_base::data_types::OracleResponse::Round
/// [`FastRetryPreservesBlock`]: crate::manager::proof::safety::FastRetryPreservesBlock
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
pub trait DeterministicExecution {}
