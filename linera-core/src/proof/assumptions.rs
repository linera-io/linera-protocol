// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The assumptions that progress needs and safety does not.
//!
//! Safety rests only on the assumptions in [`linera_chain::manager::proof::model`], all of which
//! remain in force here. This module adds the ones that mention time, responsiveness, or the
//! existence of somebody willing to drive the protocol. Every one of them can fail without
//! endangering [`CommitAgreement`]; each failure costs progress only.
//!
//! [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement

/// **Assumption (Eventual synchrony).** There is a time GST and a bound Δ, both unknown to the
/// protocol, such that every message sent between correct participants after GST is delivered
/// within Δ, and every message sent before GST is delivered by GST + Δ.
///
/// "Participants" here includes clients: a Linera consensus round is driven by a client
/// ([`ActiveCorrectDriver`]), so the relevant round trips are client-to-validator, not
/// validator-to-validator.
///
/// Before GST nothing is claimed. In particular a network that reorders, delays or drops
/// messages arbitrarily can keep [`ChainTipState::next_block_height`] fixed forever without
/// violating any result in this specification.
///
/// [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
pub trait EventualSynchrony {}

/// **Assumption (Correct validator availability).** After GST, every correct validator accepts
/// requests and answers them within Δ, and its `linera_core::worker::WorkerState` completes each
/// request in bounded local time.
///
/// This is stronger than [`CorrectValidator`], which permits a correct validator to be
/// permanently crashed. It is needed because [`CorrectValidatorsFormQuorum`] only says the
/// correct validators *hold* enough weight; progress needs them to answer.
///
/// The per-chain serialization of [`SerializedChainState`] means "bounded local time" also
/// requires that no single chain's queue grows without bound — a chain saturated by requests can
/// starve its own consensus without any validator being faulty.
///
/// [`CorrectValidator`]: linera_chain::manager::proof::model::CorrectValidator
/// [`SerializedChainState`]: linera_chain::manager::proof::model::SerializedChainState
/// [`CorrectValidatorsFormQuorum`]: linera_chain::data_types::proof::quorum::CorrectValidatorsFormQuorum
pub trait CorrectValidatorAvailability {}

/// **Assumption (An active correct driver).** Some correct owner of the chain runs a
/// [`ChainClient`] that, from some point on, repeatedly and without giving up calls
/// [`ChainClient::process_pending_block`] (or an operation that does, such as
/// [`ChainClient::execute_operations`]) with a block to propose, and holds the signing key for
/// the owner it proposes as.
///
/// **This assumption has no counterpart in most BFT protocols and is the single most important
/// thing to understand about Linera's liveness.** A validator here never proposes a block and
/// never advances a round on its own: it signs a timeout vote only when asked, through
/// `ChainInfoQuery::request_leader_timeout` (see [`TimeoutVoteConditions`]), and there is no code
/// path in `linera_core::worker` that constructs a [`BlockProposal`]. A microchain with no
/// running client is not a chain that is slow; it is a chain that is stopped, by design.
///
/// In [`Round::Validator`] rounds the leaders are drawn from
/// [`ChainManager::fallback_owners`], which [`ChainManager::reset`] populates with the
/// committee's account keys — so the driver of a fallback round is a client run by a validator
/// *operator*, still a client, and still assumed to exist.
///
/// [`ChainClient`]: crate::client::ChainClient
/// [`ChainClient::process_pending_block`]: crate::client::ChainClient::process_pending_block
/// [`ChainClient::execute_operations`]: crate::client::ChainClient::execute_operations
/// [`BlockProposal`]: linera_chain::data_types::BlockProposal
/// [`Round::Validator`]: linera_base::data_types::Round::Validator
/// [`ChainManager::fallback_owners`]: linera_chain::manager::ChainManager::fallback_owners
/// [`ChainManager::reset`]: linera_chain::manager::ChainManager::reset
/// [`TimeoutVoteConditions`]: linera_chain::manager::proof::pacemaker::TimeoutVoteConditions
pub trait ActiveCorrectDriver {}

/// **Assumption (Leader fairness).** The leader schedule selects the correct owner of
/// [`ActiveCorrectDriver`] in infinitely many [`SingleLeader`] rounds.
///
/// The schedule is deterministic and identical at every validator: by [`LeaderEligibility`], the
/// leader of `SingleLeader(n)` is drawn by seeding a `ChaCha8Rng` with
/// `u64::from(n).rotate_left(32) + seed`, where `seed` is the block height, and sampling the
/// stake-weighted `WeightedAliasIndex` over [`ChainOwnership::owners`]. The assumption is therefore about the generator, not about the
/// protocol: it holds if ChaCha8, over the round-indexed seed sequence, selects each positive
/// weight infinitely often. An owner of weight `0` is never selected, and
/// `ChainOwnership::first_leader`, if set, deterministically owns `SingleLeader(0)`.
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`ChainOwnership::owners`]: linera_base::ownership::ChainOwnership::owners
/// [`LeaderEligibility`]: linera_chain::manager::proof::pacemaker::LeaderEligibility
pub trait LeaderFairness {}

/// **Assumption (Round timeouts eventually exceed the round trip).** The round timeout grows
/// without bound over successive rounds, so that eventually a round lasts longer than the time a
/// correct leader needs to complete it after GST.
///
/// This is what [`ChainOwnership::round_timeout`] implements for the rounds that matter: for
/// `SingleLeader(r)` and `Validator(r)` it returns
/// `base_timeout + timeout_increment · r`, which is unbounded in `r` as long as
/// [`TimeoutConfig::timeout_increment`] is non-zero. **With `timeout_increment` set to zero the
/// assumption fails**, and a deployment whose round trip exceeds `base_timeout` can advance
/// rounds forever without any of them lasting long enough to finish — the classic
/// livelock this growth exists to prevent.
///
/// [`ChainOwnership::round_timeout`]: linera_base::ownership::ChainOwnership::round_timeout
/// [`TimeoutConfig::timeout_increment`]: linera_base::ownership::TimeoutConfig::timeout_increment
pub trait RoundTimeoutGrowth {}

/// **Assumption (Clock accuracy).** Correct validators' clocks
/// (`linera_storage::Clock::current_time`) advance in real time and agree within a bound small
/// compared to the round timeout.
///
/// Two places consume this. A validator refuses to sign a timeout vote before its own
/// [`round_timeout`] has passed ([`TimeoutVoteConditions`]), so a quorum's clocks must
/// approximately agree for a timeout certificate to form at all. And a validator rejects a
/// proposal whose timestamp is further in the future than
/// `ChainWorkerConfig::block_time_grace_period` with `WorkerError::InvalidTimestamp`, so a
/// leader whose clock runs fast cannot get its blocks accepted. The client reports the latter
/// back: `Client::submit_block_proposal` warns once a
/// [`validity_threshold`](linera_execution::committee::Committee::validity_threshold) of
/// validators have reported skew.
///
/// [`round_timeout`]: linera_chain::manager::ChainManager::round_timeout
/// [`TimeoutVoteConditions`]: linera_chain::manager::proof::pacemaker::TimeoutVoteConditions
pub trait ClockAccuracy {}

/// **Assumption (Full reachability during synchronization).** After GST, a correct driver's
/// [`ChainClient::synchronize_chain_state`] reaches *every* correct validator, not merely a
/// quorum of them.
///
/// This is the weakest link in the liveness argument, and it is stated separately for that
/// reason. [`LockRecovery`] needs the proposer to learn the highest lock held by *any* correct
/// validator, because a single correct validator holding a lock above the proposer's will reject
/// the proposal ([`UnlockingRequiresHigherCertificate`]) and may be exactly the weight that a
/// quorum was missing.
///
/// **What the implementation actually guarantees is weaker.**
/// `Client::synchronize_chain_from_committee` dispatches
/// `synchronize_chain_state_from` to every validator but aggregates through
/// `communicate_with_quorum`, which stops once a quorum has answered plus a grace period of
/// `quorum_grace_period` (default [`DEFAULT_QUORUM_GRACE_PERIOD`], 0.2) times the time that took;
/// still-pending responses are then dropped. A correct but slow validator holding the highest
/// lock can therefore be missed.
///
/// **Why this is usually not observable.** A validator holds a lock at round `p` only because it
/// received a [`ValidatedBlockCertificate`] at `p`, which the client that assembled it pushes to
/// every validator; so the ordinary way for a lock to exist at one validator and not at a quorum
/// is a partition that GST has since healed, followed by that validator being slow in exactly the
/// synchronization that matters. `ChainClient::process_pending_block` also retries: on a rejection
/// whose consensus-state snapshot is unchanged it performs one explicit fallback
/// `synchronize_chain_state` and retries, and the caller's loop re-enters the whole procedure. The
/// combination makes the assumption hold with probability tending to one over retries rather than
/// deterministically.
///
/// **Residual obligation.** Making this a theorem rather than an assumption requires the
/// synchronization step to wait for all correct validators — or, more cheaply, for a rejection
/// carrying [`ChainError::HasIncompatibleConfirmedVote`] to trigger a targeted pull from the
/// rejecting validator, as `NodeError::WrongRound` already does through
/// `chain_client::Error::LocalNodeLagging`. It does not: the lag-report path in
/// `RemoteNodeUpdater::sync_remote_if_needed` matches only the round and height mismatches. The
/// fallback path is also noted as untested in the source (`TODO(#6453)`).
///
/// [`ChainClient::synchronize_chain_state`]: crate::client::ChainClient::synchronize_chain_state
/// [`DEFAULT_QUORUM_GRACE_PERIOD`]: crate::DEFAULT_QUORUM_GRACE_PERIOD
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`ChainError::HasIncompatibleConfirmedVote`]: linera_chain::ChainError::HasIncompatibleConfirmedVote
/// [`LockRecovery`]: super::progress::LockRecovery
/// [`UnlockingRequiresHigherCertificate`]: linera_chain::manager::proof::voting::UnlockingRequiresHigherCertificate
pub trait FullReachability {}
