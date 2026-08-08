// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Microchain consensus: correctness specification
//!
//! Linera runs one consensus instance per *microchain* and *block height*. This is the complete
//! index to a specification of that protocol — its system model, its assumptions, and proofs of
//! its safety and liveness properties — written as rustdoc next to the code it describes, across
//! [`linera_chain`] and this crate.
//!
//! The two headline results are:
//!
//! * **Safety** — [`CommitAgreement`]: for any chain and height, all valid confirmed block
//!   certificates certify the same block. No synchrony, availability or fairness assumption is
//!   used; only [`MaxByzantineWeight`] and the cryptographic and persistence assumptions.
//! * **Liveness** — [`UnboundedProgress`]: with an active correct client and after GST, every
//!   correct reachable validator's [`ChainTipState::next_block_height`] grows without bound.
//!
//! # Reading order
//!
//! The proofs live next to the code they are about, but they are written to be read in this
//! order, and each statement cites only statements above it.
//!
//! | | section | where |
//! |---|---|---|
//! | 1 | System model | [`linera_chain::manager::proof::model`] |
//! | 2 | Fault and network assumptions | [`linera_chain::manager::proof::model`], then [`proof::assumptions`] |
//! | 3 | Protocol objects and definitions | [`linera_chain::data_types::proof::objects`] |
//! | 4 | Quorum properties | [`linera_chain::data_types::proof::quorum`] |
//! | 5 | Voting rules | [`linera_chain::manager::proof::voting`] |
//! | 6 | Locking and certificate invariants | [`linera_chain::manager::proof::rounds`], then [`linera_chain::manager::proof::locking`] |
//! | 7 | Commit rule | [`linera_chain::manager::proof::commit`] |
//! | 8 | Safety proof | [`linera_chain::manager::proof::safety`] |
//! | 9 | Pacemaker and view changes | [`linera_chain::manager::proof::pacemaker`] |
//! | 10 | Progress lemmas | [`proof::progress`] |
//! | 11 | Liveness proof | [`proof::liveness`] |
//!
//! Sections 1–9 are also indexed, on their own, in [`linera_chain::spec`], which explains the
//! conventions in full. The essentials:
//!
//! # How to read a statement
//!
//! Every claim is a public marker trait with no members, no implementors and no runtime
//! footprint. Its **name is its identity**; its doc comment carries the statement and, unless it
//! is a definition or an assumption, a proof. **Supertraits are proof dependencies**: a statement
//! lists as supertraits exactly the earlier statements whose truth its proof consumes.
//!
//! ```text
//! pub trait RoundProgress:
//!     EventuallyCorrectLeader + LockRecovery + ProposalAccepted + …
//! //  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//! //  the statements this proof consumes
//! ```
//!
//! That shape buys five checks with no bespoke tooling: unique identifiers (Rust name
//! resolution), existence of every cited statement and every cited Rust item
//! (`rustdoc::broken_intra_doc_links`, denied in CI), acyclicity of the dependency graph
//! (`rustc`, `E0391`), and a rendered, clickable dependency list on each statement's page. There
//! is deliberately no numbering: names are stable under insertion, and a stale citation is a
//! build failure rather than a silently wrong cross reference.
//!
//! # The safety/liveness split
//!
//! The two arguments are deliberately disjoint in their assumptions, and the module layout
//! reflects that:
//!
//! ```text
//!  MaxByzantineWeight, UnforgeableSignatures,           EventualSynchrony, ClockAccuracy,
//!  DurablePersistence, SerializedChainState,            CorrectValidatorAvailability,
//!  EpochAgreement, DeterministicExecution               ActiveCorrectDriver, LeaderFairness,
//!         |                                             RoundTimeoutGrowth, FullReachability
//!         v                                                     |
//!   quorum properties                                           |
//!         |                                                     |
//!         v                                                     v
//!   voting rules  -->  rounds  -->  locking  -->  commit  -->  progress
//!                                       |                        |
//!                                       v                        v
//!                                    SAFETY                   LIVENESS
//!                              CommitAgreement            UnboundedProgress
//! ```
//!
//! Everything in the right-hand column can fail — the network can partition forever, every client
//! can go away, clocks can drift — without endangering [`CommitAgreement`]. Nothing in the
//! left-hand column can fail without endangering it.
//!
//! # Known gaps
//!
//! The specification records, rather than papers over, the places where the implementation does
//! not quite discharge what a proof wants. In rough order of significance:
//!
//! * [`FullReachability`] — the lock-recovery step wants the proposer to reach *every* correct
//!   validator, while `synchronize_chain_state` guarantees only a quorum plus a grace period.
//!   Affects liveness only.
//! * [`FastRetryPreservesBlock`] — closing the gap between "same proposal" and "same block" for a
//!   fast-round retry relies on [`DeterministicExecution`] rather than on a runtime check at the
//!   retry. Affects safety, and is the one step that reaches outside consensus into execution.
//! * [`ProposalGate`] — several safety-critical guards live at the call site in
//!   `chain_worker::state` rather than inside the [`ChainManager`] methods they protect; in
//!   particular [`create_final_vote`] would sign twice in a round if invoked directly.
//! * [`SafetyStateRecovery`] — the correspondence between a
//!   [`ManagerSafetySnapshot`] and the instance it is restored into rests on a height check at
//!   the single call site, not on anything the type enforces.
//! * [`VoteConstructionSites`] — an exhaustive-search argument over the five signing sites, which
//!   a sixth would invalidate.
//!
//! [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement
//! [`UnboundedProgress`]: crate::proof::liveness::UnboundedProgress
//! [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
//! [`MaxByzantineWeight`]: linera_chain::manager::proof::model::MaxByzantineWeight
//! [`DeterministicExecution`]: linera_chain::manager::proof::model::DeterministicExecution
//! [`FullReachability`]: crate::proof::assumptions::FullReachability
//! [`FastRetryPreservesBlock`]: linera_chain::manager::proof::safety::FastRetryPreservesBlock
//! [`ProposalGate`]: linera_chain::manager::proof::voting::ProposalGate
//! [`VoteConstructionSites`]: linera_chain::manager::proof::voting::VoteConstructionSites
//! [`SafetyStateRecovery`]: linera_chain::manager::proof::locking::SafetyStateRecovery
//! [`ChainManager`]: linera_chain::manager::ChainManager
//! [`create_final_vote`]: linera_chain::manager::ChainManager::create_final_vote
//! [`ManagerSafetySnapshot`]: linera_chain::manager::ManagerSafetySnapshot
//! [`proof::assumptions`]: crate::proof::assumptions
//! [`proof::progress`]: crate::proof::progress
//! [`proof::liveness`]: crate::proof::liveness
