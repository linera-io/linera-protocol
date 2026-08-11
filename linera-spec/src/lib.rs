// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Microchain consensus: correctness specification
//!
//! Linera runs one consensus instance per *microchain* and *block height*. This crate is the
//! entry point to a specification of that protocol — its system model, its assumptions, and
//! proofs of its safety, accountability and liveness properties.
//!
//! **This crate contains no code.** The statements live next to the implementation they describe,
//! spread across [`linera_chain`] and [`linera_core`]; this crate exists only so that a single
//! index can link to all of them. Neither of those crates can do that on its own —
//! `linera-core` depends on `linera-chain`, so the chain crate cannot name the progress and
//! liveness results, and an index living there would have to describe half the specification in
//! prose. Sitting above both, this crate can cite every statement by path, so every cross
//! reference below is checked by the documentation build.
//!
//! ```bash
//! cargo doc -p linera-spec --open
//! ```
//!
//! # The three headline results
//!
//! * **Safety** — [`CommitAgreement`]: for any chain and height, all valid confirmed block
//!   certificates certify the same block. No synchrony, availability or fairness assumption is
//!   used; only [`MaxByzantineWeight`] and the cryptographic and persistence assumptions.
//! * **Accountability** — [`AccountableSafety`]: if agreement *does* fail, the two conflicting
//!   certificates alone convict validators of weight at least [`validity_threshold`] — more than
//!   [`MaxByzantineWeight`] permits — and no correct validator is ever convictable. It assumes
//!   strictly less than safety does, since it must hold precisely where safety does not.
//! * **Liveness** — [`UnboundedProgress`]: with an active correct client and after GST, every
//!   correct reachable validator's [`ChainTipState::next_block_height`] grows without bound.
//!
//! # Reading order
//!
//! The proofs live next to the code they are about, but they are written to be read in this
//! order, and each statement cites only statements above it.
//!
//! | section | where |
//! |---|---|
//! | System model | [`linera_chain::manager::proof::model`] |
//! | Fault and network assumptions | [`linera_chain::manager::proof::model`], then [`linera_core::proof::assumptions`] |
//! | Protocol objects and definitions | [`linera_chain::data_types::proof::objects`] |
//! | Quorum properties | [`linera_chain::data_types::proof::quorum`] |
//! | Voting rules | [`linera_chain::manager::proof::voting`] |
//! | Locking and certificate invariants | [`linera_chain::manager::proof::rounds`], then [`linera_chain::manager::proof::locking`] |
//! | Commit rule | [`linera_chain::manager::proof::commit`] |
//! | Safety proof | [`linera_chain::manager::proof::safety`] |
//! | Accountability | [`linera_chain::justification::proof`] |
//! | Pacemaker and view changes | [`linera_chain::manager::proof::pacemaker`] |
//! | Progress lemmas | [`linera_core::proof::progress`] |
//! | Liveness proof | [`linera_core::proof::liveness`] |
//! | Crash and deployment obligations | [`linera_core::proof::durability`] |
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
//! build failure rather than a silently wrong cross reference. The same goes for the sections
//! above — they are ordered, not numbered, so adding or splitting one renames nothing and
//! invalidates no reference.
//!
//! Each statement opens with one of six labels, all of which mean something on their own:
//!
//! | label | carries a proof | meaning |
//! |---|---|---|
//! | **Definition** | no | fixes a term, and pins it to the Rust it denotes |
//! | **Assumption** | no | something the implementation does not establish and a deployment must supply |
//! | **Invariant** | yes | holds in every reachable state, proved by induction over transitions |
//! | **Lemma** | yes | a proved statement |
//! | **Theorem** | yes | one of the results the specification exists to establish |
//! | **Remark** / **Caveat** | no | an observation or a limitation, asserting nothing new |
//!
//! None of them is *relational*: nothing is labelled by what it follows from. These pages are
//! reached from anywhere by link, so there is no preceding result for a label to refer back to —
//! and the supertrait list already names what a statement follows from, exactly and checkably.
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
//!                                       |
//!                                       v
//!                                 ACCOUNTABILITY
//!                                AccountableSafety
//! ```
//!
//! Everything in the right-hand column can fail — the network can partition forever, every client
//! can go away, clocks can drift — without endangering [`CommitAgreement`]. Nothing in the
//! left-hand column can fail without endangering it. Accountability hangs below safety rather
//! than above it: it consumes *fewer* assumptions than [`CommitAgreement`] does, because its job
//! is to hold in the one regime where safety does not.
//!
//! # Known gaps
//!
//! The specification records, rather than papers over, the places where the implementation does
//! not quite discharge what a proof wants. In rough order of significance:
//!
//! * [`FullReachability`] — the lock-recovery step wants the proposer to reach *every* correct
//!   validator, while `synchronize_chain_state` guarantees only a quorum plus a grace period.
//!   Affects liveness only.
//! * [`AccountabilityScope`] — incorrect block execution is not attributable, and its effects are
//!   not confined to one chain: a wrong `messages` or `events` field is consumed by other chains
//!   whose resulting blocks are themselves properly certified. The protections are
//!   [`CertifiedBlockWasExecuted`] and [`IncomingBundlesAreSelfDerived`], and unlike the
//!   accountability results both need [`MaxByzantineWeight`]. Tracked in
//!   [issue #6675](https://github.com/linera-io/linera-protocol/issues/6675).
//! * [`FastRetryPreservesBlock`] — closing the gap between "same proposal" and "same block" for a
//!   fast-round retry relies on [`DeterministicExecution`] rather than on a runtime check at the
//!   retry. Affects safety, and is the one step that reaches outside consensus into execution.
//! * [`ProposalGate`] — several safety-critical guards live at the call site in
//!   `chain_worker::state` rather than inside the [`ChainManager`] methods they protect; in
//!   particular [`create_final_vote`] would sign twice in a round if invoked directly.
//! * [`SafetyStateRecovery`] — the correspondence between a [`ManagerSafetySnapshot`] and the
//!   instance it is restored into rests on a height check at the single call site, not on
//!   anything the type enforces.
//! * [`VoteConstructionSites`] — an exhaustive-search argument over the five signing sites, which
//!   a sixth would invalidate.
//!
//! # Scope and non-goals
//!
//! The specification covers agreement on the *sequence of blocks* of a single microchain. It does
//! not cover:
//!
//! * **State-transition correctness** — that executing the agreed blocks yields the right state.
//!   [`DeterministicExecution`] is assumed, not proved. What the protocol does guarantee is
//!   [`CertifiedBlockWasExecuted`] and [`IncomingBundlesAreSelfDerived`].
//! * **Cross-chain messaging** as a subsystem — inboxes, outboxes and delivery are outside
//!   consensus; what consensus provides is that each chain's own block sequence is unique
//!   ([`UniqueChain`]).
//! * **Committee reconfiguration** — epoch changes are agreed *by* this protocol on the admin
//!   chain; [`EpochAgreement`] records what is assumed about them.
//!
//! [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement
//! [`UniqueChain`]: linera_chain::manager::proof::safety::UniqueChain
//! [`AccountableSafety`]: linera_chain::justification::proof::AccountableSafety
//! [`AccountabilityScope`]: linera_chain::justification::proof::AccountabilityScope
//! [`UnboundedProgress`]: linera_core::proof::liveness::UnboundedProgress
//! [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
//! [`MaxByzantineWeight`]: linera_chain::manager::proof::model::MaxByzantineWeight
//! [`DeterministicExecution`]: linera_chain::manager::proof::model::DeterministicExecution
//! [`EpochAgreement`]: linera_chain::manager::proof::model::EpochAgreement
//! [`CertifiedBlockWasExecuted`]: linera_chain::manager::proof::commit::CertifiedBlockWasExecuted
//! [`IncomingBundlesAreSelfDerived`]: linera_chain::manager::proof::commit::IncomingBundlesAreSelfDerived
//! [`FullReachability`]: linera_core::proof::assumptions::FullReachability
//! [`FastRetryPreservesBlock`]: linera_chain::manager::proof::safety::FastRetryPreservesBlock
//! [`ProposalGate`]: linera_chain::manager::proof::voting::ProposalGate
//! [`VoteConstructionSites`]: linera_chain::manager::proof::voting::VoteConstructionSites
//! [`SafetyStateRecovery`]: linera_chain::manager::proof::locking::SafetyStateRecovery
//! [`ChainManager`]: linera_chain::manager::ChainManager
//! [`create_final_vote`]: linera_chain::manager::ChainManager::create_final_vote
//! [`ManagerSafetySnapshot`]: linera_chain::manager::ManagerSafetySnapshot
//! [`validity_threshold`]: linera_execution::committee::Committee::validity_threshold

#![deny(missing_docs)]
