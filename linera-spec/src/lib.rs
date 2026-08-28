// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # The Linera protocol: correctness specification
//!
//! Linera is a multi-chain protocol: state is partitioned into *microchains*, each running its own
//! consensus instance per block height. No chain can read another's state; they communicate by
//! explicit message passing and through shared immutable stores — today, published blobs and events.
//! This crate is the entry point to a correctness specification of that protocol — its system
//! model, its assumptions, and proofs of the properties it is meant to have.
//!
//! The specification is written subsystem by subsystem; [Coverage](#coverage) says what is
//! established today and what is not yet constrained by any statement here.
//!
//! **This crate contains no code.** The statements live next to the implementation they describe —
//! today across [`linera_chain`] and [`linera_core`] — and this crate exists only so that a single
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
//! # Headline results
//!
//! Three results anchor the consensus core, each concerning one microchain's sequence of blocks.
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
//! | Leaders, timeouts and round advancement | [`linera_chain::manager::proof::timeouts`] |
//! | Progress lemmas | [`linera_core::proof::progress`] |
//! | Liveness proof | [`linera_core::proof::liveness`] |
//! | Availability, crash recovery and catch-up | [`linera_core::proof::availability`] |
//! | Client notifications | [`linera_core::proof::notifications`] |
//! | Storage validity | [`linera_core::proof::storage`] |
//! | What a checkpoint preserves | [`linera_chain::proof::checkpoints`] |
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
//!  DurablePersistence, SequentialChainState,            CorrectValidatorAvailability,
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
//! Three places where the implementation does not deliver what a proof wants. Each needs a change
//! to the code, or a weaker result, to close; none is closed by rereading.
//!
//! * [`FullReachability`] — the lock-recovery step wants the proposer to reach *every* correct
//!   validator, while `synchronize_chain_state` guarantees only a quorum plus a grace period.
//!   Affects liveness only.
//! * [`MissingDependenciesAreRecoverable`] — a block that consumes a message or reads an event
//!   depends on data originating on a *third* chain. Blobs, ancestors and chain state are
//!   self-suppliable, so a lagging validator is simply handed them; these two classes are not. If
//!   the proposer does not follow the sending or publishing chain either, the validator waits on
//!   its own catch-up of that chain, which no assumption bounds — so
//!   [`ValidationQuorumForms`]'s `2Δ` step does not apply to such blocks. Affects liveness only.
//! * [`AccountabilityScope`] — incorrect block execution is not attributable, and its effects are
//!   not confined to one chain: a wrong `messages` or `events` field is consumed by other chains
//!   whose resulting blocks are themselves properly certified. The protections are
//!   [`CertifiedBlockWasExecuted`] and [`IncomingBundlesAreSelfDerived`], and unlike the
//!   accountability results both need [`MaxByzantineWeight`]. Tracked in
//!   [issue #6675](https://github.com/linera-io/linera-protocol/issues/6675).
//!
//! # Arguments that rest on the current shape of the code
//!
//! These are not gaps. Each is sound as it stands, but holds because of how the code is arranged
//! today rather than by construction, so a change of the right kind invalidates the *argument*
//! without any statement becoming visibly wrong. They are listed so that change can be recognized
//! when it happens.
//!
//! | statement | what would invalidate it |
//! |---|---|
//! | [`VoteConstructionSites`] | a sixth signing site; the argument is an exhaustive search over the five that exist |
//! | [`ProposalGate`] | a new caller of the [`ChainManager`] methods it protects — the guards live at the call site in `chain_worker::state`, so [`create_final_vote`] would sign twice in a round if invoked directly |
//! | [`SafetyStateRecovery`] | a second restore site; the correspondence between a [`ManagerSafetySnapshot`] and the instance it is restored into rests on a height check at the one call site, not on anything the type enforces |
//!
//! The last two describe a public method whose precondition is met by convention rather than by
//! the type. That is a latent hazard as well as a maintenance obligation — the same shape as
//! [issue #6686](https://github.com/linera-io/linera-protocol/issues/6686), which tracks it for
//! `apply_confirmed_block`.
//!
//! # Coverage
//!
//! Established today, in four areas:
//!
//! * **Agreement** on the *sequence of blocks* of a single microchain, with its accountability
//!   converse and its progress counterpart — the three headline results above.
//! * **Availability** — what a certified block guarantees to nodes that were absent when it was
//!   certified, and what a crash costs.
//! * **Conservation across a checkpoint** — that events, messages, blobs and execution state behave
//!   as they would have without one ([`linera_chain::proof::checkpoints`]).
//! * **The grounding of committee knowledge** — that no committee certifies its own introduction,
//!   which is what makes induction on the epoch legitimate ([`CommitteeKnowledgeIsWellFounded`]).
//!
//! Client notifications are specified too ([`linera_core::proof::notifications`]), but as a channel
//! the model treats as lossy rather than as a guarantee anything rests on.
//!
//! Not yet covered, in the sense that no statement here constrains them. Where consensus does say
//! something adjacent, it is named.
//!
//! * **State-transition correctness** — that executing the agreed blocks yields the right state.
//!   [`DeterministicExecution`] is assumed rather than proved, and *termination* of execution is
//!   not stated at all. What is guaranteed is that a certified block was executed by some correct
//!   validator ([`CertifiedBlockWasExecuted`]), that a voter matched every consumed bundle against
//!   its own inbox ([`IncomingBundlesAreSelfDerived`]), and that the inputs execution needs can be
//!   supplied to a validator lacking them ([`MissingDependenciesAreRecoverable`]), and that its
//!   outputs — published blobs, events and the certificate — reach storage before the block counts
//!   as processed ([`BlockOutputsArePersisted`]).
//!
//!   Safety reaches into execution in exactly one place: [`FastRetryPreservesBlock`] closes the
//!   step from "same proposal" to "same block" with [`DeterministicExecution`] rather than a
//!   runtime check at the retry. Everywhere else the two are kept apart.
//! * **Cross-chain messaging** as a subsystem — delivery in particular: nothing states that an
//!   outbox is ever drained, so no bundle is guaranteed to arrive. What *is* stated is that an
//!   inbox holds only bundles its origin really sent ([`InboxHoldsOnlySentBundles`]), that no two
//!   blocks consume the same bundle ([`BundleConsumedAtMostOnce`]), and that each chain's own
//!   block sequence is unique ([`UniqueChain`]).
//! * **Committee reconfiguration** — partly covered now. [`CommitteeKnowledgeIsWellFounded`] fixes
//!   where a node's knowledge of a committee comes from, and [`MaxByzantineWeight`] is assumed of
//!   every epoch whose committee has not been revoked, so the fault bound accumulates as epochs are
//!   created. What remains assumed is that the committee *for* an epoch is agreed, which
//!   [`EpochAgreement`] records; and revocation is not usable in practice, so no committee is ever
//!   retired and that accumulation never stops.
//! * **Chain ownership and lifecycle** — who may propose at a height, and how that changes;
//!   [`ConsensusInstance`] records what is assumed about it.
//! * **Resource control and fees** — metering, declared block limits, and fee conservation.
//! * **Event streams** as a subsystem — append-only-ness and the publisher-side guarantees behind
//!   a cross-chain `OracleResponse::Event` read. What is stated concerns only the checkpoint
//!   boundary: [`EventFloorTracksCheckpoints`] says which indices stay readable across one.
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
//! [`CommitteeKnowledgeIsWellFounded`]: linera_chain::proof::epochs::CommitteeKnowledgeIsWellFounded
//! [`EventFloorTracksCheckpoints`]: linera_chain::proof::checkpoints::EventFloorTracksCheckpoints
//! [`ConsensusInstance`]: linera_chain::manager::proof::model::ConsensusInstance
//! [`CertifiedBlockWasExecuted`]: linera_chain::manager::proof::commit::CertifiedBlockWasExecuted
//! [`IncomingBundlesAreSelfDerived`]: linera_chain::manager::proof::commit::IncomingBundlesAreSelfDerived
//! [`FullReachability`]: linera_core::proof::assumptions::FullReachability
//! [`MissingDependenciesAreRecoverable`]: linera_core::proof::availability::MissingDependenciesAreRecoverable
//! [`BlockOutputsArePersisted`]: linera_core::proof::availability::BlockOutputsArePersisted
//! [`InboxHoldsOnlySentBundles`]: linera_core::proof::availability::InboxHoldsOnlySentBundles
//! [`BundleConsumedAtMostOnce`]: linera_core::proof::availability::BundleConsumedAtMostOnce
//! [`ValidationQuorumForms`]: linera_core::proof::progress::ValidationQuorumForms
//! [`FastRetryPreservesBlock`]: linera_chain::manager::proof::safety::FastRetryPreservesBlock
//! [`ProposalGate`]: linera_chain::manager::proof::voting::ProposalGate
//! [`VoteConstructionSites`]: linera_chain::manager::proof::voting::VoteConstructionSites
//! [`SafetyStateRecovery`]: linera_chain::manager::proof::locking::SafetyStateRecovery
//! [`ChainManager`]: linera_chain::manager::ChainManager
//! [`create_final_vote`]: linera_chain::manager::ChainManager::create_final_vote
//! [`ManagerSafetySnapshot`]: linera_chain::manager::ManagerSafetySnapshot
//! [`validity_threshold`]: linera_execution::committee::Committee::validity_threshold

#![deny(missing_docs)]
