// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Microchain consensus: correctness specification
//!
//! Linera runs one consensus instance per *microchain* and *block height*. This module is the
//! entry point to a specification of that consensus protocol — its system model, its assumptions,
//! and proofs of its safety and liveness properties — written as rustdoc next to the code it
//! describes.
//!
//! This crate holds the model, the voting and locking arguments, the commit rule, the safety
//! theorem and the pacemaker. Progress and liveness depend on the client driver and so live in
//! `linera_core::proof`, which cites back into here; that crate's `spec` module carries the same
//! index with those two sections filled in.
//!
//! # Reading order
//!
//! The proofs live next to the code they are about, but they are written to be read in this
//! order, and each statement cites only statements above it.
//!
//! | | section | where |
//! |---|---|---|
//! | 1 | System model | [`manager::proof::model`] |
//! | 2 | Fault and network assumptions | [`manager::proof::model`], and `linera_core::proof::assumptions` for the ones only liveness needs |
//! | 3 | Protocol objects and definitions | [`data_types::proof::objects`] |
//! | 4 | Quorum properties | [`data_types::proof::quorum`] |
//! | 5 | Voting rules | [`manager::proof::voting`] |
//! | 6 | Locking and certificate invariants | [`manager::proof::rounds`], then [`manager::proof::locking`] |
//! | 7 | Commit rule | [`manager::proof::commit`] |
//! | 8 | Safety proof | [`manager::proof::safety`] |
//! | 9 | Pacemaker and view changes | [`manager::proof::pacemaker`] |
//! | 10 | Progress lemmas | `linera_core::proof::progress` |
//! | 11 | Liveness proof | `linera_core::proof::liveness` |
//!
//! The two headline results are [`CommitAgreement`] — at most one block is ever committed per
//! chain and height — and `linera_core::proof::liveness::UnboundedProgress`.
//!
//! # How to read a statement
//!
//! Every claim is a public marker trait with no members, no implementors and no runtime
//! footprint. Its **name is its identity**; its doc comment carries the statement and, unless it
//! is a definition or an assumption, a proof.
//!
//! ```text
//! pub trait UniqueValidatedBlockPerRound:
//!     OneValidationVotePerRound + CorrectValidatorInIntersection + …
//! //  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//! //  the statements this proof consumes
//! ```
//!
//! **Supertraits are proof dependencies.** A statement lists as supertraits exactly those earlier
//! statements whose truth its proof consumes. Definitions and forward pointers are cited in prose
//! instead, so that the supertrait graph stays a proof-dependency graph rather than a vocabulary
//! graph.
//!
//! This shape was chosen for what the compiler and rustdoc then check for free, with no bespoke
//! tooling:
//!
//! | property | checked by |
//! |---|---|
//! | statement identifiers are unique | Rust name resolution |
//! | every cited statement exists | `rustdoc::broken_intra_doc_links`, denied in CI |
//! | every cited Rust item exists | the same lint — a renamed field or method breaks the doc build |
//! | the dependency graph is acyclic | `rustc` (`E0391`, cycle in supertraits) |
//! | dependencies are visible to a reader | rustdoc renders each supertrait as a link |
//!
//! There is deliberately **no numbering**. Names are stable under insertion and are what
//! citations resolve against, so a statement can be added, split or moved without touching
//! unrelated text; a stale citation is a build failure rather than a silently wrong cross
//! reference.
//!
//! What is *not* mechanically checked is the thing that matters most: that the prose proof is
//! correct, and that the code it cites still does what the proof says. Several statements carry
//! an explicit **"where this is fragile"** or **"residual obligation"** paragraph naming the
//! change that would invalidate them — see [`ProposalGate`], [`VoteConstructionSites`],
//! [`SafetyStateRecovery`] and [`FastRetryPreservesBlock`].
//!
//! # Scope and non-goals
//!
//! The specification covers agreement on the *sequence of blocks* of a single microchain. It does
//! not cover:
//!
//! * **State-transition correctness** — that executing the agreed blocks yields the right state.
//!   [`DeterministicExecution`] is assumed, not proved.
//! * **Cross-chain messaging** — inboxes, outboxes and delivery are outside consensus; the
//!   relevant guarantee is that each chain's own block sequence is unique, which
//!   [`UniqueChain`] provides.
//! * **Committee reconfiguration** — epoch changes are agreed *by* this protocol on the admin
//!   chain; [`EpochAgreement`] records what is assumed about them.
//! * **Fault attribution** — a converse property, proved in [`crate::justification`] and stated
//!   here as [`Accountability`].
//!
//! [`manager::proof::model`]: crate::manager::proof::model
//! [`manager::proof::voting`]: crate::manager::proof::voting
//! [`manager::proof::rounds`]: crate::manager::proof::rounds
//! [`manager::proof::locking`]: crate::manager::proof::locking
//! [`manager::proof::commit`]: crate::manager::proof::commit
//! [`manager::proof::safety`]: crate::manager::proof::safety
//! [`manager::proof::pacemaker`]: crate::manager::proof::pacemaker
//! [`data_types::proof::objects`]: crate::data_types::proof::objects
//! [`data_types::proof::quorum`]: crate::data_types::proof::quorum
//! [`CommitAgreement`]: crate::manager::proof::safety::CommitAgreement
//! [`UniqueChain`]: crate::manager::proof::safety::UniqueChain
//! [`Accountability`]: crate::manager::proof::safety::Accountability
//! [`FastRetryPreservesBlock`]: crate::manager::proof::safety::FastRetryPreservesBlock
//! [`ProposalGate`]: crate::manager::proof::voting::ProposalGate
//! [`VoteConstructionSites`]: crate::manager::proof::voting::VoteConstructionSites
//! [`SafetyStateRecovery`]: crate::manager::proof::locking::SafetyStateRecovery
//! [`DeterministicExecution`]: crate::manager::proof::model::DeterministicExecution
//! [`EpochAgreement`]: crate::manager::proof::model::EpochAgreement
