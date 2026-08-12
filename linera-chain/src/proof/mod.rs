// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a checkpoint preserves.
//!
//! A checkpoint lets a node adopt a chain's state at some height without replaying the blocks
//! below it. Every statement here is a *conservation* claim — "this behaves as it would have
//! without the checkpoint" — stated relative to whatever that behaviour is, so none of them needs
//! a specification of execution or messaging to be meaningful.
//!
//! | module | covers |
//! |---|---|
//! | [`checkpoints`] | event streams across a checkpoint boundary |

pub mod checkpoints;
