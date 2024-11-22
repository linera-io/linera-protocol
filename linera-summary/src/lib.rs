// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This crate provides the internal tool executable for performance summary generation.

#![deny(clippy::large_futures)]
#![allow(missing_docs)]

pub mod ci_runtime_comparison;
pub mod github;
pub mod performance_summary;
pub mod summary_options;
