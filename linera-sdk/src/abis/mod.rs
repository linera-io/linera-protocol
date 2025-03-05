// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common ABIs that may have multiple implementations.

#[cfg(with_revm)]
pub mod evm;
pub mod fungible;
