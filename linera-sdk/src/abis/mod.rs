// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common ABIs that may have multiple implementations.

/// The ABI of a controller application that manages worker and controller commands.
pub mod controller;
pub mod evm;
pub mod fungible;
pub mod wrapped_fungible;
