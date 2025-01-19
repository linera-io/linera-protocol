// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from [`std::sync`].

use std::sync::Arc;

impl_for_wrapper_type!(Arc);
