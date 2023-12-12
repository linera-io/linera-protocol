// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::OnceLock;
use tokio::sync::Mutex;

/// A static lock to prevent integration tests from running in parallel.
pub static INTEGRATION_TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
