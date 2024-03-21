// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};
use thiserror::Error;

/// The application state.
#[derive(RootView)]
#[view(context = "ViewStorageContext")]
pub struct NativeFungibleToken {
    // TODO(#968): We should support stateless applications/empty user views
    pub _dummy: RegisterView<u8>,
}

/// Attempts to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
