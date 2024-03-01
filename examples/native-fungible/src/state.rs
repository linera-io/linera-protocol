// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};
use thiserror::Error;

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
// In the service we also want a tickerSymbol query, which is not derived from a struct member.
// This attribute requires having a ComplexObject implementation that adds such fields.
// The implementation with tickerSymbol is in service.rs. Since a ComplexObject impl is required,
// there is also an empty one in contract.rs.
#[graphql(complex)]
#[view(context = "ViewStorageContext")]
pub struct NativeFungibleToken {
    // TODO(#968): We should support stateless applications/empty user views
    pub _dummy: RegisterView<u8>,
}

/// Attempts to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
