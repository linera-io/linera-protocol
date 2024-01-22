// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RootView, ViewStorageContext, RegisterView};
use thiserror::Error;

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
// In the service we also want a metadata query, which is not derived from a struct member.
// This attribute requires having a ComplexObject implementation that adds such fields.
// The implementation with metadata is in service.rs. Since a ComplexObject impl is required,
// there is also an empty one in contract.rs.
#[graphql(complex)]
#[view(context = "ViewStorageContext")]
pub struct NativeFungibleToken {
    // TODO(#968): We should support stateless Applications/empty user views
    pub _dummy: RegisterView<u8>,
}

/// Attempts to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
