// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use thiserror_context::Context;

use crate::{persistent, util};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum Inner {
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("chain client error: {0}")]
    ChainClient(#[from] linera_core::client::ChainClientError),
    #[error("options error: {0}")]
    Options(#[from] crate::client_options::Error),
    #[error("persistence error: {0}")]
    Persistence(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("view error: {0}")]
    View(#[from] linera_views::views::ViewError),
    #[error("non-existent chain: {0:?}")]
    NonexistentChain(linera_base::identifiers::ChainId),
    #[error("no keypair found for chain: {0:?}")]
    NonexistentKeypair(linera_base::identifiers::ChainId),
    #[error("error on the local node: {0}")]
    LocalNode(#[from] linera_core::local_node::LocalNodeError),
}

thiserror_context::impl_context!(Error(Inner));

util::impl_from_dynamic!(Inner:Persistence, persistent::memory::Error);
#[cfg(feature = "fs")]
util::impl_from_dynamic!(Inner:Persistence, persistent::file::Error);
#[cfg(with_local_storage)]
util::impl_from_dynamic!(Inner:Persistence, persistent::local_storage::Error);
