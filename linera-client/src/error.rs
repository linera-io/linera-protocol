// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use thiserror_context::Context;

#[cfg(feature = "benchmark")]
use crate::benchmark::BenchmarkError;
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
    #[error("remote node operation failed: {0}")]
    RemoteNode(#[from] linera_core::node::NodeError),
    #[error("chain info response missing latest committee")]
    ChainInfoResponseMissingCommittee,
    #[error("arithmetic error: {0}")]
    Arithmetic(#[from] linera_base::data_types::ArithmeticError),
    #[error("invalid open message")]
    InvalidOpenMessage(Option<linera_execution::Message>),
    #[error("incorrect chain ownership")]
    ChainOwnership,
    #[cfg(feature = "benchmark")]
    #[error("Benchmark error: {0}")]
    Benchmark(#[from] BenchmarkError),
}

thiserror_context::impl_context!(Error(Inner));

util::impl_from_dynamic!(Inner:Persistence, persistent::memory::Error);
#[cfg(feature = "fs")]
util::impl_from_dynamic!(Inner:Persistence, persistent::file::Error);
#[cfg(with_indexed_db)]
util::impl_from_dynamic!(Inner:Persistence, persistent::indexed_db::Error);
