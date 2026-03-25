// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::ValidatorPublicKey, data_types::NetworkDescription, identifiers::ChainId,
};
use linera_core::node::NodeError;
use linera_version::VersionInfo;
use thiserror_context::Context;

#[cfg(not(web))]
use crate::benchmark::BenchmarkError;
use crate::util;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum Inner {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("chain client error: {0}")]
    ChainClient(#[from] linera_core::client::chain_client::Error),
    #[error("options error: {0}")]
    Options(#[from] crate::client_options::Error),
    #[error("wallet error: {0}")]
    Wallet(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("view error: {0}")]
    View(#[from] linera_views::ViewError),
    #[error("error on the local node: {0}")]
    LocalNode(#[from] linera_core::LocalNodeError),
    #[error("remote node operation failed: {0}")]
    RemoteNode(#[from] linera_core::node::NodeError),
    #[error("arithmetic error: {0}")]
    Arithmetic(#[from] linera_base::data_types::ArithmeticError),
    #[error("incorrect chain ownership")]
    ChainOwnership,
    #[cfg(not(web))]
    #[error("Benchmark error: {0}")]
    Benchmark(#[from] BenchmarkError),
    #[error("Validator version {remote} is not compatible with local version {local}.")]
    UnexpectedVersionInfo {
        remote: Box<VersionInfo>,
        local: Box<VersionInfo>,
    },
    #[error("Failed to get version information for validator {address}: {error}")]
    UnavailableVersionInfo {
        address: String,
        error: Box<NodeError>,
    },
    #[error("Validator's network description {remote:?} does not match our own: {local:?}.")]
    UnexpectedNetworkDescription {
        remote: Box<NetworkDescription>,
        local: Box<NetworkDescription>,
    },
    #[error("Failed to get network description for validator {address}: {error}")]
    UnavailableNetworkDescription {
        address: String,
        error: Box<NodeError>,
    },
    #[error("Signature for public key {public_key} is invalid.")]
    InvalidSignature { public_key: ValidatorPublicKey },
    #[error("Failed to get chain info for validator {address} and chain {chain_id}: {error}")]
    UnavailableChainInfo {
        address: String,
        chain_id: ChainId,
        error: Box<NodeError>,
    },
}

impl Inner {
    pub fn wallet(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Wallet(Box::new(error) as _)
    }
}

thiserror_context::impl_context!(Error(Inner));

impl Error {
    pub(crate) fn wallet(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Inner::wallet(error).into()
    }
}

util::impl_from_infallible!(Error);
