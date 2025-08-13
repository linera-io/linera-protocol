// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::ValidatorPublicKey, data_types::NetworkDescription, identifiers::ChainId,
};
use linera_core::node::NodeError;
use linera_persistent as persistent;
use linera_version::VersionInfo;
use thiserror_context::Context;

#[cfg(not(web))]
use crate::benchmark::BenchmarkError;
use crate::util;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum Inner {
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("chain client error: {0}")]
    ChainClient(#[from] linera_core::client::ChainClientError),
    #[error("options error: {0}")]
    Options(#[from] crate::client_options::Error),
    #[error("persistence error: {0}")]
    Persistence(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("view error: {0}")]
    View(#[from] linera_views::ViewError),
    #[error("non-existent chain: {0:?}")]
    NonexistentChain(linera_base::identifiers::ChainId),
    #[error("no keypair found for chain: {0:?}")]
    NonexistentKeypair(linera_base::identifiers::ChainId),
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

thiserror_context::impl_context!(Error(Inner));

util::impl_from_dynamic!(Inner:Persistence, persistent::memory::Error);
#[cfg(feature = "fs")]
util::impl_from_dynamic!(Inner:Persistence, persistent::file::Error);
#[cfg(web)]
util::impl_from_dynamic!(Inner:Persistence, persistent::indexed_db::Error);
