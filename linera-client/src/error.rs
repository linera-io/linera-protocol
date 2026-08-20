// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::ValidatorPublicKey, data_types::NetworkDescription, identifiers::ChainId,
};
use linera_core::node::NodeError;
use linera_version::VersionInfo;

#[cfg(not(web))]
use crate::benchmark::BenchmarkError;
use crate::util;

/// The kinds of error that the client can return.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A value could not be BCS-serialized or -deserialized.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),
    /// An operation on a chain failed.
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    /// An operation performed through the chain client failed.
    #[error("chain client error: {0}")]
    ChainClient(#[from] linera_core::client::chain_client::Error),
    /// The client options were invalid.
    #[error("options error: {0}")]
    Options(#[from] crate::client_options::Error),
    /// An operation on the wallet backend failed.
    #[error("wallet error: {0}")]
    Wallet(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// An operation on a view failed.
    #[error("view error: {0}")]
    View(#[from] linera_views::ViewError),
    /// An operation on the local node failed.
    #[error("error on the local node: {0}")]
    LocalNode(#[from] linera_core::LocalNodeError),
    /// An operation on a remote validator failed.
    #[error("remote node operation failed: {0}")]
    RemoteNode(#[from] linera_core::node::NodeError),
    /// An arithmetic operation overflowed.
    #[error("arithmetic error: {0}")]
    Arithmetic(#[from] linera_base::data_types::ArithmeticError),
    /// The chain is not owned by the expected owner.
    #[error("incorrect chain ownership")]
    ChainOwnership,
    /// A benchmark run failed.
    #[cfg(not(web))]
    #[error("Benchmark error: {0}")]
    Benchmark(#[from] BenchmarkError),
    /// The new chain could not be assigned to the wallet.
    #[error("failed to assign the new chain to the wallet: {0}")]
    AssignChain(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The chain's ownership could not be changed.
    #[error("failed to change chain ownership: {0}")]
    ChangeOwnership(#[source] Box<linera_core::client::chain_client::Error>),
    /// The module could not be published.
    #[error("failed to publish the module: {0}")]
    PublishModule(#[source] Box<linera_core::client::chain_client::Error>),
    /// The data blob could not be published.
    #[error("failed to publish the data blob: {0}")]
    PublishDataBlob(#[source] Box<linera_core::client::chain_client::Error>),
    /// The data blob could not be read back after publishing.
    #[error("failed to read back the data blob: {0}")]
    VerifyDataBlob(#[source] Box<linera_core::client::chain_client::Error>),
    /// The formats data blob could not be published.
    #[error("failed to publish the formats data blob: {0}")]
    PublishFormatsBlob(#[source] Box<linera_core::client::chain_client::Error>),
    /// The module could not be published together with its format registration.
    #[error("failed to publish the module and register its formats: {0}")]
    PublishModuleAndRegisterFormats(#[source] Box<linera_core::client::chain_client::Error>),
    /// The messages that create the new chains could not be delivered.
    #[error("failed to deliver the outgoing messages that create the new chains: {0}")]
    DeliverNewChainMessages(#[source] Box<linera_core::client::chain_client::Error>),
    /// A validator is running an incompatible version.
    #[error("Validator version {remote} is not compatible with local version {local}.")]
    UnexpectedVersionInfo {
        /// The validator's version.
        remote: Box<VersionInfo>,
        /// Our own version.
        local: Box<VersionInfo>,
    },
    /// A validator's version could not be retrieved.
    #[error("Failed to get version information for validator {address}: {error}")]
    UnavailableVersionInfo {
        /// The validator's address.
        address: String,
        /// The underlying failure.
        error: Box<NodeError>,
    },
    /// A validator disagrees with us about the network.
    #[error("Validator's network description {remote:?} does not match our own: {local:?}.")]
    UnexpectedNetworkDescription {
        /// The validator's network description.
        remote: Box<NetworkDescription>,
        /// Our own network description.
        local: Box<NetworkDescription>,
    },
    /// A validator's network description could not be retrieved.
    #[error("Failed to get network description for validator {address}: {error}")]
    UnavailableNetworkDescription {
        /// The validator's address.
        address: String,
        /// The underlying failure.
        error: Box<NodeError>,
    },
    /// A validator's signature did not verify.
    #[error("Signature for public key {public_key} is invalid.")]
    InvalidSignature {
        /// The public key the signature was checked against.
        public_key: ValidatorPublicKey,
    },
    /// A validator's information about a chain could not be retrieved.
    #[error("Failed to get chain info for validator {address} and chain {chain_id}: {error}")]
    UnavailableChainInfo {
        /// The validator's address.
        address: String,
        /// The chain that was queried.
        chain_id: ChainId,
        /// The underlying failure.
        error: Box<NodeError>,
    },
}

impl Error {
    /// Wraps an error from the wallet backend.
    pub fn wallet(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Wallet(Box::new(error) as _)
    }
}

util::impl_from_infallible!(Error);
