// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, net::SocketAddr};

use bincode::ErrorKind;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{data_types::IncomingBundle, types::ConfirmedBlock};
use linera_rpc::grpc::{GrpcError, GrpcProtoConversionError};
use linera_sdk::views::ViewError;
use serde::{Deserialize, Serialize};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::Status;

/// Errors that can occur while running the block exporter.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum ExporterError {
    #[error("received an invalid notification.")]
    BadNotification(BadNotificationKind),

    #[error("unable to load the exporter state: {0}")]
    StateError(ViewError),

    #[error("Missing certificate: {0}")]
    ReadCertificateError(CryptoHash),

    #[error("Missing blob: {0}")]
    ReadBlobError(BlobId),

    #[error("generic storage error: {0}")]
    ViewError(#[from] ViewError),

    #[error("block not processed by the block processor yet")]
    UnprocessedBlock,

    #[error("chain not yet processed by the block exporter")]
    UnprocessedChain,

    #[error("chain should be initialized from block zero")]
    BadInitialization,

    #[error("trying to re-initialize the chain: {0}")]
    ChainAlreadyExists(ChainId),

    #[error("unable to establish an underlying stream")]
    SynchronizationFailed(Box<Status>),

    #[error(transparent)]
    GrpcError(#[from] GrpcError),

    #[error("generic error: {0}")]
    GenericError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// The reason a notification was rejected as invalid.
#[derive(Debug)]
#[allow(missing_docs)]
pub enum BadNotificationKind {
    InvalidChainId {
        #[debug(skip_if = Option::is_none)]
        inner: Option<GrpcProtoConversionError>,
    },

    InvalidReason {
        #[debug(skip_if = Option::is_none)]
        inner: Option<Box<ErrorKind>>,
    },
}

impl From<BadNotificationKind> for ExporterError {
    fn from(value: BadNotificationKind) -> Self {
        ExporterError::BadNotification(value)
    }
}

/// A block together with the blobs it depends on, in canonical order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalBlock {
    /// The blobs required by the block.
    pub blobs: Box<[BlobId]>,
    /// The hash of the block.
    pub block_hash: CryptoHash,
}

impl CanonicalBlock {
    /// Creates a new canonical block from a block hash and its blobs.
    pub fn new(hash: CryptoHash, blobs: &[BlobId]) -> CanonicalBlock {
        CanonicalBlock {
            block_hash: hash,
            blobs: blobs.to_vec().into_boxed_slice(),
        }
    }
}

/// A fully-qualified block identifier holding its hash, chain and height.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct BlockId {
    /// The hash of the block.
    pub hash: CryptoHash,
    /// The chain the block belongs to.
    pub chain_id: ChainId,
    /// The height of the block within its chain.
    pub height: BlockHeight,
}

/// A lightweight block identifier holding only its hash and height.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiteBlockId {
    /// The hash of the block.
    pub hash: CryptoHash,
    /// The height of the block within its chain.
    pub height: BlockHeight,
}

impl BlockId {
    /// Creates a new block identifier from its chain, hash and height.
    pub fn new(chain_id: ChainId, hash: CryptoHash, height: BlockHeight) -> BlockId {
        BlockId {
            hash,
            chain_id,
            height,
        }
    }

    /// Creates a block identifier from a confirmed block.
    pub fn from_confirmed_block(block: &ConfirmedBlock) -> BlockId {
        BlockId::new(block.chain_id(), block.inner().hash(), block.height())
    }

    /// Creates a block identifier from an incoming message bundle.
    pub fn from_incoming_bundle(bundle: &IncomingBundle) -> BlockId {
        BlockId::new(
            bundle.origin,
            bundle.bundle.certificate_hash,
            bundle.bundle.height,
        )
    }
}

impl LiteBlockId {
    pub(crate) fn new(height: BlockHeight, hash: CryptoHash) -> LiteBlockId {
        LiteBlockId { hash, height }
    }
}

impl From<BlockId> for LiteBlockId {
    fn from(value: BlockId) -> Self {
        LiteBlockId::new(value.height, value.hash)
    }
}

/// A cancellation signal that resolves once the underlying token is cancelled.
#[derive(Clone)]
pub struct ExporterCancellationSignal {
    token: CancellationToken,
}

impl ExporterCancellationSignal {
    /// Creates a new cancellation signal from the given token.
    pub fn new(token: CancellationToken) -> Self {
        Self { token }
    }
}

impl IntoFuture for ExporterCancellationSignal {
    type Output = ();
    type IntoFuture = WaitForCancellationFutureOwned;

    fn into_future(self) -> Self::IntoFuture {
        self.token.cancelled_owned()
    }
}

/// Returns the socket address listening on all interfaces for the given port.
pub fn get_address(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}
