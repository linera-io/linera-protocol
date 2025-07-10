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

#[derive(thiserror::Error, Debug)]
pub(crate) enum ExporterError {
    #[error("received an invalid notification.")]
    BadNotification(BadNotificationKind),

    #[error("unable to load the exporter state: {0}")]
    StateError(ViewError),

    #[error("Missing certificate: {0}")]
    ReadCertificateError(CryptoHash),

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

#[derive(Debug)]
pub(crate) enum BadNotificationKind {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanonicalBlock {
    pub blobs: Box<[BlobId]>,
    pub block_hash: CryptoHash,
}

impl CanonicalBlock {
    pub(crate) fn new(hash: CryptoHash, blobs: &[BlobId]) -> CanonicalBlock {
        CanonicalBlock {
            block_hash: hash,
            blobs: blobs.to_vec().into_boxed_slice(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) struct BlockId {
    pub hash: CryptoHash,
    pub chain_id: ChainId,
    pub height: BlockHeight,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LiteBlockId {
    pub hash: CryptoHash,
    pub height: BlockHeight,
}

impl BlockId {
    pub(crate) fn new(chain_id: ChainId, hash: CryptoHash, height: BlockHeight) -> BlockId {
        BlockId {
            hash,
            chain_id,
            height,
        }
    }

    pub(crate) fn from_incoming_bundle(incoming_bundle: &IncomingBundle) -> Self {
        Self {
            hash: incoming_bundle.bundle.certificate_hash,
            chain_id: incoming_bundle.origin,
            height: incoming_bundle.bundle.height,
        }
    }

    pub(crate) fn from_confirmed_block(block: &ConfirmedBlock) -> BlockId {
        BlockId::new(block.chain_id(), block.inner().hash(), block.height())
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

#[derive(Clone)]
pub(crate) struct ExporterCancellationSignal {
    token: CancellationToken,
}

impl ExporterCancellationSignal {
    pub(crate) fn new(token: CancellationToken) -> Self {
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

pub(crate) fn get_address(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}
