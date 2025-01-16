// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    data_types::BlobContent,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{BlockProposal, LiteVote},
    types::{ConfirmedBlock, ConfirmedBlockCertificate},
};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    node::NodeError,
};
use linera_version::VersionInfo;
use serde::{Deserialize, Serialize};

use crate::{
    HandleConfirmedCertificateRequest, HandleLiteCertRequest, HandleTimeoutCertificateRequest,
    HandleValidatedCertificateRequest,
};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub enum RpcMessage {
    // Inbound
    BlockProposal(Box<BlockProposal>),
    TimeoutCertificate(Box<HandleTimeoutCertificateRequest>),
    ValidatedCertificate(Box<HandleValidatedCertificateRequest>),
    ConfirmedCertificate(Box<HandleConfirmedCertificateRequest>),
    LiteCertificate(Box<HandleLiteCertRequest<'static>>),
    ChainInfoQuery(Box<ChainInfoQuery>),
    UploadBlob(Box<BlobContent>),
    DownloadBlob(Box<BlobId>),
    DownloadPendingBlob(Box<(ChainId, BlobId)>),
    HandlePendingBlob(Box<(ChainId, BlobContent)>),
    DownloadConfirmedBlock(Box<CryptoHash>),
    DownloadCertificates(Vec<CryptoHash>),
    BlobLastUsedBy(Box<BlobId>),
    MissingBlobIds(Vec<BlobId>),
    VersionInfoQuery,
    GenesisConfigHashQuery,

    // Outbound
    Vote(Box<LiteVote>),
    ChainInfoResponse(Box<ChainInfoResponse>),
    Error(Box<NodeError>),
    VersionInfoResponse(Box<VersionInfo>),
    GenesisConfigHashResponse(Box<CryptoHash>),
    UploadBlobResponse(Box<BlobId>),
    DownloadBlobResponse(Box<BlobContent>),
    DownloadPendingBlobResponse(Box<BlobContent>),
    DownloadConfirmedBlockResponse(Box<ConfirmedBlock>),
    DownloadCertificatesResponse(Vec<ConfirmedBlockCertificate>),
    BlobLastUsedByResponse(Box<CryptoHash>),
    MissingBlobIdsResponse(Vec<BlobId>),

    // Internal to a validator
    CrossChainRequest(Box<CrossChainRequest>),
}

impl RpcMessage {
    /// Obtains the [`ChainId`] of the chain targeted by this message, if there is one.
    ///
    /// Only inbound messages have target chains.
    pub fn target_chain_id(&self) -> Option<ChainId> {
        use RpcMessage::*;

        let chain_id = match self {
            BlockProposal(proposal) => proposal.content.block.chain_id,
            LiteCertificate(request) => request.certificate.value.chain_id,
            TimeoutCertificate(request) => request.certificate.inner().chain_id(),
            ValidatedCertificate(request) => request.certificate.inner().chain_id(),
            ConfirmedCertificate(request) => request.certificate.inner().chain_id(),
            ChainInfoQuery(query) => query.chain_id,
            CrossChainRequest(request) => request.target_chain_id(),
            DownloadPendingBlob(request) => request.0,
            HandlePendingBlob(request) => request.0,
            Vote(_)
            | Error(_)
            | ChainInfoResponse(_)
            | VersionInfoQuery
            | VersionInfoResponse(_)
            | GenesisConfigHashQuery
            | GenesisConfigHashResponse(_)
            | UploadBlob(_)
            | UploadBlobResponse(_)
            | DownloadBlob(_)
            | DownloadBlobResponse(_)
            | DownloadPendingBlobResponse(_)
            | DownloadConfirmedBlock(_)
            | DownloadConfirmedBlockResponse(_)
            | DownloadCertificates(_)
            | BlobLastUsedBy(_)
            | BlobLastUsedByResponse(_)
            | MissingBlobIds(_)
            | MissingBlobIdsResponse(_)
            | DownloadCertificatesResponse(_) => {
                return None;
            }
        };

        Some(chain_id)
    }

    /// Whether this message is "local" i.e. will be executed locally on the proxy
    /// or if it'll be proxied to the server.
    pub fn is_local_message(&self) -> bool {
        use RpcMessage::*;

        match self {
            VersionInfoQuery
            | GenesisConfigHashQuery
            | UploadBlob(_)
            | DownloadBlob(_)
            | DownloadConfirmedBlock(_)
            | BlobLastUsedBy(_)
            | MissingBlobIds(_)
            | DownloadCertificates(_) => true,
            BlockProposal(_)
            | LiteCertificate(_)
            | TimeoutCertificate(_)
            | ValidatedCertificate(_)
            | ConfirmedCertificate(_)
            | ChainInfoQuery(_)
            | CrossChainRequest(_)
            | Vote(_)
            | Error(_)
            | ChainInfoResponse(_)
            | VersionInfoResponse(_)
            | GenesisConfigHashResponse(_)
            | UploadBlobResponse(_)
            | DownloadPendingBlob(_)
            | DownloadPendingBlobResponse(_)
            | HandlePendingBlob(_)
            | DownloadBlobResponse(_)
            | DownloadConfirmedBlockResponse(_)
            | BlobLastUsedByResponse(_)
            | MissingBlobIdsResponse(_)
            | DownloadCertificatesResponse(_) => false,
        }
    }
}

impl TryFrom<RpcMessage> for ChainInfoResponse {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::ChainInfoResponse(response) => Ok(*response),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for VersionInfo {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::VersionInfoResponse(version_info) => Ok(*version_info),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for BlobContent {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::DownloadBlobResponse(blob)
            | RpcMessage::DownloadPendingBlobResponse(blob) => Ok(*blob),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for ConfirmedBlock {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::DownloadConfirmedBlockResponse(certificate) => Ok(*certificate),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for Vec<ConfirmedBlockCertificate> {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::DownloadCertificatesResponse(certificates) => Ok(certificates),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for CryptoHash {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::BlobLastUsedByResponse(hash) => Ok(*hash),
            RpcMessage::GenesisConfigHashResponse(hash) => Ok(*hash),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for Vec<BlobId> {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::MissingBlobIdsResponse(blob_ids) => Ok(blob_ids),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for BlobId {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        match message {
            RpcMessage::UploadBlobResponse(blob_id) => Ok(*blob_id),
            RpcMessage::Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl From<NodeError> for RpcMessage {
    fn from(error: NodeError) -> Self {
        RpcMessage::Error(Box::new(error))
    }
}
