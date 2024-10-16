// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    data_types::BlobContent,
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{BlockProposal, Certificate, CertificateValue, LiteVote};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    node::NodeError,
};
use linera_version::VersionInfo;
use serde::{Deserialize, Serialize};

use crate::{HandleCertificateRequest, HandleLiteCertRequest};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub enum RpcMessage {
    // Inbound
    BlockProposal(Box<BlockProposal>),
    Certificate(Box<HandleCertificateRequest>),
    LiteCertificate(Box<HandleLiteCertRequest<'static>>),
    ChainInfoQuery(Box<ChainInfoQuery>),
    DownloadBlobContent(Box<BlobId>),
    DownloadCertificateValue(Box<CryptoHash>),
    DownloadCertificate(Box<CryptoHash>),
    DownloadCertificates(Box<Vec<CryptoHash>>),
    BlobLastUsedBy(Box<BlobId>),
    VersionInfoQuery,
    GenesisConfigHashQuery,

    // Outbound
    Vote(Box<LiteVote>),
    ChainInfoResponse(Box<ChainInfoResponse>),
    Error(Box<NodeError>),
    VersionInfoResponse(Box<VersionInfo>),
    GenesisConfigHashResponse(Box<CryptoHash>),
    DownloadBlobContentResponse(Box<BlobContent>),
    DownloadCertificateValueResponse(Box<CertificateValue>),
    DownloadCertificateResponse(Box<Certificate>),
    DownloadCertificatesResponse(Box<Vec<Certificate>>),
    BlobLastUsedByResponse(Box<CryptoHash>),

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
            Certificate(request) => request.certificate.value().chain_id(),
            ChainInfoQuery(query) => query.chain_id,
            CrossChainRequest(request) => request.target_chain_id(),
            Vote(_)
            | Error(_)
            | ChainInfoResponse(_)
            | VersionInfoQuery
            | VersionInfoResponse(_)
            | GenesisConfigHashQuery
            | GenesisConfigHashResponse(_)
            | DownloadBlobContent(_)
            | DownloadBlobContentResponse(_)
            | DownloadCertificateValue(_)
            | DownloadCertificateValueResponse(_)
            | DownloadCertificate(_)
            | DownloadCertificates(_)
            | BlobLastUsedBy(_)
            | BlobLastUsedByResponse(_)
            | DownloadCertificateResponse(_)
            | DownloadCertificatesResponse(_) => {
                return None;
            }
        };

        Some(chain_id)
    }

    /// Wether this messsage is "local" i.e. will be executed locally on the proxy
    /// or if it'll be proxied to the server.
    pub fn is_local_message(&self) -> bool {
        use RpcMessage::*;

        match self {
            VersionInfoQuery
            | GenesisConfigHashQuery
            | DownloadBlobContent(_)
            | DownloadCertificateValue(_)
            | BlobLastUsedBy(_)
            | DownloadCertificate(_)
            | DownloadCertificates(_) => true,
            BlockProposal(_)
            | LiteCertificate(_)
            | Certificate(_)
            | ChainInfoQuery(_)
            | CrossChainRequest(_)
            | Vote(_)
            | Error(_)
            | ChainInfoResponse(_)
            | VersionInfoResponse(_)
            | GenesisConfigHashResponse(_)
            | DownloadBlobContentResponse(_)
            | DownloadCertificateValueResponse(_)
            | BlobLastUsedByResponse(_)
            | DownloadCertificateResponse(_)
            | DownloadCertificatesResponse(_) => false,
        }
    }
}

impl TryFrom<RpcMessage> for ChainInfoResponse {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            ChainInfoResponse(response) => Ok(*response),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for VersionInfo {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            VersionInfoResponse(version_info) => Ok(*version_info),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for BlobContent {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            DownloadBlobContentResponse(blob) => Ok(*blob),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for CertificateValue {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            DownloadCertificateValueResponse(certificate) => Ok(*certificate),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for Certificate {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            DownloadCertificateResponse(certificate) => Ok(*certificate),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for Vec<Certificate> {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            DownloadCertificatesResponse(certificates) => Ok(*certificates),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl TryFrom<RpcMessage> for CryptoHash {
    type Error = NodeError;
    fn try_from(message: RpcMessage) -> Result<Self, Self::Error> {
        use RpcMessage::*;
        match message {
            BlobLastUsedByResponse(hash) => Ok(*hash),
            GenesisConfigHashResponse(hash) => Ok(*hash),
            Error(error) => Err(*error),
            _ => Err(NodeError::UnexpectedMessage),
        }
    }
}

impl From<BlockProposal> for RpcMessage {
    fn from(block_proposal: BlockProposal) -> Self {
        RpcMessage::BlockProposal(Box::new(block_proposal))
    }
}

impl From<HandleLiteCertRequest<'static>> for RpcMessage {
    fn from(request: HandleLiteCertRequest<'static>) -> Self {
        RpcMessage::LiteCertificate(Box::new(request))
    }
}

impl From<HandleCertificateRequest> for RpcMessage {
    fn from(request: HandleCertificateRequest) -> Self {
        RpcMessage::Certificate(Box::new(request))
    }
}

impl From<Vec<CryptoHash>> for RpcMessage {
    fn from(hashes: Vec<CryptoHash>) -> Self {
        RpcMessage::DownloadCertificates(Box::new(hashes))
    }
}

impl From<ChainInfoQuery> for RpcMessage {
    fn from(chain_info_query: ChainInfoQuery) -> Self {
        RpcMessage::ChainInfoQuery(Box::new(chain_info_query))
    }
}

impl From<LiteVote> for RpcMessage {
    fn from(vote: LiteVote) -> Self {
        RpcMessage::Vote(Box::new(vote))
    }
}

impl From<ChainInfoResponse> for RpcMessage {
    fn from(chain_info_response: ChainInfoResponse) -> Self {
        RpcMessage::ChainInfoResponse(Box::new(chain_info_response))
    }
}

impl From<NodeError> for RpcMessage {
    fn from(error: NodeError) -> Self {
        RpcMessage::Error(Box::new(error))
    }
}

impl From<CrossChainRequest> for RpcMessage {
    fn from(cross_chain_request: CrossChainRequest) -> Self {
        RpcMessage::CrossChainRequest(Box::new(cross_chain_request))
    }
}

impl From<VersionInfo> for RpcMessage {
    fn from(version_info: VersionInfo) -> Self {
        RpcMessage::VersionInfoResponse(Box::new(version_info))
    }
}

impl From<BlobContent> for RpcMessage {
    fn from(blob: BlobContent) -> Self {
        RpcMessage::DownloadBlobContentResponse(Box::new(blob))
    }
}

impl From<CertificateValue> for RpcMessage {
    fn from(certificate: CertificateValue) -> Self {
        RpcMessage::DownloadCertificateValueResponse(Box::new(certificate))
    }
}

impl From<Certificate> for RpcMessage {
    fn from(certificate: Certificate) -> Self {
        RpcMessage::DownloadCertificateResponse(Box::new(certificate))
    }
}

impl From<Vec<Certificate>> for RpcMessage {
    fn from(certificates: Vec<Certificate>) -> Self {
        RpcMessage::DownloadCertificatesResponse(Box::new(certificates))
    }
}
