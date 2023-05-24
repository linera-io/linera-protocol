// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::ChainId;
use linera_chain::data_types::{
    BlockProposal, Certificate, HashedValue, LiteCertificate, LiteVote,
};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    node::NodeError,
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum RpcMessage {
    // Inbound
    BlockProposal(Box<BlockProposal>),
    Certificate(Box<HandleCertificateRequest>),
    LiteCertificate(Box<HandleLiteCertificateRequest<'static>>),
    ChainInfoQuery(Box<ChainInfoQuery>),
    // Outbound
    Vote(Box<LiteVote>),
    ChainInfoResponse(Box<ChainInfoResponse>),
    Error(Box<NodeError>),
    // Internal to a validator
    CrossChainRequest(Box<CrossChainRequest>),
}

impl RpcMessage {
    /// Obtains the [`ChainId`] of the chain targeted by this message, if there is one.
    ///
    /// Only inbound messages have target chains.
    pub fn target_chain_id(&self) -> Option<ChainId> {
        let chain_id = match self {
            RpcMessage::BlockProposal(proposal) => proposal.content.block.chain_id,
            RpcMessage::LiteCertificate(request) => request.certificate.value.chain_id,
            RpcMessage::Certificate(request) => request.certificate.value().chain_id(),
            RpcMessage::ChainInfoQuery(query) => query.chain_id,
            RpcMessage::CrossChainRequest(request) => request.target_chain_id(),
            RpcMessage::Vote(_) | RpcMessage::Error(_) | RpcMessage::ChainInfoResponse(_) => {
                return None;
            }
        };
        Some(chain_id)
    }
}

impl From<BlockProposal> for RpcMessage {
    fn from(block_proposal: BlockProposal) -> Self {
        RpcMessage::BlockProposal(Box::new(block_proposal))
    }
}

impl From<HandleLiteCertificateRequest<'static>> for RpcMessage {
    fn from(request: HandleLiteCertificateRequest<'static>) -> Self {
        RpcMessage::LiteCertificate(Box::new(request))
    }
}

impl From<HandleCertificateRequest> for RpcMessage {
    fn from(request: HandleCertificateRequest) -> Self {
        RpcMessage::Certificate(Box::new(request))
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

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct HandleLiteCertificateRequest<'a> {
    pub certificate: LiteCertificate<'a>,
    pub wait_for_outgoing_messages: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct HandleCertificateRequest {
    pub certificate: Certificate,
    pub wait_for_outgoing_messages: bool,
    pub blobs: Vec<HashedValue>,
}
