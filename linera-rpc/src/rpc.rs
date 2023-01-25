// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::ChainId;
use linera_chain::data_types::{BlockProposal, Certificate, HashCertificate, Vote};
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
    Certificate(Box<Certificate>),
    HashCertificate(Box<HashCertificate>),
    ChainInfoQuery(Box<ChainInfoQuery>),
    // Outbound
    Vote(Box<Vote>),
    ChainInfoResponse(Box<ChainInfoResponse>),
    Error(Box<NodeError>),
    // Internal to a validator
    CrossChainRequest(Box<CrossChainRequest>),
}

impl RpcMessage {
    /// Obtain the [`ChainId`] of the chain targeted by this message, if there is one.
    ///
    /// Only inbound messages have target chains.
    pub fn target_chain_id(&self) -> Option<ChainId> {
        let chain_id = match self {
            RpcMessage::BlockProposal(proposal) => proposal.content.block.chain_id,
            RpcMessage::HashCertificate(certificate) => certificate.chain_id,
            RpcMessage::Certificate(certificate) => certificate.value.chain_id(),
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

impl From<HashCertificate> for RpcMessage {
    fn from(certificate: HashCertificate) -> Self {
        RpcMessage::HashCertificate(Box::new(certificate))
    }
}

impl From<Certificate> for RpcMessage {
    fn from(certificate: Certificate) -> Self {
        RpcMessage::Certificate(Box::new(certificate))
    }
}

impl From<ChainInfoQuery> for RpcMessage {
    fn from(chain_info_query: ChainInfoQuery) -> Self {
        RpcMessage::ChainInfoQuery(Box::new(chain_info_query))
    }
}

impl From<Vote> for RpcMessage {
    fn from(vote: Vote) -> Self {
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
