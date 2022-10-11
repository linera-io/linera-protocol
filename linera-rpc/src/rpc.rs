// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{error::*, messages::*};
use linera_chain::messages::{BlockProposal, Certificate, Vote};
use linera_core::messages::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum Message {
    // Inbound
    BlockProposal(Box<BlockProposal>),
    Certificate(Box<Certificate>),
    ChainInfoQuery(Box<ChainInfoQuery>),
    // Outbound
    Vote(Box<Vote>),
    ChainInfoResponse(Box<ChainInfoResponse>),
    Error(Box<Error>),
    // Internal to a validator
    CrossChainRequest(Box<CrossChainRequest>),
}

impl Message {
    /// Obtain the [`ChainId`] of the chain targeted by this message, if there is one.
    ///
    /// Only inbound messages have target chains.
    pub fn target_chain_id(&self) -> Option<ChainId> {
        let chain_id = match self {
            Message::BlockProposal(proposal) => proposal.content.block.chain_id,
            Message::Certificate(certificate) => certificate.value.chain_id(),
            Message::ChainInfoQuery(query) => query.chain_id,
            Message::CrossChainRequest(request) => request.target_chain_id(),
            Message::Vote(_) | Message::Error(_) | Message::ChainInfoResponse(_) => {
                return None;
            }
        };
        Some(chain_id)
    }
}

impl From<BlockProposal> for Message {
    fn from(block_proposal: BlockProposal) -> Self {
        Message::BlockProposal(Box::new(block_proposal))
    }
}

impl From<Certificate> for Message {
    fn from(certificate: Certificate) -> Self {
        Message::Certificate(Box::new(certificate))
    }
}

impl From<ChainInfoQuery> for Message {
    fn from(chain_info_query: ChainInfoQuery) -> Self {
        Message::ChainInfoQuery(Box::new(chain_info_query))
    }
}

impl From<Vote> for Message {
    fn from(vote: Vote) -> Self {
        Message::Vote(Box::new(vote))
    }
}

impl From<ChainInfoResponse> for Message {
    fn from(chain_info_response: ChainInfoResponse) -> Self {
        Message::ChainInfoResponse(Box::new(chain_info_response))
    }
}

impl From<Error> for Message {
    fn from(error: Error) -> Self {
        Message::Error(Box::new(error))
    }
}

impl From<CrossChainRequest> for Message {
    fn from(cross_chain_request: CrossChainRequest) -> Self {
        Message::CrossChainRequest(Box::new(cross_chain_request))
    }
}
