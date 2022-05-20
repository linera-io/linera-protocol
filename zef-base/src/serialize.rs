// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::messages::*;
use crate::error::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedMessage {
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

impl From<BlockProposal> for SerializedMessage {
    fn from(block_proposal: BlockProposal) -> Self {
        SerializedMessage::BlockProposal(Box::new(block_proposal))
    }
}

impl From<Certificate> for SerializedMessage {
    fn from(certificate: Certificate) -> Self {
        SerializedMessage::Certificate(Box::new(certificate))
    }
}

impl From<ChainInfoQuery> for SerializedMessage {
    fn from(chain_info_query: ChainInfoQuery) -> Self {
        SerializedMessage::ChainInfoQuery(Box::new(chain_info_query))
    }
}

impl From<Vote> for SerializedMessage {
    fn from(vote: Vote) -> Self {
        SerializedMessage::Vote(Box::new(vote))
    }
}

impl From<ChainInfoResponse> for SerializedMessage {
    fn from(chain_info_response: ChainInfoResponse) -> Self {
        SerializedMessage::ChainInfoResponse(Box::new(chain_info_response))
    }
}

impl From<Error> for SerializedMessage {
    fn from(error: Error) -> Self {
        SerializedMessage::Error(Box::new(error))
    }
}

impl From<CrossChainRequest> for SerializedMessage {
    fn from(cross_chain_request: CrossChainRequest) -> Self {
        SerializedMessage::CrossChainRequest(Box::new(cross_chain_request))
    }
}
