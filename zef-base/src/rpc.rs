// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::messages::*;
use crate::error::*;

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
