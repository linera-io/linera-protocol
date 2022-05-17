// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::messages::*;
use crate::error::*;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
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

fn serialize<T>(msg: &T) -> Vec<u8>
where
    T: Serialize,
{
    let mut buf = Vec::new();
    bincode::serialize_into(&mut buf, msg)
        .expect("Serializing to a resizable buffer should not fail.");
    buf
}

pub fn serialize_message(msg: &SerializedMessage) -> Vec<u8> {
    serialize(msg)
}

pub fn deserialize_message<R>(reader: R) -> Result<SerializedMessage, Box<bincode::ErrorKind>>
where
    R: std::io::Read,
{
    bincode::deserialize_from(reader)
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
