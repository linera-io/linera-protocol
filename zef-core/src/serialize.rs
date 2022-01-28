// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::messages::*;
use crate::error::*;

use failure::format_err;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum SerializedMessage {
    // Inbound
    RequestOrder(Box<RequestOrder>),
    ConfirmationOrder(Box<ConfirmationOrder>),
    ConsensusOrder(Box<ConsensusOrder>),
    AccountInfoQuery(Box<AccountInfoQuery>),
    // Outbound
    Vote(Box<Vote>),
    AccountInfoResponse(Box<AccountInfoResponse>),
    ConsensusInfoResponse(Box<ConsensusInfoResponse>),
    Error(Box<Error>),
    // Internal to an authority
    CrossShardRequest(Box<CrossShardRequest>),
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

pub fn deserialize_message<R>(reader: R) -> Result<SerializedMessage, failure::Error>
where
    R: std::io::Read,
{
    bincode::deserialize_from(reader).map_err(|err| format_err!("{}", err))
}
