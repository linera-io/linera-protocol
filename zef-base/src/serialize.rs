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
    Certificate(Box<Certificate>),
    ChainInfoQuery(Box<ChainInfoQuery>),
    // Outbound
    Vote(Box<Vote>),
    ChainInfoResponse(Box<ChainInfoResponse>),
    Error(Box<Error>),
    // Internal to an validator
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
