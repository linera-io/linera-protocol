// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
# Meta-Counter Example Application

This application is only used for testing cross-application calls.
*/

use async_graphql::{Request, Response};
use linera_sdk::base::{ApplicationId, ChainId, ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct MetaCounterAbi;

impl ContractAbi for MetaCounterAbi {
    type InitializationArgument = ();
    type Parameters = ApplicationId<counter::CounterAbi>;
    type Operation = Operation;
    type ApplicationCall = ();
    type Message = Message;
    type Response = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Operation {
    pub recipient_id: ChainId,
    pub authenticated: bool,
    pub is_tracked: bool,
    pub fuel_grant: u64,
    pub message: Message,
}

impl Operation {
    pub fn increment(recipient_id: ChainId, value: u64) -> Self {
        Operation {
            recipient_id,
            authenticated: false,
            is_tracked: false,
            fuel_grant: 0,
            message: Message::Increment(value),
        }
    }

    pub fn fail(recipient_id: ChainId) -> Self {
        Operation {
            recipient_id,
            authenticated: false,
            is_tracked: false,
            fuel_grant: 0,
            message: Message::Fail,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Increment(u64),
    Fail,
}

impl ServiceAbi for MetaCounterAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = ApplicationId<counter::CounterAbi>;
}
