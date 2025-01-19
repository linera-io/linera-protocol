// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Meta-Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::base::{ChainId, ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct MetaCounterAbi;

impl ContractAbi for MetaCounterAbi {
    type Operation = Operation;
    type Response = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Operation {
    pub recipient_id: ChainId,
    pub authenticated: bool,
    pub is_tracked: bool,
    pub query_service: bool,
    pub fuel_grant: u64,
    pub message: Message,
}

impl Operation {
    pub fn increment(recipient_id: ChainId, value: u64, query_service: bool) -> Self {
        Operation {
            recipient_id,
            authenticated: false,
            is_tracked: false,
            query_service,
            fuel_grant: 0,
            message: Message::Increment(value),
        }
    }

    pub fn fail(recipient_id: ChainId) -> Self {
        Operation {
            recipient_id,
            authenticated: false,
            is_tracked: false,
            query_service: false,
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
}
