// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ChainId, ContractAbi, ServiceAbi},
};
use serde::{Deserialize, Serialize};

pub struct EventEmitterAbi;

impl ContractAbi for EventEmitterAbi {
    type Operation = Operation;
    type Response = Option<String>;
}

impl ServiceAbi for EventEmitterAbi {
    type Query = Request;
    type QueryResponse = Response;
}

#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
pub enum Operation {
    Emit {
        stream_name: String,
        value: String,
    },
    ReadEvent {
        chain_id: ChainId,
        stream_name: String,
        index: u32,
    },
}
