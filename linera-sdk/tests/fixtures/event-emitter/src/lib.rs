// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Request, Response};
use linera_sdk::{
    formats::StableEnum,
    graphql::GraphQLMutationRoot,
    linera_base_types::{ChainId, ContractAbi, ServiceAbi},
};

pub struct EventEmitterAbi;

impl ContractAbi for EventEmitterAbi {
    type Operation = Operation;
    type Response = Option<String>;
}

impl ServiceAbi for EventEmitterAbi {
    type Query = Request;
    type QueryResponse = Response;
}

#[derive(Debug, StableEnum, GraphQLMutationRoot)]
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
