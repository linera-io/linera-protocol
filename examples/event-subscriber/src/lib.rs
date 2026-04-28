// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ApplicationId, ChainId, ContractAbi, ServiceAbi},
};
use serde::{Deserialize, Serialize};

pub struct EventSubscriberAbi;

impl ContractAbi for EventSubscriberAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for EventSubscriberAbi {
    type Query = Request;
    type QueryResponse = Response;
}

#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
pub enum Operation {
    Subscribe {
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: String,
    },
    Unsubscribe {
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: String,
    },
}
