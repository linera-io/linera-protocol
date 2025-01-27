// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Requests For Quotes Example Application */

use async_graphql::{scalar, InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    base::{Amount, ApplicationId, BytecodeId, ChainId, ContractAbi, MessageId, Owner, ServiceAbi},
    graphql::GraphQLMutationRoot,
};
use matching_engine::{Order, Price};
use serde::{Deserialize, Serialize};

pub struct RfqAbi;

impl ContractAbi for RfqAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for RfqAbi {
    type Query = Request;
    type QueryResponse = Response;
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Parameters {
    pub me_bytecode_id: BytecodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "TokenPairInput")]
pub struct TokenPair {
    pub token_offered: ApplicationId,
    pub token_asked: ApplicationId,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "RequestIdInput")]
pub struct RequestId {
    other_chain_id: ChainId,
    seq_num: u64,
}

impl RequestId {
    pub fn new(other_chain_id: ChainId, seq_num: u64) -> Self {
        Self {
            other_chain_id,
            seq_num,
        }
    }

    pub fn chain_id(&self) -> ChainId {
        self.other_chain_id
    }

    pub fn seq_number(&self) -> u64 {
        self.seq_num
    }
}

/// Operations that can be sent to the application.
#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
pub enum Operation {
    RequestQuote {
        target: ChainId,
        token_pair: TokenPair,
        amount: Amount,
    },
    ProvideQuote {
        request_id: RequestId,
        quote: Price,
        quoter_owner: Owner,
    },
    AcceptQuote {
        request_id: RequestId,
        owner: Owner,
        fee_budget: Amount,
    },
    FinalizeDeal {
        request_id: RequestId,
    },
    CloseRequest {
        request_id: RequestId,
    },
    CancelRequest {
        request_id: RequestId,
    },
}

scalar!(Operation);

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    RequestQuote {
        seq_number: u64,
        token_pair: TokenPair,
        amount: Amount,
    },
    ProvideQuote {
        seq_number: u64,
        quote: Price,
        quoter_owner: Owner,
    },
    QuoteAccepted {
        request_id: RequestId,
        matching_engine_message_id: MessageId,
        matching_engine_app_id: ApplicationId,
    },
    CancelRequest {
        seq_number: u64,
    },
    StartMatchingEngine {
        initiator: ChainId,
        request_id: RequestId,
        order: Order,
        token_pair: Box<TokenPair>,
        matching_engine_message_id: MessageId,
    },
    TokensSent {
        matching_engine_app_id: ApplicationId,
        order: Order,
    },
    CloseChain,
    ChainClosed {
        request_id: RequestId,
    },
}

impl Message {
    pub fn seq_number(&self) -> u64 {
        match self {
            Message::RequestQuote { seq_number, .. }
            | Message::ProvideQuote { seq_number, .. }
            | Message::CancelRequest { seq_number, .. } => *seq_number,
            Message::StartMatchingEngine { request_id, .. }
            | Message::QuoteAccepted { request_id, .. }
            | Message::ChainClosed { request_id } => request_id.seq_num,
            Message::TokensSent { .. } | Message::CloseChain => {
                // not important, we can just return 0
                0
            }
        }
    }
}
