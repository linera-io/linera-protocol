// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Requests For Quotes Example Application */

use async_graphql::{scalar, InputObject, Request, Response, SimpleObject};
use fungible::Account;
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{Amount, Application, ChainId, ContractAbi, Owner, ServiceAbi},
};
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

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "TokenPairInput")]
pub struct TokenPair {
    pub token_offered: Application,
    pub token_asked: Application,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "RequestIdInput")]
pub struct RequestId {
    other_chain_id: ChainId,
    seq_num: u64,
    we_requested: bool,
}

impl RequestId {
    pub fn new(other_chain_id: ChainId, seq_num: u64, we_requested: bool) -> Self {
        Self {
            other_chain_id,
            seq_num,
            we_requested,
        }
    }

    pub fn chain_id(&self) -> ChainId {
        self.other_chain_id
    }

    pub fn seq_number(&self) -> u64 {
        self.seq_num
    }

    pub fn is_our_request(&self) -> bool {
        self.we_requested
    }

    pub fn with_we_requested(self, we_requested: bool) -> Self {
        Self {
            we_requested,
            ..self
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "TokensInput")]
pub struct Tokens {
    pub token_id: Application,
    pub owner: Account,
    pub amount: Amount,
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
        quote: Amount,
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
        quote: Amount,
        quoter_owner: Owner,
    },
    QuoteAccepted {
        request_id: RequestId,
    },
    CancelRequest {
        seq_number: u64,
        recipient_requested: bool,
    },
    StartExchange {
        initiator: ChainId,
        request_id: RequestId,
        token_pair: Box<TokenPair>,
        tokens: Box<Tokens>,
    },
    TokensSent {
        tokens: Box<Tokens>,
    },
    CloseChain,
    ChainClosed {
        request_id: RequestId,
    },
}

impl Message {
    pub fn request_id(&self, other_chain_id: ChainId) -> RequestId {
        match self {
            Message::RequestQuote { seq_number, .. } => {
                RequestId::new(other_chain_id, *seq_number, false)
            }
            Message::ProvideQuote { seq_number, .. } => {
                RequestId::new(other_chain_id, *seq_number, true)
            }
            Message::QuoteAccepted { request_id }
            | Message::StartExchange { request_id, .. }
            | Message::ChainClosed { request_id } => request_id.clone(),
            Message::CancelRequest {
                seq_number,
                recipient_requested,
            } => RequestId::new(other_chain_id, *seq_number, *recipient_requested),
            Message::TokensSent { .. } | Message::CloseChain => {
                // unused
                RequestId::new(other_chain_id, 0, false)
            }
        }
    }
}
