// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{InputObject, SimpleObject, Union};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount, ChainId},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};
use rfq::{RequestId, TokenPair, Tokens};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct QuoteRequested {
    token_pair: TokenPair,
    amount: Amount,
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct QuoteProvided {
    token_pair: TokenPair,
    amount: Amount,
    amount_offered: Amount,
    quoter_owner: AccountOwner,
}

impl QuoteProvided {
    pub fn get_quoter_owner(&self) -> AccountOwner {
        self.quoter_owner
    }

    pub fn get_amount(&self) -> Amount {
        self.amount
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct ExchangeInProgress {
    temp_chain_id: ChainId,
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct AwaitingTokens {
    pub token_pair: TokenPair,
    pub amount_offered: Amount,
    pub quoter_account: AccountOwner,
    pub temp_chain_id: ChainId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Union)]
pub enum RequestState {
    QuoteRequested(QuoteRequested),
    QuoteProvided(QuoteProvided),
    AwaitingTokens(Box<AwaitingTokens>),
    ExchangeInProgress(ExchangeInProgress),
}

impl RequestState {
    fn token_pair(&self) -> TokenPair {
        match self {
            RequestState::QuoteRequested(QuoteRequested { token_pair, .. })
            | RequestState::QuoteProvided(QuoteProvided { token_pair, .. }) => token_pair.clone(),
            _ => panic!("invalid state for reading the token pair"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct RequestData {
    state: RequestState,
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct TempChainTokenHolder {
    pub account_owner: AccountOwner,
    pub chain_id: ChainId,
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct TempChainState {
    request_id: RequestId,
    initiator: ChainId,
    token_pair: TokenPair,
    tokens_in_hold: Option<Tokens>,
}

#[derive(RootView, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct RfqState {
    next_seq_number: RegisterView<u64>,
    requests: MapView<RequestId, RequestData>,
    temp_chain_state: RegisterView<Option<TempChainState>>,
}

impl RequestData {
    pub fn update_state_with_quote(&mut self, quote: Amount, quoter_owner: AccountOwner) {
        match &self.state {
            RequestState::QuoteRequested(QuoteRequested { token_pair, amount }) => {
                self.state = RequestState::QuoteProvided(QuoteProvided {
                    token_pair: token_pair.clone(),
                    amount: *amount,
                    amount_offered: quote,
                    quoter_owner,
                });
            }
            _ => panic!("Request not in the QuoteRequested state!"),
        }
    }

    pub fn quote_provided(&self) -> QuoteProvided {
        match &self.state {
            RequestState::QuoteProvided(quote_provided) => quote_provided.clone(),
            _ => panic!("Request not in the QuoteProvided state!"),
        }
    }

    pub fn start_exchange(&mut self, temp_chain_id: ChainId) {
        match &self.state {
            RequestState::QuoteProvided(_) | RequestState::AwaitingTokens(_) => {
                self.state = RequestState::ExchangeInProgress(ExchangeInProgress { temp_chain_id });
            }
            _ => panic!("Request not in the QuoteProvided or AwaitingTokens state!"),
        }
    }

    pub fn accept_quote(&mut self, temp_chain_id: ChainId) {
        match &self.state {
            RequestState::QuoteProvided(QuoteProvided {
                token_pair,
                amount_offered,
                quoter_owner,
                ..
            }) => {
                self.state = RequestState::AwaitingTokens(Box::new(AwaitingTokens {
                    token_pair: token_pair.clone(),
                    amount_offered: *amount_offered,
                    quoter_account: *quoter_owner,
                    temp_chain_id,
                }));
            }
            _ => panic!("Request not in the QuoteProvided state!"),
        }
    }

    pub fn awaiting_tokens(&self) -> AwaitingTokens {
        match &self.state {
            RequestState::AwaitingTokens(awaiting_tokens) => (**awaiting_tokens).clone(),
            _ => panic!("Request not in the AwaitingTokens state!"),
        }
    }

    pub fn token_pair(&self) -> TokenPair {
        self.state.token_pair()
    }
}

impl RfqState {
    pub fn create_new_request(
        &mut self,
        target: ChainId,
        token_pair: TokenPair,
        amount: Amount,
    ) -> u64 {
        let seq_number = *self.next_seq_number.get();
        let request_state = RequestState::QuoteRequested(QuoteRequested { token_pair, amount });
        self.requests
            .insert(
                &RequestId::new(target, seq_number, true),
                RequestData {
                    state: request_state,
                },
            )
            .expect("Couldn't insert a new request state");
        self.next_seq_number.set(seq_number + 1);
        seq_number
    }

    pub fn register_request(
        &mut self,
        request_id: RequestId,
        token_pair: TokenPair,
        amount: Amount,
    ) {
        self.requests
            .insert(
                &request_id.with_we_requested(false),
                RequestData {
                    state: RequestState::QuoteRequested(QuoteRequested { token_pair, amount }),
                },
            )
            .expect("Couldn't insert a new request state");
    }

    pub async fn cancel_request(&mut self, request_id: &RequestId) -> Option<ChainId> {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match req_data.state {
            RequestState::ExchangeInProgress(ExchangeInProgress { temp_chain_id })
                if request_id.is_our_request() =>
            {
                Some(temp_chain_id)
            }
            _ => {
                self.requests.remove(request_id).expect("Request not found");
                None
            }
        }
    }

    pub async fn close_request(&mut self, request_id: &RequestId) {
        self.requests.remove(request_id).expect("Request not found");
    }

    pub async fn request_data(&mut self, request_id: &RequestId) -> Option<&mut RequestData> {
        self.requests.get_mut(request_id).await.expect("ViewError")
    }

    pub fn init_temp_chain_state(
        &mut self,
        request_id: RequestId,
        initiator: ChainId,
        token_pair: TokenPair,
        sent_tokens: Tokens,
    ) {
        self.temp_chain_state.set(Some(TempChainState {
            request_id,
            initiator,
            token_pair,
            tokens_in_hold: Some(sent_tokens),
        }));
    }

    pub fn is_temp_chain(&self) -> bool {
        self.temp_chain_state.get().is_some()
    }

    pub fn temp_chain_held_tokens(&self) -> Option<Tokens> {
        self.temp_chain_state
            .get()
            .as_ref()
            .and_then(|temp_state| temp_state.tokens_in_hold.clone())
    }

    pub fn take_temp_chain_held_tokens(&mut self) -> Option<Tokens> {
        self.temp_chain_state
            .get_mut()
            .as_mut()
            .and_then(|temp_state| temp_state.tokens_in_hold.take())
    }

    pub fn temp_chain_initiator_and_request_id(&self) -> (ChainId, RequestId) {
        let temp_chain_state = self
            .temp_chain_state
            .get()
            .as_ref()
            .expect("No TempChainState found!");
        (
            temp_chain_state.initiator,
            temp_chain_state.request_id.clone(),
        )
    }
}
