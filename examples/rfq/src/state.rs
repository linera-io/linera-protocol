// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{InputObject, SimpleObject, Union};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, ChainId, Owner},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};
use matching_engine::{Order, OrderNature, Price};
use rfq::{RequestId, TokenPair};
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
    price: Price,
    quoter_owner: Owner,
}

impl QuoteProvided {
    pub fn get_ask_order(&self, owner: AccountOwner) -> Order {
        Order::Insert {
            owner,
            amount: self.amount,
            price: self.price,
            nature: OrderNature::Ask,
        }
    }

    pub fn get_quoter_owner(&self) -> Owner {
        self.quoter_owner
    }

    pub fn get_amount(&self) -> Amount {
        self.amount
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct ExchangeInProgress {
    matching_engine_chain_id: ChainId,
}

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct AwaitingTokens {
    pub token_pair: TokenPair,
    pub amount: Amount,
    pub price: Price,
    pub quoter_account: AccountOwner,
    pub matching_engine_chain_id: ChainId,
    pub matching_engine_app_id: ApplicationId,
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
    we_requested: bool,
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
    me_application_id: ApplicationId,
    token_pair: TokenPair,
    account_owners: Vec<TempChainTokenHolder>,
}

#[derive(RootView, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct RfqState {
    next_seq_number: RegisterView<u64>,
    requests: MapView<RequestId, RequestData>,
    temp_chain_state: RegisterView<Option<TempChainState>>,
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
                &RequestId::new(target, seq_number),
                RequestData {
                    state: request_state,
                    we_requested: true,
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
                &request_id,
                RequestData {
                    state: RequestState::QuoteRequested(QuoteRequested { token_pair, amount }),
                    we_requested: false,
                },
            )
            .expect("Couldn't insert a new request state");
    }

    pub async fn update_state_with_quote(
        &mut self,
        request_id: &RequestId,
        quote: Price,
        quoter_owner: Owner,
    ) {
        let req_data = self
            .requests
            .get_mut(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteRequested(QuoteRequested { token_pair, amount }) => {
                req_data.state = RequestState::QuoteProvided(QuoteProvided {
                    token_pair: token_pair.clone(),
                    amount: *amount,
                    price: quote,
                    quoter_owner,
                });
            }
            _ => panic!("Request not in the QuoteRequested state!"),
        }
    }

    pub async fn quote_provided(&self, request_id: &RequestId) -> QuoteProvided {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided(quote_provided) => quote_provided.clone(),
            _ => panic!("Request not in the QuoteProvided state!"),
        }
    }

    pub async fn start_exchange(
        &mut self,
        request_id: &RequestId,
        matching_engine_chain_id: ChainId,
    ) {
        let req_data = self
            .requests
            .get_mut(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided(_) | RequestState::AwaitingTokens(_) => {
                req_data.state = RequestState::ExchangeInProgress(ExchangeInProgress {
                    matching_engine_chain_id,
                });
            }
            _ => panic!("Request not in the QuoteProvided or AwaitingTokens state!"),
        }
    }

    pub async fn quote_accepted(
        &mut self,
        request_id: &RequestId,
        matching_engine_chain_id: ChainId,
        matching_engine_app_id: ApplicationId,
    ) {
        let req_data = self
            .requests
            .get_mut(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided(QuoteProvided {
                token_pair,
                amount,
                price,
                quoter_owner,
                ..
            }) => {
                req_data.state = RequestState::AwaitingTokens(Box::new(AwaitingTokens {
                    token_pair: token_pair.clone(),
                    amount: *amount,
                    price: *price,
                    quoter_account: (*quoter_owner).into(),
                    matching_engine_chain_id,
                    matching_engine_app_id,
                }));
            }
            _ => panic!("Request not in the QuoteProvided state!"),
        }
    }

    pub async fn get_awaiting_tokens(&self, request_id: &RequestId) -> AwaitingTokens {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::AwaitingTokens(awaiting_tokens) => (**awaiting_tokens).clone(),
            _ => panic!("Request not in the AwaitingTokens state!"),
        }
    }

    pub async fn cancel_request(&mut self, request_id: &RequestId) -> Option<ChainId> {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match req_data.state {
            RequestState::ExchangeInProgress(ExchangeInProgress {
                matching_engine_chain_id,
            }) if req_data.we_requested => Some(matching_engine_chain_id),
            _ => {
                self.requests.remove(request_id).expect("Request not found");
                None
            }
        }
    }

    pub async fn get_matching_engine_chain_id(&self, request_id: &RequestId) -> ChainId {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match req_data.state {
            RequestState::ExchangeInProgress(ExchangeInProgress {
                matching_engine_chain_id,
            }) => matching_engine_chain_id,
            _ => panic!("Request not in the ExchangeInProgress state!"),
        }
    }

    pub async fn close_request(&mut self, request_id: &RequestId) {
        self.requests.remove(request_id).expect("Request not found");
    }

    pub async fn token_pair(&self, request_id: &RequestId) -> TokenPair {
        self.requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found")
            .state
            .token_pair()
    }

    pub fn init_temp_chain_state(
        &mut self,
        request_id: RequestId,
        initiator: ChainId,
        me_application_id: ApplicationId,
        token_pair: TokenPair,
    ) {
        self.temp_chain_state.set(Some(TempChainState {
            request_id,
            initiator,
            me_application_id,
            token_pair,
            account_owners: vec![],
        }));
    }

    pub fn add_owner_to_temp_chain_state(
        &mut self,
        account_owner: AccountOwner,
        chain_id: ChainId,
    ) {
        let temp_chain_state = self
            .temp_chain_state
            .get_mut()
            .as_mut()
            .expect("No TempChainState found!");
        temp_chain_state.account_owners.push(TempChainTokenHolder {
            account_owner,
            chain_id,
        });
    }

    pub fn temp_chain_account_owners(&self) -> Vec<TempChainTokenHolder> {
        self.temp_chain_state
            .get()
            .as_ref()
            .map(|temp_state| temp_state.account_owners.clone())
            .unwrap_or_default()
    }

    pub fn temp_chain_token_pair(&self) -> TokenPair {
        self.temp_chain_state
            .get()
            .as_ref()
            .expect("No TempChainState found!")
            .token_pair
            .clone()
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

    pub fn me_application_id(&mut self) -> Option<ApplicationId> {
        self.temp_chain_state
            .get()
            .clone()
            .map(|temp_chain_state| temp_chain_state.me_application_id)
    }
}
