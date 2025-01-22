// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{InputObject, SimpleObject, Union};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, ChainId, PublicKey},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};
use matching_engine::{Order, OrderNature, Price};
use serde::{Deserialize, Serialize};

use rfq::{RequestId, TokenPair};

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
    quoter_pub_key: PublicKey,
    maybe_quoter_account: Option<AccountOwner>,
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

    pub fn get_quoter_pub_key(&self) -> PublicKey {
        self.quoter_pub_key
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
    AwaitingTokens(AwaitingTokens),
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
pub struct TempChainState {
    request_id: RequestId,
    initiator: ChainId,
    me_application_id: ApplicationId,
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
        quoter_pub_key: PublicKey,
        maybe_quoter_account: Option<AccountOwner>,
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
                    quoter_pub_key,
                    maybe_quoter_account,
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
                maybe_quoter_account: Some(quoter_account),
                ..
            }) => {
                req_data.state = RequestState::AwaitingTokens(AwaitingTokens {
                    token_pair: token_pair.clone(),
                    amount: *amount,
                    price: *price,
                    quoter_account: *quoter_account,
                    matching_engine_chain_id,
                    matching_engine_app_id,
                });
            }
            _ => panic!("Request not in the QuoteProvided state or no quoter account present!"),
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
            RequestState::AwaitingTokens(awaiting_tokens) => awaiting_tokens.clone(),
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
        self.requests.remove(request_id).expect("Request not found");
        match req_data.state {
            RequestState::ExchangeInProgress(ExchangeInProgress {
                matching_engine_chain_id,
            }) if req_data.we_requested => Some(matching_engine_chain_id),
            _ => None,
        }
    }

    pub async fn close_request(&mut self, request_id: &RequestId) -> ChainId {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        self.requests.remove(request_id).expect("Request not found");
        match req_data.state {
            RequestState::ExchangeInProgress(ExchangeInProgress {
                matching_engine_chain_id: exchange_chain_id,
            }) => exchange_chain_id,
            _ => panic!("Request not in the ExchangeInProgress state!"),
        }
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
    ) {
        self.temp_chain_state.set(Some(TempChainState {
            request_id,
            initiator,
            me_application_id,
        }));
    }

    pub fn me_application_id(&mut self) -> Option<ApplicationId> {
        self.temp_chain_state
            .get()
            .clone()
            .map(|temp_chain_state| temp_chain_state.me_application_id)
    }
}
