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

#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
pub struct ExchangeInProgress {
    exchange_chain_id: ChainId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Union)]
pub enum RequestState {
    QuoteRequested(QuoteRequested),
    QuoteProvided(QuoteProvided),
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

    pub async fn get_bid_order(
        &mut self,
        request_id: &RequestId,
        owner: AccountOwner,
    ) -> (Order, PublicKey) {
        let req_data = self
            .requests
            .get_mut(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided(QuoteProvided {
                amount,
                price,
                quoter_pub_key,
                ..
            }) => (
                Order::Insert {
                    owner,
                    amount: *amount,
                    nature: OrderNature::Bid,
                    price: *price,
                },
                *quoter_pub_key,
            ),
            _ => panic!("Request not in the QuoteProvided state!"),
        }
    }

    pub async fn get_quoter_account(&self, request_id: &RequestId) -> AccountOwner {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided(QuoteProvided {
                maybe_quoter_account: Some(quoter_account),
                ..
            }) => quoter_account.clone(),
            _ => panic!("Request not in the QuoteProvided state, or quoter account doesn't exist!"),
        }
    }

    pub async fn get_ask_order(&self, request_id: &RequestId, owner: AccountOwner) -> Order {
        let req_data = self
            .requests
            .get(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided(QuoteProvided { amount, price, .. }) => Order::Insert {
                owner,
                amount: *amount,
                price: *price,
                nature: OrderNature::Ask,
            },
            _ => panic!("Request not in the QuoteProvided state!"),
        }
    }

    pub async fn start_exchange(&mut self, request_id: &RequestId, exchange_chain_id: ChainId) {
        let req_data = self
            .requests
            .get_mut(request_id)
            .await
            .expect("ViewError")
            .expect("Request not found");
        match &req_data.state {
            RequestState::QuoteProvided { .. } => {
                req_data.state =
                    RequestState::ExchangeInProgress(ExchangeInProgress { exchange_chain_id });
            }
            _ => panic!("Request not in the QuoteProvided state!"),
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
            RequestState::ExchangeInProgress(ExchangeInProgress { exchange_chain_id })
                if req_data.we_requested =>
            {
                Some(exchange_chain_id)
            }
            _ => None,
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
}
