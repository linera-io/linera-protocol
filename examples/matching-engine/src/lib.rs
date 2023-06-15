// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{scalar, InputObject, Request, Response, SimpleObject};
use fungible::{AccountOwner, FungibleTokenAbi};
use linera_sdk::base::{Amount, ApplicationId, ContractAbi, ServiceAbi};
use linera_views::{common::CustomSerialize, views::ViewError};
use serde::{Deserialize, Serialize};

// TODO(#768): Remove the derive macros.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct MatchingEngineAbi;

impl ContractAbi for MatchingEngineAbi {
    type InitializationArgument = ();
    type Parameters = Parameters;
    type Operation = Operation;
    type ApplicationCall = ApplicationCall;
    type Message = Message;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for MatchingEngineAbi {
    type Parameters = Parameters;
    type Query = Request;
    type QueryResponse = Response;
}

/// The asking or bidding price of token 1 in units of token 0.
///
/// Forgetting about types and units, if `account` is buying `quantity` for a `price`:
/// ```ignore
/// account[0] -= price * quantity;
/// account[1] += quantity;
/// ```
/// Thus the quantity (also called _count_) is an `Amount`.
///
/// When we have ask > bid then the winner for the residual cash is the liquidity provider.
/// We choose to force the price to be an integer u64. This is because the tokens are undivisible.
/// In practice, this means that the value of token1 has to be much higher than the price of token0
/// just as in a normal market where the price is in multiple of cents.
///
/// TODO(#841): Implementing fractional price is preferable for some exchanges. This cause some
/// technical issues with the serialization because we want the serialization order to be the
/// same as the original fractions. One way is to keep the serialization order we can limit
/// ourselves to fractions of the form say x / 100000.
/// The next problem is that if we do the fractions, then the order can only be filled partially. And
/// in a mathematical way, Euclidean divisions have to be done.
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Deserialize, Serialize, SimpleObject, InputObject,
)]
#[graphql(input_name = "PriceInput")]
pub struct Price {
    pub price: u64,
}

/// We use the custom serialization for the Price so that the order of the serialization
/// corresponds to the order of the Prices.
impl CustomSerialize for Price {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let mut short_key = bcs::to_bytes(&self.price)?;
        short_key.reverse();
        Ok(short_key)
    }

    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError> {
        let mut bytes = short_key.to_vec();
        bytes.reverse();
        let value = bcs::from_bytes(&bytes)?;
        Ok(value)
    }
}

impl Price {
    pub fn revert(&self) -> Self {
        Price {
            price: u64::MAX - self.price,
        }
    }
}

pub fn product_price_amount(price: Price, count: Amount) -> Amount {
    count.try_mul(price.price as u128).expect("product")
}

/// An identifier for a buy or sell order
pub type OrderId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OrderNature {
    /// A bid for buying token 1 and paying in token 0
    Bid,
    /// An ask for selling token 1, to be paid in token 0
    Ask,
}

scalar!(OrderNature);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Order {
    /// Insertion of an order
    Insert {
        owner: AccountOwner,
        amount: Amount,
        nature: OrderNature,
        price: Price,
    },
    /// Cancelling of an order
    Cancel {
        owner: AccountOwner,
        order_id: OrderId,
    },
    /// Modifying order (only decreasing is allowed)
    Modify {
        owner: AccountOwner,
        order_id: OrderId,
        cancel_amount: Amount,
    },
}

scalar!(Order);

/// When the matching engine is created we need to create to
/// trade between two tokens 0 and 1. Those two tokens
/// are put as parameters in the creation of the matching engine
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Parameters {
    /// The token0 and token1 used for the matching engine
    pub tokens: [ApplicationId<FungibleTokenAbi>; 2],
}

scalar!(Parameters);

/// Operations that can be sent to the application.
#[derive(Debug, Deserialize, Serialize)]
pub enum Operation {
    /// The order that is going to be executed on the chain of the order book.
    ExecuteOrder { order: Order },
}

/// Messages that can be processed by the application.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// The order being transmitted from the chain and received by the chain of the order book.
    ExecuteOrder { order: Order },
}

/// Arguments for an application call to the matching engine by another application.
#[derive(Debug, Deserialize, Serialize)]
pub enum ApplicationCall {
    /// The order from the application
    ExecuteOrder { order: Order },
}
