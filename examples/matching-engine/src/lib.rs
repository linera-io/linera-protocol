// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Matching Engine Example Application */

use async_graphql::{scalar, InputObject, Request, Response, SimpleObject};
use fungible::FungibleTokenAbi;
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount, ApplicationId, ContractAbi, ServiceAbi},
    views::{CustomSerialize, ViewError},
};
use serde::{Deserialize, Serialize};

pub struct MatchingEngineAbi;

impl ContractAbi for MatchingEngineAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for MatchingEngineAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// The asking or bidding price of token 1 in units of token 0.
///
/// Forgetting about types and units, if `account` is buying `quantity` for a `price` value:
/// ```ignore
/// account[0] -= price * quantity * 10^-price_decimals;
/// account[1] += quantity;
/// ```
/// where `price_decimals` is a parameter set when the market is created.
///
/// The `quantity` (also called _count_) is of type `Amount` as well as the balance of the
/// accounts. Therefore, the number of decimals used by quantities in a valid order must
/// not exceed `Amount::DECIMAL_PLACES - price_decimals`.
///
/// When we have ask > bid then the winner for the residual cash is the liquidity provider.
/// We choose to force the price to be an integer u64. This is because the tokens are undivisible.
/// In practice, this means that the value of token1 has to be much higher than the price of token0
/// just as in a normal market where the price is in multiple of cents.
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Deserialize, Serialize, SimpleObject, InputObject,
)]
#[graphql(input_name = "PriceInput")]
pub struct Price {
    /// A price expressed as a multiple of 10^-price_decimals increments.
    pub price: u64,
}

impl Price {
    pub fn to_bid(&self) -> PriceBid {
        PriceBid { price: self.price }
    }
    pub fn to_ask(&self) -> PriceAsk {
        PriceAsk { price: self.price }
    }
}

#[derive(Clone, Copy, Debug, SimpleObject, InputObject)]
#[graphql(input_name = "PriceAskInput")]
pub struct PriceAsk {
    /// A price expressed as a multiple of 10^-price_decimals increments.
    pub price: u64,
}

impl PriceAsk {
    pub fn to_price(&self) -> Price {
        Price { price: self.price }
    }
}

/// We use the custom serialization for the PriceAsk so that the order of the serialization
/// corresponds to the order of the Prices.
impl CustomSerialize for PriceAsk {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let mut short_key = bcs::to_bytes(&self.price)?;
        short_key.reverse();
        Ok(short_key)
    }

    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError> {
        let mut bytes = short_key.to_vec();
        bytes.reverse();
        let price = bcs::from_bytes(&bytes)?;
        Ok(PriceAsk { price })
    }
}

#[derive(Clone, Copy, Debug, SimpleObject, InputObject)]
#[graphql(input_name = "PriceBidInput")]
pub struct PriceBid {
    /// A price expressed as a multiple of 10^-price_decimals increments.
    pub price: u64,
}

impl PriceBid {
    pub fn to_price(&self) -> Price {
        Price { price: self.price }
    }
}

/// We use the custom serialization for the PriceBid so that the order of the serialization
/// corresponds to the order of the Prices.
impl CustomSerialize for PriceBid {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let price_rev = u64::MAX - self.price;
        let mut short_key = bcs::to_bytes(&price_rev)?;
        short_key.reverse();
        Ok(short_key)
    }

    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError> {
        let mut bytes = short_key.to_vec();
        bytes.reverse();
        let price_rev = bcs::from_bytes::<u64>(&bytes)?;
        let price = u64::MAX - price_rev;
        Ok(PriceBid { price })
    }
}

/// An identifier for a buy or sell order
pub type OrderId = u64;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum OrderNature {
    /// A bid for buying token 1 and paying in token 0
    Bid,
    /// An ask for selling token 1, to be paid in token 0
    Ask,
}

scalar!(OrderNature);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Order {
    /// Insert an order
    Insert {
        owner: AccountOwner,
        quantity: Amount,
        nature: OrderNature,
        price: Price,
    },
    /// Modify an order. The quantity can only be decreased. If zero, the order is
    /// canceled.
    Modify {
        owner: AccountOwner,
        order_id: OrderId,
        new_quantity: Amount,
    },
}

scalar!(Order);

impl Order {
    /// Get the owner from the order
    pub fn owner(&self) -> AccountOwner {
        match self {
            Order::Insert { owner, .. } => *owner,
            Order::Modify { owner, .. } => *owner,
        }
    }

    pub fn check_precision(&self, price_decimals: u16) -> Result<(), &'static str> {
        // Check if price_decimals is too high
        if price_decimals as u8 > Amount::DECIMAL_PLACES {
            return Err("price_decimals exceeds Amount::DECIMAL_PLACES");
        }

        // Get the quantity/amount to check based on the order type
        let quantity = match self {
            Order::Insert { quantity, .. } => *quantity,
            Order::Modify { new_quantity, .. } => *new_quantity,
        };

        // Calculate the minimum precision unit allowed
        // If price_decimals = 2, we need the amount to be divisible by 10^2
        let min_unit = 10u128.pow(price_decimals as u32);

        // Reconstruct the full u128 value from upper and lower halves
        let full_value = (quantity.upper_half() as u128) << 64 | (quantity.lower_half() as u128);

        // We allow this because `is_multiple_of` is still unstable in our MSRV.
        #[allow(unknown_lints)]
        #[allow(clippy::manual_is_multiple_of)]
        // Check if the quantity is divisible by the minimum unit
        // This ensures it doesn't use more than (DECIMAL_PLACES - price_decimals) decimal places
        if full_value % min_unit != 0 {
            return Err("Quantity has too much precision");
        }

        Ok(())
    }
}

/// When the matching engine is created we need to create to
/// trade between two tokens 0 and 1. Those two tokens
/// are put as parameters in the creation of the matching engine
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Parameters {
    /// The token0 and token1 used for the matching engine
    pub tokens: [ApplicationId<FungibleTokenAbi>; 2],
    /// The number of decimals for the smallest increment of a price (aka "tick size").
    /// This limits the number of decimals "quantities" can use in a valid order.
    pub price_decimals: u16,
}

scalar!(Parameters);

impl Parameters {
    pub fn check_precision(&self, order: &Order) {
        order
            .check_precision(self.price_decimals)
            .expect("Invalid precision in order");
    }

    pub fn product_price_amount(&self, price: Price, quantity: Amount) -> Amount {
        let count = quantity.saturating_div(10u128.pow(self.price_decimals as u32));
        count
            .try_mul(price.price as u128)
            .expect("overflow in pricing")
    }

    /// The application engine is trading between two tokens. Those tokens are the parameters of the
    /// construction of the exchange and are accessed by index in the system.
    pub fn fungible_id(&self, token_idx: u32) -> ApplicationId<FungibleTokenAbi> {
        self.tokens[token_idx as usize]
    }

    /// Returns amount and type of tokens that need to be transferred to the matching engine when
    /// an order is added:
    /// * For an ask, just the token1 have to be put forward
    /// * For a bid, the product of the price with the amount has to be put
    pub fn get_amount_idx(
        &self,
        nature: &OrderNature,
        price: &Price,
        quantity: &Amount,
    ) -> (Amount, u32) {
        match nature {
            OrderNature::Bid => {
                let size0 = self.product_price_amount(*price, *quantity);
                (size0, 0)
            }
            OrderNature::Ask => (*quantity, 1),
        }
    }
}

/// Operations that can be sent to the application.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// The order that is going to be executed on the chain of the order book.
    ExecuteOrder { order: Order },
    /// Close this chain, and cancel all orders.
    /// Requires that this application is authorized to close the chain.
    CloseChain,
}

/// Information about a pending order. The order ID is stored separately.
#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct PendingOrderInfo {
    pub nature: OrderNature,
    pub price: Price,
    pub quantity: Amount,
}

/// Messages that can be processed by the application.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// The order being transmitted from the chain and received by the chain of the order book.
    ExecuteOrder { order: Order },
    /// Acknowledgment sent from the matching engine to the order sender when an order is
    /// inserted and at least partially pending.
    OrderPending {
        owner: AccountOwner,
        order_id: OrderId,
        order_info: PendingOrderInfo,
    },
    /// Notification sent from the matching engine when an order is modified, filled, or cancelled.
    OrderUpdated {
        owner: AccountOwner,
        order_id: OrderId,
        new_quantity: Amount,
    },
}

#[cfg(test)]
mod tests {
    use linera_sdk::{linera_base_types::Amount, views::CustomSerialize};

    use super::{Order, OrderNature, Price, PriceAsk, PriceBid};

    #[test]
    fn test_check_precision() {
        use linera_sdk::linera_base_types::{AccountOwner, CryptoHash};
        let owner = AccountOwner::from(CryptoHash::from([0; 32]));

        // Test with price_decimals = 2, max allowed precision is 18 - 2 = 16 decimals
        let price_decimals = 2;

        // Valid: quantity with 16 decimals (divisible by 10^2)
        let valid_order = Order::Insert {
            owner,
            quantity: Amount::from_attos(100), // 100 attotokens, divisible by 100
            nature: OrderNature::Bid,
            price: Price { price: 1000 },
        };
        assert!(valid_order.check_precision(price_decimals).is_ok());

        // Invalid: quantity with 18 decimals (not divisible by 10^2)
        let invalid_order = Order::Insert {
            owner,
            quantity: Amount::from_attos(101), // 101 attotokens, not divisible by 100
            nature: OrderNature::Bid,
            price: Price { price: 1000 },
        };
        assert!(invalid_order.check_precision(price_decimals).is_err());

        // Test Modify order
        let modify_valid = Order::Modify {
            owner,
            order_id: 1,
            new_quantity: Amount::from_attos(200),
        };
        assert!(modify_valid.check_precision(price_decimals).is_ok());

        let modify_invalid = Order::Modify {
            owner,
            order_id: 1,
            new_quantity: Amount::from_attos(199),
        };
        assert!(modify_invalid.check_precision(price_decimals).is_err());

        // Test order cancellation.
        let cancel_order = Order::Modify {
            owner,
            order_id: 1,
            new_quantity: Amount::ZERO,
        };
        assert!(cancel_order.check_precision(price_decimals).is_ok());

        // Test with price_decimals = 0 (any quantity should be valid)
        let order_zero_decimals = Order::Insert {
            owner,
            quantity: Amount::from_attos(1),
            nature: OrderNature::Ask,
            price: Price { price: 500 },
        };
        assert!(order_zero_decimals.check_precision(0).is_ok());

        // Test edge case: price_decimals exceeds Amount::DECIMAL_PLACES
        assert!(valid_order.check_precision(19).is_err());
    }

    #[test]
    fn test_ordering_serialization() {
        let n = 20;
        let mut vec = Vec::new();
        let mut val = 1;
        for _ in 0..n {
            val *= 3;
            vec.push(val);
        }
        for i in 1..vec.len() {
            let val1 = vec[i - 1];
            let val2 = vec[i];
            assert!(val1 < val2);
            let price_ask1 = PriceAsk { price: val1 };
            let price_ask2 = PriceAsk { price: val2 };
            let price_bid1 = PriceBid { price: val1 };
            let price_bid2 = PriceBid { price: val2 };
            let ser_ask1 = price_ask1.to_custom_bytes().unwrap();
            let ser_ask2 = price_ask2.to_custom_bytes().unwrap();
            let ser_bid1 = price_bid1.to_custom_bytes().unwrap();
            let ser_bid2 = price_bid2.to_custom_bytes().unwrap();
            assert!(ser_ask1 < ser_ask2);
            assert!(ser_bid1 > ser_bid2);

            let price_ask1_back = PriceAsk::from_custom_bytes(&ser_ask1).unwrap();
            let price_ask2_back = PriceAsk::from_custom_bytes(&ser_ask2).unwrap();
            let price_bid1_back = PriceBid::from_custom_bytes(&ser_bid1).unwrap();
            let price_bid2_back = PriceBid::from_custom_bytes(&ser_bid2).unwrap();
            assert_eq!(price_ask1.price, price_ask1_back.price);
            assert_eq!(price_ask2.price, price_ask2_back.price);
            assert_eq!(price_bid1.price, price_bid1_back.price);
            assert_eq!(price_bid2.price, price_bid2_back.price);
        }
    }
}
