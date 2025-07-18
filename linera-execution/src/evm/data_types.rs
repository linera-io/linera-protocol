// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_primitives::U256;
use linera_base::data_types::Amount;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
/// An encapsulation of U256 in order to have a specific serialization
pub struct AmountU256(U256);

impl Serialize for AmountU256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;
        let v: [u8; 32] = self.0.to_be_bytes();
        let mut tuple = serializer.serialize_tuple(32)?;
        for byte in v.iter() {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for AmountU256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: [u8; 32] = Deserialize::deserialize(deserializer)?;
        let value = U256::from_be_bytes(bytes);
        Ok(AmountU256(value))
    }
}

impl From<Amount> for AmountU256 {
    fn from(amount: Amount) -> AmountU256 {
        AmountU256(amount.into())
    }
}

#[cfg(test)]
mod tests {
    use linera_base::data_types::Amount;

    use crate::evm::data_types::AmountU256;

    #[test]
    fn check_bcs_serialization() -> anyhow::Result<()> {
        let value = 7837438347454859505557763535536363636;
        let value = Amount::from_tokens(value);
        let value = AmountU256::from(value);
        let vec = bcs::to_bytes(&value)?;
        assert_eq!(vec.len(), 32);
        assert_eq!(value, bcs::from_bytes(&vec)?);
        Ok(())
    }
}
