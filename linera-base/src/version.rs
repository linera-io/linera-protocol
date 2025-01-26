//! Linera Protocol version.

use serde::{Deserialize, Serialize};

use crate::codec::Error;

/// Single Byte to distinguish between different
/// Linera Protocol version.
/// Currently seperates Mainnet and Testnet.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize)]
pub enum Protocol {
    /// Tetstnet byte set to 255
    Testnet = 0xFF,

    /// Mainnet is represented as 01
    Mainnet = 0x01,
}

impl TryFrom<u8> for Protocol {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xFF => Ok(Self::Testnet),
            0x01 => Ok(Self::Mainnet),
            _ => Err(Error::SerializeError(format!("Protcol version corresponding to value: {value} is not part of the curerent consensus"))),
        }
    }
}

impl From<Protocol> for u8 {
    fn from(value: Protocol) -> Self {
        match value {
            Protocol::Testnet => 0xFF,
            Protocol::Mainnet => 0x01,
        }
    }
}
