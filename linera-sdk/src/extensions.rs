#[cfg(feature = "serde")]
use serde::de::DeserializeOwned;

#[cfg(feature = "serde")]
pub trait FromBcsBytes: Sized {
    fn from_bcs_bytes(bytes: &[u8]) -> Result<Self, bcs::Error>;
}

#[cfg(feature = "serde")]
impl<'de, AnyType> FromBcsBytes for AnyType
where
    AnyType: DeserializeOwned,
{
    fn from_bcs_bytes(bytes: &[u8]) -> Result<Self, bcs::Error> {
        bcs::from_bytes(bytes)
    }
}
