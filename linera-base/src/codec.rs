//! Defines a universal consensus codec for Linera.

use core::{error, fmt};
use std::io::{self, Read, Write};

use crate::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

/// Error type for serialization related operations.
#[derive(Debug)]
pub enum Error {
    /// Failed to encode
    SerializeError(String),
    /// Failed to read
    ReadError(io::Error),
    /// Failed to decode
    DeserializeError(String),
    /// Failed to write
    WriteError(io::Error),
    /// Underflow -- not enough bytes
    UnderflowError(String),
    /// Overflow -- too big
    OverflowError(String),
    /// Array is too big
    ArrayTooLong,
    /// Generic error
    GenericError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::SerializeError(ref s) => fmt::Display::fmt(s, f),
            Error::DeserializeError(ref s) => fmt::Display::fmt(s, f),
            Error::ReadError(ref io) => fmt::Display::fmt(io, f),
            Error::WriteError(ref io) => fmt::Display::fmt(io, f),
            Error::UnderflowError(ref s) => fmt::Display::fmt(s, f),
            Error::OverflowError(ref s) => fmt::Display::fmt(s, f),
            Error::ArrayTooLong => write!(f, "Array too long"),
            Error::GenericError(ref s) => fmt::Display::fmt(s, f),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::SerializeError(ref _s) => None,
            Error::ReadError(ref io) => Some(io),
            Error::DeserializeError(ref _s) => None,
            Error::WriteError(ref io) => Some(io),
            Error::UnderflowError(ref _s) => None,
            Error::OverflowError(ref _s) => None,
            Error::ArrayTooLong => None,
            Error::GenericError(ref _s) => None,
        }
    }
}

/// Types implementing a single, universally agreed consensus.
/// Helps to make serialization and deserialization a single pipeline
/// creating a confidence of compatibilty among all types implementing it.
pub trait Codec {
    /// Serialize directly into a writable file descriptor.
    fn consensus_serialize<W: Write>(&self, fd: &mut W) -> Result<(), Error>
    where
        Self: Sized;

    /// Deserialize from a readable file descriptor
    fn consensus_deserialize<R: Read>(fd: &mut R) -> Result<Self, Error>
    where
        Self: Sized;
}

/// Convenience function for the [`Codec`] trait.
/// Just to make implementing the trait a bit more elegant.
pub fn write_next<T: Codec, W: Write>(fd: &mut W, item: &T) -> Result<(), Error> {
    item.consensus_serialize(fd)
}

/// Convenience function for the [`Codec`] trait.
/// Just to make implementing the trait a bit more elegant.
pub fn read_next<T: Codec, R: Read>(fd: &mut R) -> Result<T, Error> {
    let item: T = T::consensus_deserialize(fd)?;
    Ok(item)
}

crate::impl_byte_array_codec!(CryptoHash, 32);
crate::impl_byte_array_codec!(ChainId, 32);

crate::impl_codec_for_int!(u8; [0; 1]);
crate::impl_codec_for_int!(u16; [0; 2]);
crate::impl_codec_for_int!(u32; [0; 4]);
crate::impl_codec_for_int!(u64; [0; 8]);
crate::impl_codec_for_int!(i64; [0; 8]);

crate::impl_codec_for_int_wrapper!(BlockHeight);

/// Helper macro for redundant generic implementations of the [`Codec`] trait
/// for primitive integer types.
#[macro_export]
macro_rules! impl_codec_for_int {
    ($typ:ty; $array:expr) => {
        impl $crate::codec::Codec for $typ {
            fn consensus_serialize<W: Write>(
                &self,
                fd: &mut W,
            ) -> Result<(), $crate::codec::Error> {
                fd.write_all(&self.to_be_bytes())
                    .map_err($crate::codec::Error::WriteError)
            }

            fn consensus_deserialize<R: Read>(fd: &mut R) -> Result<Self, $crate::codec::Error> {
                let mut buf = $array;
                fd.read_exact(&mut buf)
                    .map_err($crate::codec::Error::ReadError)?;
                Ok(<$typ>::from_be_bytes(buf))
            }
        }
    };
}

/// Helper macro for redundant generic implementations of the [`Codec`] trait
/// for types encompassing an array of bytes of variable lengths.
#[macro_export]
macro_rules! impl_byte_array_codec {
    ($thing:ident, $len:expr) => {
        impl $crate::codec::Codec for $thing {
            fn consensus_serialize<W: std::io::Write>(
                &self,
                fd: &mut W,
            ) -> Result<(), $crate::codec::Error> {
                fd.write_all(self.as_bytes())
                    .map_err($crate::codec::Error::WriteError)
            }

            fn consensus_deserialize<R: std::io::Read>(
                fd: &mut R,
            ) -> Result<$thing, $crate::codec::Error> {
                let mut buf = [0u8; ($len as usize)];
                fd.read_exact(&mut buf)
                    .map_err($crate::codec::Error::ReadError)?;
                let ret = $thing::try_from(&buf as &[u8])
                    .map_err(|e| $crate::codec::Error::UnderflowError(e.to_string()))?;
                Ok(ret)
            }
        }
    };
}

/// Helper macro for redundant generic implementations of the [`Codec`] trait
/// for types wrapping a primitive integer type.
#[macro_export]
macro_rules! impl_codec_for_int_wrapper {
    ($thing:ident) => {
        impl $crate::codec::Codec for $thing {
            fn consensus_serialize<W: std::io::Write>(
                &self,
                fd: &mut W,
            ) -> Result<(), $crate::codec::Error> {
                write_next(fd, &self.0)?;
                Ok(())
            }

            fn consensus_deserialize<R: std::io::Read>(
                fd: &mut R,
            ) -> Result<$thing, $crate::codec::Error> {
                Ok(Self(read_next(fd)?))
            }
        }
    };
}

