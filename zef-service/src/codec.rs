use bytes::{Buf, BufMut, BytesMut};
use std::io;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
use zef_base::rpc;

/// An encoder/decoder of [`rpc::Message`]s for the RPC protocol.
///
/// Handles the serialization of RPC messages.
#[derive(Clone, Copy, Debug)]
pub struct Codec;

impl Encoder<rpc::Message> for Codec {
    type Error = Error;

    fn encode(&mut self, message: rpc::Message, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        bincode::serialize_into(&mut buffer.writer(), &message)
            .map_err(|error| Error::Serialization(*error))
    }
}

impl Decoder for Codec {
    type Item = rpc::Message;
    type Error = Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match bincode::deserialize_from(buffer.reader()) {
            Ok(message) => Ok(Some(message)),
            Err(boxed_error) => match *boxed_error {
                bincode::ErrorKind::Io(io_error)
                    if io_error.kind() == io::ErrorKind::UnexpectedEof =>
                {
                    Ok(None)
                }
                bincode::ErrorKind::Io(io_error) => Err(Error::Io(io_error)),
                error => Err(Error::Deserialization(error)),
            },
        }
    }
}

/// Errors that can arise during transmission or reception of [`rpc::Message`]s.
#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error in the underlying transport")]
    Io(#[from] io::Error),

    #[error("Failed to deserialize an incoming message")]
    Deserialization(#[source] bincode::ErrorKind),

    #[error("Failed to serialize outgoing message")]
    Serialization(#[source] bincode::ErrorKind),
}
