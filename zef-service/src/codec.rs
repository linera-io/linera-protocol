use bytes::{Buf, BufMut, BytesMut};
use std::io;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
use zef_base::serialize::SerializedMessage;

#[derive(Clone, Copy, Debug)]
pub struct Codec;

impl Encoder<SerializedMessage> for Codec {
    type Error = Error;

    fn encode(
        &mut self,
        message: SerializedMessage,
        buffer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        bincode::serialize_into(&mut buffer.writer(), &message)
            .map_err(|error| Error::Serialization(*error))
    }
}

impl Decoder for Codec {
    type Item = SerializedMessage;
    type Error = Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(bincode::deserialize_from(buffer.reader()).ok())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error in the underlying transport")]
    Io(#[from] io::Error),

    #[error("Failed to deserialize an incoming message")]
    Deserialization(#[source] bincode::ErrorKind),

    #[error("Failed to serialize outgoing message")]
    Serialization(#[source] bincode::ErrorKind),
}
