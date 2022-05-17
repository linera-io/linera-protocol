use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use zef_base::serialize::SerializedMessage;

#[derive(Clone, Copy, Debug)]
pub struct Codec;

impl Encoder<SerializedMessage> for Codec {
    type Error = Box<bincode::ErrorKind>;

    fn encode(
        &mut self,
        message: SerializedMessage,
        buffer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        bincode::serialize_into(&mut buffer.writer(), &message)
    }
}

impl Decoder for Codec {
    type Item = SerializedMessage;
    type Error = Box<bincode::ErrorKind>;

    fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(bincode::deserialize_from(buffer.reader()).ok())
    }
}
