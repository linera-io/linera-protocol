// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, mem, ops::DerefMut};

use bytes::{Buf, BufMut, BytesMut};
use linera_core::node::NodeError;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use crate::RpcMessage;

/// The size of the frame prefix that contains the payload size.
const PREFIX_SIZE: u8 = mem::size_of::<u32>() as u8;

/// An encoder/decoder of [`RpcMessage`]s for the RPC protocol.
///
/// The frames are length-delimited by a [`u32`] prefix, and the payload is deserialized by
/// [`bincode`].
#[derive(Clone, Copy, Debug)]
pub struct Codec;

impl Encoder<RpcMessage> for Codec {
    type Error = Error;

    fn encode(&mut self, message: RpcMessage, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let mut frame_buffer = buffer.split_off(buffer.len());

        frame_buffer.put_u32_le(0);

        let mut frame_writer = frame_buffer.writer();

        bincode::serialize_into(&mut frame_writer, &message)
            .map_err(|error| Error::Serialization(*error))?;

        let mut frame_buffer = frame_writer.into_inner();
        let frame_size = frame_buffer.len();
        let payload_size = frame_size - PREFIX_SIZE as usize;

        let mut start_of_frame = frame_buffer.deref_mut();

        start_of_frame.put_u32_le(
            payload_size
                .try_into()
                .map_err(|_| Error::MessageTooBig { size: payload_size })?,
        );

        buffer.unsplit(frame_buffer);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = RpcMessage;
    type Error = Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buffer.len() < PREFIX_SIZE.into() {
            return Ok(None);
        }

        let mut start_of_buffer: &[u8] = &*buffer;
        let payload_size = start_of_buffer
            .get_u32_le()
            .try_into()
            .expect("u32 should fit in a usize");

        let frame_size = PREFIX_SIZE as usize + payload_size;

        if buffer.len() < frame_size {
            buffer.reserve(frame_size);
            return Ok(None);
        }

        let _prefix = buffer.split_to(PREFIX_SIZE.into());
        let payload = buffer.split_to(payload_size);

        let message =
            bincode::deserialize(&payload).map_err(|error| Error::Deserialization(*error))?;

        Ok(Some(message))
    }
}

/// Errors that can arise during transmission or reception of [`RpcMessage`]s.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error in the underlying transport: {0}")]
    IoError(#[from] io::Error),

    #[error("Failed to deserialize an incoming message: {0}")]
    Deserialization(#[source] bincode::ErrorKind),

    #[error("Failed to serialize outgoing message: {0}")]
    Serialization(#[source] bincode::ErrorKind),

    #[error("RpcMessage is too big to fit in a protocol frame: \
        message is {size} bytes but can't be larger than {max} bytes.",
        max = u32::MAX)]
    MessageTooBig { size: usize },
}

impl From<Error> for NodeError {
    fn from(error: Error) -> NodeError {
        match error {
            Error::IoError(io_error) => NodeError::ClientIoError {
                error: format!("{}", io_error),
            },
            err => {
                tracing::error!("Unexpected decoding error: {err}");
                NodeError::InvalidDecoding
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use linera_core::data_types::ChainInfoQuery;
    use test_strategy::proptest;
    use tokio_util::codec::{Decoder, Encoder};

    use super::{Codec, RpcMessage, PREFIX_SIZE};

    /// Test decoding of a frame from a buffer.
    ///
    /// The buffer may contain leading or trailing bytes around the frame. The frame contains the
    /// size of the payload, and the payload is a serialized dummy [`RpcMessage`].
    ///
    /// The decoder should produce the exact same message as used as the test input, and it should
    /// ignore the leading and trailing bytes.
    #[proptest]
    fn decodes_frame_ignoring_leading_and_trailing_bytes(
        leading_bytes: Vec<u8>,
        message_contents: ChainInfoQuery,
        trailing_bytes: Vec<u8>,
    ) {
        let message = RpcMessage::ChainInfoQuery(Box::new(message_contents));
        let payload = bincode::serialize(&message).expect("RpcMessage is serializable");

        let mut buffer = BytesMut::with_capacity(
            leading_bytes.len() + PREFIX_SIZE as usize + payload.len() + trailing_bytes.len(),
        );

        buffer.extend_from_slice(&leading_bytes);

        let start_of_buffer = buffer.split();

        buffer.put_u32_le(payload.len() as u32);
        buffer.extend_from_slice(&payload);
        buffer.extend_from_slice(&trailing_bytes);

        let result = Codec.decode(&mut buffer);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(message));

        assert_eq!(&start_of_buffer, &leading_bytes);
        assert_eq!(&buffer, &trailing_bytes);
    }

    /// Test encoding a message to buffer.
    ///
    /// The buffer may already contain some leading bytes, but the cursor is set to where the frame
    /// should start.
    ///
    /// The encoder should write a prefix with the size of the serialized message, followed by the
    /// serialized message bytes. It should not touch the leading bytes nor append any trailing
    /// bytes.
    #[proptest]
    fn encodes_at_the_correct_buffer_offset(
        leading_bytes: Vec<u8>,
        message_contents: ChainInfoQuery,
    ) {
        let message = RpcMessage::ChainInfoQuery(Box::new(message_contents));
        let serialized_message =
            bincode::serialize(&message).expect("Serialization should succeed");

        let mut buffer = BytesMut::new();

        buffer.extend_from_slice(&leading_bytes);

        let frame_start = buffer.len();
        let prefix_end = frame_start + PREFIX_SIZE as usize;

        let result = Codec.encode(message, &mut buffer);

        assert!(matches!(result, Ok(())));
        assert_eq!(&buffer[..frame_start], &leading_bytes);

        let prefix = u32::from_le_bytes(
            buffer[frame_start..prefix_end]
                .try_into()
                .expect("Incorrect prefix slice indices"),
        );

        assert_eq!(prefix as usize, serialized_message.len());
        assert_eq!(
            buffer.len(),
            leading_bytes.len() + PREFIX_SIZE as usize + prefix as usize
        );

        assert_eq!(&buffer[prefix_end..], &serialized_message);
    }
}
