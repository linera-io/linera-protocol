// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types useful for sending synchronous responses from a [`RuntimeActor`]

// TODO(#1416)
#[allow(dead_code)]
/// Creates a channel that wraps a [`oneshot`] channel with the [`Sender`] type not
/// implementing [`std::future::Future`].
///
/// This forces the channel to be used in a blocking manner.
pub fn channel<T>() -> (SyncSender<T>, SyncReceiver<T>) {
    let (sender, receiver) = oneshot::channel();

    (SyncSender(sender), SyncReceiver(receiver))
}

/// A wrapper around [`oneshot::Sender`] that is connected to a synchronous [`SyncReceiver`].
pub struct SyncSender<T>(oneshot::Sender<T>);

impl<T> SyncSender<T> {
    /// Sends a `message` to the synchronous [`SyncReceiver`] endpoint.
    pub fn send(self, message: T) -> Result<(), oneshot::SendError<T>> {
        self.0.send(message)
    }
}

// TODO(#1416)
#[allow(dead_code)]
/// A wrapper around [`oneshot::Receiver`] that is connected to a synchronous [`SyncSender`].
///
/// This type does not implement [`std::future::Future`], so it can't be used to receive
/// messages asynchronously.
pub struct SyncReceiver<T>(oneshot::Receiver<T>);

// TODO(#1416)
#[allow(dead_code)]
impl<T> SyncReceiver<T> {
    /// Blocks until a message from the [`SyncSender`] endpoint is received.
    pub fn recv(self) -> Result<T, oneshot::RecvError> {
        self.0.recv()
    }
}
