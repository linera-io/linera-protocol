// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper traits and functions.

mod sync_response;

use futures::channel::mpsc;

pub use self::sync_response::SyncSender;
use crate::ExecutionError;

/// Extension trait to help with sending requests to an actor.
///
/// Prepares a channel for the actor to send a response back to the sender of the request.
pub trait UnboundedSenderExt<Request> {
    /// Sends a request built by `builder`, returning a [`oneshot::Receiver`] for receiving the
    /// `Response`.
    fn send_request<Response>(
        &self,
        builder: impl FnOnce(oneshot::Sender<Response>) -> Request,
    ) -> Result<oneshot::Receiver<Response>, ExecutionError>
    where
        Response: Send;

    // TODO(#1416)
    #[allow(dead_code)]
    /// Sends a synchronous request built by `builder`, blocking until the `Response` is received.
    fn send_sync_request<Response>(
        &self,
        builder: impl FnOnce(SyncSender<Response>) -> Request,
    ) -> Result<Response, ExecutionError>
    where
        Response: Send;
}

impl<Request> UnboundedSenderExt<Request> for mpsc::UnboundedSender<Request>
where
    Request: Send,
{
    fn send_request<Response>(
        &self,
        builder: impl FnOnce(oneshot::Sender<Response>) -> Request,
    ) -> Result<oneshot::Receiver<Response>, ExecutionError>
    where
        Response: Send,
    {
        let (response_sender, response_receiver) = oneshot::channel();
        let request = builder(response_sender);

        self.unbounded_send(request).map_err(|send_error| {
            assert!(
                send_error.is_disconnected(),
                "`send_request` should only be used with unbounded senders"
            );
            ExecutionError::MissingRuntimeResponse
        })?;

        Ok(response_receiver)
    }

    // TODO(#1416)
    #[allow(dead_code)]
    fn send_sync_request<Response>(
        &self,
        builder: impl FnOnce(SyncSender<Response>) -> Request,
    ) -> Result<Response, ExecutionError>
    where
        Response: Send,
    {
        let (response_sender, response_receiver) = sync_response::channel();
        let request = builder(response_sender);

        self.unbounded_send(request).map_err(|send_error| {
            assert!(
                send_error.is_disconnected(),
                "`send_request` should only be used with unbounded senders"
            );
            ExecutionError::MissingRuntimeResponse
        })?;

        response_receiver
            .recv()
            .map_err(|_| ExecutionError::MissingRuntimeResponse)
    }
}

/// Extension trait to help with receiving responses with a [`oneshot::Receiver`].
pub trait ReceiverExt<T> {
    /// Receives a response `T`, or returns an [`ExecutionError`] if the sender endpoint is closed.
    fn recv_response(self) -> Result<T, ExecutionError>;
}

impl<T> ReceiverExt<T> for oneshot::Receiver<T> {
    fn recv_response(self) -> Result<T, ExecutionError> {
        self.recv()
            .map_err(|oneshot::RecvError| ExecutionError::MissingRuntimeResponse)
    }
}

/// Helper trait to send a response and log on failure.
pub trait RespondExt {
    type Response;

    /// Responds to a request using the `response_sender` channel endpoint.
    fn respond(self, response: Self::Response);
}

impl<Response> RespondExt for oneshot::Sender<Response> {
    type Response = Response;

    fn respond(self, response: Self::Response) {
        if self.send(response).is_err() {
            tracing::debug!("Request sent to `RuntimeActor` was canceled");
        }
    }
}

impl<Response> RespondExt for SyncSender<Response> {
    type Response = Response;

    fn respond(self, response: Self::Response) {
        if self.send(response).is_err() {
            tracing::debug!("Request sent to `RuntimeActor` was canceled");
        }
    }
}
