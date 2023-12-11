// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor implementation to handle a user application runtime.

mod handlers;
mod requests;
pub mod senders;
mod sync_response;

use self::handlers::RequestHandler;
pub use self::{
    requests::{BaseRequest, ContractRequest, ServiceRequest},
    senders::{ContractRuntimeSender, ServiceRuntimeSender},
    sync_response::{SyncReceiver, SyncSender},
};
use crate::ExecutionError;
use futures::{
    channel::mpsc,
    stream::{StreamExt, TryStreamExt},
};

/// A handler of application system APIs that runs as a separate actor.
///
/// Receives `Request`s from the application and handles them using the `Runtime`.
pub struct RuntimeActor<Runtime, Request> {
    runtime: Runtime,
    requests: mpsc::UnboundedReceiver<Request>,
}

impl<Runtime, Request> RuntimeActor<Runtime, Request> {
    /// Creates a new [`RuntimeActor`] using the provided `Runtime` to handle `Request`s.
    pub fn new(runtime: Runtime, requests: mpsc::UnboundedReceiver<Request>) -> Self {
        Self { runtime, requests }
    }
}

impl<Runtime, Request> RuntimeActor<Runtime, Request>
where
    Runtime: RequestHandler<Request>,
    Request: std::fmt::Debug,
{
    /// Runs the [`RuntimeActor`], handling `Request`s until all the sender endpoints are closed.
    pub async fn run(self) -> Result<(), ExecutionError> {
        let runtime = self.runtime;

        self.requests
            .map(Ok)
            .try_for_each_concurrent(None, |request| runtime.handle_request(request))
            .await
    }
}

/// Extension trait to help with sending requests to [`RuntimeActor`]s.
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

#[cfg(test)]
mod tests;
