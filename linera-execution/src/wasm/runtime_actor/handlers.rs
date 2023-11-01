// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of how requests should be handled inside a [`RuntimeActor`].

use crate::ExecutionError;
use async_trait::async_trait;

/// A type that is able to handle incoming `Request`s.
#[async_trait]
pub trait RequestHandler<Request> {
    /// Handles a `Request`.
    ///
    /// Returns an error if the request could not be handled and no further requests should be sent
    /// to this handler.
    async fn handle_request(&self, request: Request) -> Result<(), ExecutionError>;
}

/// Helper trait to send a response and log on failure.
trait RespondExt {
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
