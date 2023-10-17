// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to handle async code between the host WebAssembly runtime and guest WebAssembly
//! modules.

use super::{
    async_determinism::HostFutureQueue,
    common::{ApplicationRuntimeContext, WasmRuntimeContext},
    ExecutionError, WasmExecutionError,
};
use futures::{channel::oneshot, ready, stream::StreamExt, FutureExt};
use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

/// An actor that runs a future implemented in a Wasm module.
///
/// The actor should run in its own separate thread, where it will block until it receives poll
/// requests from a [`PollSender`].
pub struct GuestFutureActor<Future, Application>
where
    Application: ApplicationRuntimeContext,
    Future: GuestFutureInterface<Application>,
{
    future: Future,
    context: WasmRuntimeContext<Application>,
    poll_request_receiver: std::sync::mpsc::Receiver<PollRequest<Future::Output>>,
}

impl<Future, Application> GuestFutureActor<Future, Application>
where
    Application: ApplicationRuntimeContext + Send + 'static,
    Future: GuestFutureInterface<Application> + Send + 'static,
    Future::Output: Send,
{
    /// Spawns a new thread and runs the `future` in a [`GuestFutureActor`].
    ///
    /// Returns the [`PollSender`] which can be used in an asynchronous context to `await` the
    /// result.
    pub fn spawn(
        future: Future,
        mut context: WasmRuntimeContext<Application>,
    ) -> PollSender<Future::Output> {
        let (dummy_future_queue, _) = HostFutureQueue::new();
        let host_future_queue = mem::replace(&mut context.future_queue, dummy_future_queue);

        let (poll_request_sender, poll_request_receiver) = std::sync::mpsc::channel();
        let poll_sender = PollSender::new(host_future_queue, poll_request_sender);
        let actor = Self::new(future, context, poll_request_receiver);

        thread::spawn(|| actor.run());

        poll_sender
    }

    /// Creates a new [`GuestFutureActor`] to run `future` using a Wasm runtime `context`.
    pub fn new(
        future: Future,
        context: WasmRuntimeContext<Application>,
        poll_request_receiver: std::sync::mpsc::Receiver<PollRequest<Future::Output>>,
    ) -> Self {
        GuestFutureActor {
            future,
            context,
            poll_request_receiver,
        }
    }

    /// Executes the future, polling it as requested by the [`PollSender`] until it completes.
    pub fn run(mut self) {
        while let Ok(request) = self.poll_request_receiver.recv() {
            let response = self
                .future
                .poll(&self.context.application, &mut self.context.store);
            let mut finished = response.is_ready();

            if request
                .response_sender
                .send(PollResponse(response))
                .is_err()
            {
                tracing::debug!("Wasm guest future cancelled");
                finished = true;
            }

            if finished {
                break;
            }
        }
    }
}

/// A message type sent to request the actor to poll the guest Wasm future.
pub struct PollRequest<Output> {
    response_sender: oneshot::Sender<PollResponse<Output>>,
}

/// A wrapper type representing the response sent from a future actor to a poll request sent from a
/// [`PollSender`].
///
/// This helps to simplify some types used, avoiding some Clippy lints.
struct PollResponse<Output>(Poll<Result<Output, ExecutionError>>);

/// An abstraction over a [`Future`] running as an actor on a separate thread.
///
/// This type implements [`Future`] and sends poll requests to the actor implementation. When the
/// actor finishes, it sends back the result to this type, which then returns it.
///
/// Poll requests may not be sent to the implementation if it would cause non-deterministic
/// execution (as controlled by the [`HostFutureQueue`]).
pub struct PollSender<Output> {
    host_future_queue: HostFutureQueue<'static>,
    poll_request_sender: std::sync::mpsc::Sender<PollRequest<Output>>,
    state: PollSenderState<Output>,
}

/// The internal state of the [`PollSender`] type.
#[derive(Debug)]
enum PollSenderState<Output> {
    /// Waiting to be polled.
    Queued,

    /// Waiting for response to the previous poll request.
    Polling(oneshot::Receiver<PollResponse<Output>>),

    /// Result received and returned.
    Finished,
}

impl<Output> PollSender<Output> {
    /// Creates a new [`PollSender`] using the provided [`HostFutureQueue`] to ensure deterministic
    /// polling.
    ///
    /// Returns the new [`PollSender`] together with the receiver endpoint of the poll requests.
    fn new(
        host_future_queue: HostFutureQueue<'static>,
        poll_request_sender: std::sync::mpsc::Sender<PollRequest<Output>>,
    ) -> Self {
        PollSender {
            host_future_queue,
            poll_request_sender,
            state: PollSenderState::Queued,
        }
    }

    /// Sends a poll request if allowed by the [`HostFutureQueue`].
    fn poll_start(&mut self, context: &mut Context) -> Poll<()> {
        ready!(self.host_future_queue.poll_next_unpin(context));

        let (response_sender, response_receiver) = oneshot::channel();
        let _ = self
            .poll_request_sender
            .send(PollRequest { response_sender });
        // If sending fails, `poll_response` will handle the `Err(oneshot::Canceled)` when it
        // attempts to receive the response
        self.state = PollSenderState::Polling(response_receiver);

        Poll::Ready(())
    }

    /// Checks if a response to the last previous request has been received.
    ///
    /// If a response has been received, the state is updated based on if the response is that the
    /// result is ready or that it's still pending.
    fn poll_response(
        &mut self,
        context: &mut Context,
    ) -> Option<Poll<Result<Output, ExecutionError>>> {
        let PollSenderState::Polling(receiver) = &mut self.state else {
            panic!("`poll_response` called without being in a `PollSenderState::Polling` state");
        };

        match receiver.poll_unpin(context) {
            Poll::Ready(Ok(PollResponse(Poll::Ready(response)))) => {
                self.state = PollSenderState::Finished;
                Some(Poll::Ready(response))
            }
            Poll::Ready(Ok(PollResponse(Poll::Pending))) => {
                self.state = PollSenderState::Queued;
                None
            }
            Poll::Ready(Err(oneshot::Canceled)) => {
                tracing::debug!("Wasm guest is no longer reachable");
                self.state = PollSenderState::Finished;
                Some(Poll::Ready(Err(WasmExecutionError::Aborted.into())))
            }
            Poll::Pending => Some(Poll::Pending),
        }
    }
}

impl<Output> Future for PollSender<Output> {
    type Output = Result<Output, ExecutionError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut this = self.as_mut();

        loop {
            match this.state {
                PollSenderState::Queued => ready!(this.poll_start(context)),
                PollSenderState::Polling(_) => {
                    if let Some(return_value) = this.poll_response(context) {
                        break return_value;
                    }
                }
                PollSenderState::Finished => panic!("Future polled after it has finished"),
            }
        }
    }
}

/// Interface to poll a future implemented in a Wasm module.
pub trait GuestFutureInterface<Application>
where
    Application: ApplicationRuntimeContext,
{
    /// The output of the guest future.
    type Output;

    /// Polls the guest future to attempt to progress it.
    ///
    /// May return an [`ExecutionError`] if the guest Wasm module panics, for example.
    fn poll(
        &self,
        application: &Application,
        store: &mut Application::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>>;
}
