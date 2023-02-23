// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to enforce determinism on asynchronous code called from a guest WASM module.
//!
//! To ensure that asynchronous calls from a guest WASM module are deterministic, the following
//! rules are enforced:
//!
//! - Futures are completed in the exact same order that they were created;
//! - The guest WASM module is only polled when the next future to be completed has finished;
//! - Every time the guest WASM module is polled, exactly one future will return [`Poll::Ready`];
//! - All other futures will return [`Poll::Pending`].
//!
//! To enforce these rules, the futures have to be polled separately from the guest WASM module.
//! The traditional asynchronous behavior is for the host to poll the guest, and for the guest to
//! poll the host futures again. This is problematic because the number of times the host futures
//! need to be polled might not be deterministic. So even if the futures are made to finish
//! sequentially, the number of times the guest is polled would not be deterministic.
//!
//! For the guest to be polled separately from the host futures it calls, two types are used:
//! [`HostFutureQueue`] and [`QueuedHostFutureFactory`]. The [`QueuedHostFutureFactory`] is what is
//! used by the guest WASM module handle to enqueue futures for deterministic execution (i.e.,
//! normally stored in the application's exported API handler). For every future that's enqueued, a
//! [`HostFuture`] is returned that contains only a [`oneshot::Receiver`] for the future's result.
//! The future itself is actually sent to the [`HostFutureQueue`] to be polled separately from the
//! guest.
//!
//! The [`HostFutureQueue`] implements [`Stream`], and produces a marker `()` item every time the
//! next future in the queue is ready for completion. Therefore, the [`GuestFuture`] is responsible
//! for always polling the [`HostFutureQueue`] before polling the guest WASM module.

use super::async_boundary::HostFuture;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, BoxFuture, FutureExt},
    sink::SinkExt,
    stream::{FuturesOrdered, Stream, StreamExt},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A queue of host futures called by a WASM guest module that finish in the same order they were
/// created.
///
/// Futures are added to the queue through the [`QueuedHostFutureFactory`] associated to the
/// [`HostFutureQueue`]. The futures are executed (polled) when the [`HostFutureQueue`] is polled
/// (as a [`Stream`]).
///
/// [`QueuedHostFutureFactory`] wraps the future before sending it to [`HostFutureQueue`] so that
/// it returns a closure that sends the future's output to the corresponding [`HostFuture`]. The
/// [`HostFutureQueue`] runs that closure when it's time to complete the [`HostFuture`], ensuring
/// that only one is completed after each item produced by the [`HostFutureQueue`]'s implementation
/// of [`Stream`].
pub struct HostFutureQueue<'futures> {
    new_futures: mpsc::Receiver<BoxFuture<'futures, Box<dyn FnOnce() + Send>>>,
    queue: FuturesOrdered<BoxFuture<'futures, Box<dyn FnOnce() + Send>>>,
}

impl<'futures> HostFutureQueue<'futures> {
    /// Creates a new [`HostFutureQueue`] and its associated [`QueuedHostFutureFactory`].
    ///
    /// An initial empty future is added to the queue so that the first time the queue is polled it
    /// returns an item, allowing the guest WASM module to be polled for the first time.
    pub fn new() -> (Self, QueuedHostFutureFactory<'futures>) {
        let (sender, receiver) = mpsc::channel(25);

        let empty_completion: Box<dyn FnOnce() + Send> = Box::new(|| ());
        let initial_future = future::ready(empty_completion).boxed();

        (
            HostFutureQueue {
                new_futures: receiver,
                queue: FuturesOrdered::from_iter([initial_future]),
            },
            QueuedHostFutureFactory { sender },
        )
    }

    /// Polls the futures in the queue.
    ///
    /// Returns `true` if the next future in the queue has completed.
    ///
    /// If the next future has completed, its returned closure is executed in order to send the
    /// future's result to its associated [`HostFuture`].
    fn poll_futures(&mut self, context: &mut Context<'_>) -> bool {
        match self.queue.poll_next_unpin(context) {
            Poll::Ready(Some(future_completion)) => {
                future_completion();
                true
            }
            Poll::Ready(None) => false,
            Poll::Pending => false,
        }
    }

    /// Polls the [`mpsc::Receiver`] of futures to add to the queue.
    ///
    /// Returns true if the [`mpsc::Sender`] endpoint has been closed.
    fn poll_incoming(&mut self, context: &mut Context<'_>) -> bool {
        match self.new_futures.poll_next_unpin(context) {
            Poll::Pending => false,
            Poll::Ready(Some(new_future)) => {
                self.queue.push_back(new_future);
                false
            }
            Poll::Ready(None) => true,
        }
    }
}

impl<'futures> Stream for HostFutureQueue<'futures> {
    type Item = ();

    /// Polls the [`HostFutureQueue`], producing a `()` item if a future was completed.
    ///
    /// First the incoming channel of futures is polled, in order to add any newly created futures
    /// to the queue. Then the futures are polled.
    ///
    /// # Note on [`Poll::Pending`]
    ///
    /// This function returns [`Poll::Pending`] correctly, because it's only returned if either:
    ///
    /// - No new futures were received (the [`mpsc::Receiver`] returned [`Poll::Pending`]) and the
    ///   queue is empty, which means that this task will receive a wakeup when a new future is
    ///   received;
    /// - No queued future was completed (the [`FuturesOrdered`] returned [`Poll::Pending`]), which
    ///   which means that all futures in the queue have scheduled wakeups for this task;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let incoming_closed = self.poll_incoming(context);

        if incoming_closed && self.queue.is_empty() {
            return Poll::Ready(None);
        }

        if self.poll_futures(context) {
            Poll::Ready(Some(()))
        } else {
            Poll::Pending
        }
    }
}

/// A factory of [`HostFuture`]s that enforces determinism of the host futures they represent.
///
/// This type is created by [`HostFutureQueue::new`], and is associated to the [`HostFutureQueue`]
/// returned with it. Both must be used together in the correct manner as described by the module
/// documentation. The summary is that the [`HostFutureQueue`] should be polled until it returns an
/// item before the guest WASM module is polled, so that the created [`HostFuture`]s are only polled
/// deterministically.
#[derive(Clone)]
pub struct QueuedHostFutureFactory<'futures> {
    sender: mpsc::Sender<BoxFuture<'futures, Box<dyn FnOnce() + Send>>>,
}

impl<'futures> QueuedHostFutureFactory<'futures> {
    /// Enqueues a `future` in the associated [`HostFutureQueue`].
    ///
    /// Returns a [`HostFuture`] that can be passed to the guest WASM module, and that will only be
    /// ready when the inner `future` is ready and all previous futures added to the queue are
    /// ready.
    ///
    /// The `future` itself is only executed when the associated [`HostFutureQueue`] is polled.
    /// When the `future` is complete, the result is paired inside a closure with a
    /// [`oneshot::Sender`] that's connected to the [`oneshot::Receiver`] inside the returned
    /// [`HostFuture`]. The [`HostFutureQueue`] runs the closure when it's time to complete the
    /// [`HostFuture`].
    ///
    /// # Panics
    ///
    /// The returned [`HostFuture`] may panic if it is polled after the [`HostFutureQueue`] is
    /// dropped.
    pub fn enqueue<Output>(
        &mut self,
        future: impl Future<Output = Output> + Send + 'futures,
    ) -> HostFuture<'futures, Output>
    where
        Output: Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();
        let mut future_sender = self.sender.clone();

        HostFuture::new(async move {
            future_sender
                .send(
                    future
                        .map(move |result| -> Box<dyn FnOnce() + Send> {
                            Box::new(move || {
                                // An error when sending the result indicates that the user
                                // application dropped the `HostFuture`, and no longer needs the
                                // result
                                let _ = result_sender.send(result);
                            })
                        })
                        .boxed(),
                )
                .await
                .expect(
                    "`HostFutureQueue` should not be dropped while `QueuedHostFutureFactory` is \
                    still enqueuing futures",
                );

            result_receiver.await.expect(
                "`HostFutureQueue` should not be dropped while the `HostFuture`s of the queued \
                futures are still alive",
            )
        })
    }
}
