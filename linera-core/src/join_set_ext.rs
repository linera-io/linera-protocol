// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An extension trait to allow determining at compile time how tasks are spawned on the Tokio
//! runtime.
//!
//! In most cases the [`Future`] task to be spawned should implement [`Send`], but that's
//! not possible when compiling for `wasm32-unknown-unknown`. In that case, the task is
//! spawned in a [`LocalSet`][`tokio::tast::LocalSet`].

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};

/// An extension trait for the [`JoinSet`] type.
#[cfg(not(web))]
pub trait JoinSetExt: Sized {
    /// Spawns a `future` task on this [`JoinSet`] using [`JoinSet::spawn`].
    ///
    /// Returns a [`oneshot::Receiver`] to receive the `future`'s output, and an
    /// [`AbortHandle`] to cancel execution of the task.
    fn spawn_task<F>(&mut self, future: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send;

    /// Awaits all tasks spawned in this [`JoinSet`].
    fn await_all_tasks(&mut self) -> impl Future<Output = ()> + Send;

    /// Reaps tasks that have finished.
    fn reap_finished_tasks(&mut self);
}

/// An extension trait for the [`JoinSet`] type.
#[cfg(web)]
pub trait JoinSetExt: Sized {
    /// Spawns a `future` task on this [`JoinSet`] using [`JoinSet::spawn_local`].
    ///
    /// Returns a [`oneshot::Receiver`] to receive the `future`'s output, and an
    /// [`AbortHandle`] to cancel execution of the task.
    fn spawn_task<F>(&mut self, future: F) -> TaskHandle<F::Output>
    where
        F: Future + 'static;

    /// Awaits all tasks spawned in this [`JoinSet`].
    fn await_all_tasks(&mut self) -> impl Future<Output = ()>;

    /// Reaps tasks that have finished.
    fn reap_finished_tasks(&mut self);
}

#[cfg(not(web))]
impl JoinSetExt for JoinSet<()> {
    fn spawn_task<F>(&mut self, future: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let (output_sender, output_receiver) = oneshot::channel();

        let abort_handle = self.spawn(async move {
            let _ = output_sender.send(future.await);
        });

        TaskHandle {
            output_receiver,
            abort_handle,
        }
    }

    async fn await_all_tasks(&mut self) {
        while self.join_next().await.is_some() {}
    }

    fn reap_finished_tasks(&mut self) {
        while self.try_join_next().is_some() {}
    }
}

#[cfg(web)]
impl JoinSetExt for JoinSet<()> {
    fn spawn_task<F>(&mut self, future: F) -> TaskHandle<F::Output>
    where
        F: Future + 'static,
    {
        let (output_sender, output_receiver) = oneshot::channel();

        let abort_handle = self.spawn_local(async move {
            let _ = output_sender.send(future.await);
        });

        TaskHandle {
            output_receiver,
            abort_handle,
        }
    }

    async fn await_all_tasks(&mut self) {
        while self.join_next().await.is_some() {}
    }

    fn reap_finished_tasks(&mut self) {
        while self.try_join_next().is_some() {}
    }
}

/// A handle to a task spawned with [`JoinSetExt`].
///
/// Dropping a handle aborts its respective task.
pub struct TaskHandle<Output> {
    output_receiver: oneshot::Receiver<Output>,
    abort_handle: AbortHandle,
}

impl<Output> Future for TaskHandle<Output> {
    type Output = Result<Output, oneshot::error::RecvError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut().output_receiver.poll_unpin(context)
    }
}

impl<Output> TaskHandle<Output> {
    /// Aborts the task.
    pub fn abort(&self) {
        self.abort_handle.abort();
    }

    /// Returns [`true`] if the task is still running.
    pub fn is_running(&mut self) -> bool {
        matches!(
            self.output_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        )
    }
}
