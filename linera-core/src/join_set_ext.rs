// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An extension trait to allow determining at compile time how tasks are spawned on the Tokio
//! runtime.
//!
//! In most cases the [`Future`] task to be spawned should implement [`Send`], but that's
//! not possible when compiling for `wasm32-unknown-unknown`. In that case, the task is
//! spawned in a [`LocalSet`][`tokio::tast::LocalSet`].

use std::future::Future;

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
    fn spawn_task<F>(&mut self, future: F) -> (oneshot::Receiver<F::Output>, AbortHandle)
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
    fn spawn_task<F>(&mut self, future: F) -> (oneshot::Receiver<F::Output>, AbortHandle)
    where
        F: Future + 'static;

    /// Awaits all tasks spawned in this [`JoinSet`].
    fn await_all_tasks(&mut self) -> impl Future<Output = ()>;

    /// Reaps tasks that have finished.
    fn reap_finished_tasks(&mut self);
}

#[cfg(not(web))]
impl JoinSetExt for JoinSet<()> {
    fn spawn_task<F>(&mut self, future: F) -> (oneshot::Receiver<F::Output>, AbortHandle)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let (output_sender, output_receiver) = oneshot::channel();

        let abort_handle = self.spawn(async move {
            let _ = output_sender.send(future.await);
        });

        (output_receiver, abort_handle)
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
    fn spawn_task<F>(&mut self, future: F) -> (oneshot::Receiver<F::Output>, AbortHandle)
    where
        F: Future + 'static,
    {
        let (output_sender, output_receiver) = oneshot::channel();

        let abort_handle = self.spawn_local(async move {
            let _ = output_sender.send(future.await);
        });

        (output_receiver, abort_handle)
    }

    async fn await_all_tasks(&mut self) {
        while self.join_next().await.is_some() {}
    }

    fn reap_finished_tasks(&mut self) {
        while self.try_join_next().is_some() {}
    }
}
