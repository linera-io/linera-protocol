// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An extension trait to allow determining at compile time how tasks are spawned on the Tokio
//! runtime.
//!
//! In most cases the [`Future`] task to be spawned should implement [`Send`], but that's
//! not possible when compiling for the Web. In that case, the task is spawned on the
//! browser event loop.

use futures::channel::oneshot;

#[cfg(web)]
mod implementation {
    pub use futures::future::AbortHandle;
    use futures::{future, stream, StreamExt as _};

    use super::*;

    #[derive(Default)]
    pub struct JoinSet(Vec<oneshot::Receiver<()>>);

    /// An extension trait for the [`JoinSet`] type.
    pub trait JoinSetExt: Sized {
        /// Spawns a `future` task on this [`JoinSet`] using [`JoinSet::spawn_local`].
        ///
        /// Returns a [`oneshot::Receiver`] to receive the `future`'s output, and an
        /// [`AbortHandle`] to cancel execution of the task.
        fn spawn_task<F: Future + 'static>(&mut self, future: F) -> TaskHandle<F::Output>;

        /// Awaits all tasks spawned in this [`JoinSet`].
        fn await_all_tasks(&mut self) -> impl Future<Output = ()>;

        /// Reaps tasks that have finished.
        fn reap_finished_tasks(&mut self);
    }

    impl JoinSetExt for JoinSet {
        fn spawn_task<F: Future + 'static>(&mut self, future: F) -> TaskHandle<F::Output> {
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let (send_done, recv_done) = oneshot::channel();
            let (send_output, recv_output) = oneshot::channel();
            let future = async move {
                let _ = send_output.send(future.await);
                let _ = send_done.send(());
            };
            self.0.push(recv_done);
            wasm_bindgen_futures::spawn_local(
                future::Abortable::new(future, abort_registration).map(drop),
            );

            TaskHandle {
                output_receiver: recv_output,
                abort_handle,
            }
        }

        async fn await_all_tasks(&mut self) {
            stream::iter(&mut self.0)
                .then(|x| x)
                .map(drop)
                .collect()
                .await
        }

        fn reap_finished_tasks(&mut self) {
            self.0.retain_mut(|task| task.try_recv() == Ok(None));
        }
    }
}

#[cfg(not(web))]
mod implementation {
    pub use tokio::task::AbortHandle;

    use super::*;

    pub type JoinSet = tokio::task::JoinSet<()>;

    /// An extension trait for the [`JoinSet`] type.
    #[trait_variant::make(Send)]
    pub trait JoinSetExt: Sized {
        /// Spawns a `future` task on this [`JoinSet`] using [`JoinSet::spawn`].
        ///
        /// Returns a [`oneshot::Receiver`] to receive the `future`'s output, and an
        /// [`AbortHandle`] to cancel execution of the task.
        fn spawn_task<F: Future<Output: Send> + Send + 'static>(
            &mut self,
            future: F,
        ) -> TaskHandle<F::Output>;

        /// Awaits all tasks spawned in this [`JoinSet`].
        async fn await_all_tasks(&mut self);

        /// Reaps tasks that have finished.
        fn reap_finished_tasks(&mut self);
    }

    impl JoinSetExt for JoinSet {
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
}

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt as _;
pub use implementation::*;

/// A handle to a task spawned with [`JoinSetExt`].
///
/// Dropping a handle detaches its respective task.
pub struct TaskHandle<Output> {
    output_receiver: oneshot::Receiver<Output>,
    abort_handle: AbortHandle,
}

impl<Output> Future for TaskHandle<Output> {
    type Output = Result<Output, oneshot::Canceled>;

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
        self.output_receiver.try_recv().is_err()
    }
}
