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

    /// The set of tasks spawned on the current thread in a Web environment.
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
        ///
        /// Unlike its native counterpart this cannot re-raise a panic: on the Web a task's
        /// panic aborts the whole Wasm instance, so there is nothing left to re-raise.
        fn await_all_tasks(&mut self) -> impl Future<Output = ()>;

        /// Awaits all tasks spawned in this [`JoinSet`].
        ///
        /// Identical to [`JoinSetExt::await_all_tasks`] here; the two differ only on
        /// native targets, where a task's panic does not stop the process.
        fn await_all_tasks_logging_panics(&mut self) -> impl Future<Output = ()>;

        /// Reaps tasks that have finished.
        fn reap_finished_tasks(&mut self);
    }

    impl JoinSetExt for JoinSet {
        fn spawn_task<F: Future + 'static>(&mut self, future: F) -> TaskHandle<F::Output> {
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let (send_done, recv_done) = oneshot::channel();
            let (send_output, recv_output) = oneshot::channel();
            let future = async move {
                // Receiver may have been dropped if the task was aborted.
                send_output.send(future.await).ok();
                send_done.send(()).ok();
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

        async fn await_all_tasks_logging_panics(&mut self) {
            self.await_all_tasks().await
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

    /// The set of tasks spawned on the Tokio runtime.
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

        /// Awaits all tasks spawned in this [`JoinSet`], re-raising the panic of any task
        /// that panicked.
        ///
        /// Tokio catches a task's panic instead of stopping the runtime, so a set of
        /// long-lived tasks that is merely drained leaves the process running with one of
        /// its subsystems silently gone. Re-raising turns that into an exit, which a
        /// supervisor can act on.
        ///
        /// For sets of tasks that each serve a single request or connection, where losing
        /// one is recoverable and exiting would let one caller stop the process, use
        /// [`JoinSetExt::await_all_tasks_logging_panics`] instead.
        async fn await_all_tasks(&mut self);

        /// Awaits all tasks spawned in this [`JoinSet`], logging rather than re-raising
        /// the panic of any task that panicked.
        async fn await_all_tasks_logging_panics(&mut self);

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
                // Receiver may have been dropped if the task was aborted.
                output_sender.send(future.await).ok();
            });

            TaskHandle {
                output_receiver,
                abort_handle,
            }
        }

        async fn await_all_tasks(&mut self) {
            while let Some(result) = self.join_next().await {
                if let Err(error) = result {
                    match error.try_into_panic() {
                        // Tokio contained the panic, so the process is still running
                        // without whatever this task was doing. Put it back.
                        Ok(payload) => std::panic::resume_unwind(payload),
                        Err(error) => tracing::debug!(%error, "Task was cancelled"),
                    }
                }
            }
        }

        async fn await_all_tasks_logging_panics(&mut self) {
            while let Some(result) = self.join_next().await {
                if let Err(error) = result {
                    if error.is_panic() {
                        tracing::error!(%error, "Task panicked");
                    } else {
                        tracing::debug!(%error, "Task was cancelled");
                    }
                }
            }
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

#[cfg(all(test, not(web)))]
mod tests {
    use futures::future;

    use super::*;

    /// A set of long-lived tasks must not lose one silently: the panic reaches whoever
    /// awaits the set, and from there the process.
    #[tokio::test]
    async fn test_await_all_tasks_reraises_a_panic() {
        let joined = tokio::spawn(async {
            let mut join_set = JoinSet::new();
            join_set.spawn_task(async {
                panic!("task panicked");
            });
            join_set.await_all_tasks().await;
        })
        .await;

        assert!(
            joined.expect_err("the panic reached the caller").is_panic(),
            "await_all_tasks re-raised the task's panic",
        );
    }

    #[tokio::test]
    async fn test_await_all_tasks_logging_panics_keeps_going() {
        let mut join_set = JoinSet::new();
        join_set.spawn_task(async {
            panic!("task panicked");
        });
        join_set.spawn_task(async {});

        join_set.await_all_tasks_logging_panics().await;
    }

    /// Aborting a task is how shutdown works, so it must not be mistaken for a failure.
    #[tokio::test]
    async fn test_await_all_tasks_ignores_cancelled_tasks() {
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn_task(future::pending::<()>());
        handle.abort();

        join_set.await_all_tasks().await;
    }
}
