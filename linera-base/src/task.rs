// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over tasks that can be used natively or on the Web.
 */

use futures::{future, Future, FutureExt as _};

/// Spawns `future` on the runtime and awaits its completion.
///
/// Unlike [`Task::spawn`], dropping the returned future does *not* cancel the
/// spawned task — it runs to completion in the background. Use this when the
/// spawned work (e.g. a storage write paired with its in-memory finalization)
/// must not be torn apart mid-flight by caller cancellation.
#[cfg(not(web))]
pub async fn run_detached<F, R>(future: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn(future)
        .await
        .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}

/// Web variant: runs `future` on the event loop via
/// [`wasm_bindgen_futures::spawn_local`], delivering the output via a oneshot.
/// The spawned task is inherently detached from any handle on web.
#[cfg(web)]
pub async fn run_detached<F, R>(future: F) -> R
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    let (tx, rx) = futures::channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let _ = tx.send(future.await);
    });
    rx.await
        .expect("spawned task dropped without sending its result")
}

/// The type of a future awaiting another task.
///
/// On drop, the remote task will be asynchronously cancelled, but will remain
/// alive until it reaches a yield point.
///
/// To wait for the task to be fully cancelled, use [`Task::cancel`].
pub struct Task<R> {
    abort_handle: future::AbortHandle,
    output: future::RemoteHandle<Result<R, future::Aborted>>,
}

impl<R: 'static> Task<R> {
    fn spawn_<F: Future<Output = R>, T>(
        future: F,
        spawn: impl FnOnce(future::Remote<future::Abortable<F>>) -> T,
    ) -> Self {
        let (abortable_future, abort_handle) = future::abortable(future);
        let (task, output) = abortable_future.remote_handle();
        let _ = spawn(task);
        Self {
            abort_handle,
            output,
        }
    }

    /// Spawns a new task, potentially on the current thread.
    #[cfg(not(web))]
    pub fn spawn<F: Future<Output = R> + Send + 'static>(future: F) -> Self
    where
        R: Send,
    {
        Self::spawn_(future, tokio::task::spawn)
    }

    /// Spawns a new task on the current thread.
    #[cfg(web)]
    pub fn spawn<F: Future<Output = R> + 'static>(future: F) -> Self {
        Self::spawn_(future, wasm_bindgen_futures::spawn_local)
    }

    /// Creates a [`Task`] that is immediately ready.
    pub fn ready(value: R) -> Self {
        Self::spawn_(async { value }, |fut| {
            fut.now_or_never().expect("the future is ready")
        })
    }

    /// Cancels the task, resolving only when the wrapped future is completely dropped.
    pub async fn cancel(self) {
        self.abort_handle.abort();
        let _ = self.output.await;
    }

    /// Forgets the task. The task will continue to run to completion in the
    /// background, but will no longer be joinable or cancelable.
    pub fn forget(self) {
        self.output.forget();
    }
}

impl<R: 'static> std::future::IntoFuture for Task<R> {
    type Output = R;
    type IntoFuture = future::Map<
        future::RemoteHandle<Result<R, future::Aborted>>,
        fn(Result<R, future::Aborted>) -> R,
    >;

    fn into_future(self) -> Self::IntoFuture {
        self.output
            .map(|result| result.expect("we have the only AbortHandle"))
    }
}
