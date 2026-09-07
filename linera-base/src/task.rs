// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over tasks that can be used natively or on the Web.
 */

use futures::{future, Future, FutureExt as _};

/// `Send` on native targets; no bound on web (where there's only one thread).
///
/// Use this in generic bounds that need `Send` on native but should compile on
/// web without the bound. Combined with [`run_detached`], this lets a single
/// function body support both targets.
#[cfg(not(web))]
pub trait MaybeSend: Send {}
#[cfg(not(web))]
impl<T: Send> MaybeSend for T {}

/// `Sync` on native targets; no bound on web (where there's only one thread).
///
/// Use this in generic bounds that need `Sync` on native but should compile on
/// web without the bound.
#[cfg(not(web))]
pub trait MaybeSync: Sync {}
#[cfg(not(web))]
impl<T: Sync> MaybeSync for T {}

/// `Send` on native targets; no bound on web (where there's only one thread).
#[cfg(web)]
pub trait MaybeSend {}
#[cfg(web)]
impl<T> MaybeSend for T {}

/// `Sync` on native targets; no bound on web (where there's only one thread).
#[cfg(web)]
pub trait MaybeSync {}
#[cfg(web)]
impl<T> MaybeSync for T {}

/// Spawns `future` on the runtime and awaits its completion.
///
/// Dropping the returned future does *not* cancel the spawned task — it runs
/// to completion in the background. Use this when the spawned work (e.g. a
/// storage write paired with its in-memory finalization) must not be torn
/// apart mid-flight by caller cancellation.
pub async fn run_detached<F, R>(future: F) -> R
where
    F: Future<Output = R> + MaybeSend + 'static,
    R: MaybeSend + 'static,
{
    // On native, `tokio::task::spawn` returns a `JoinHandle` that already
    // detaches on drop. On web, `wasm_bindgen_futures::spawn_local` is
    // fire-and-forget, so we deliver the output through a oneshot channel.
    #[cfg(not(web))]
    {
        join_detached(tokio::task::spawn(future)).await
    }
    #[cfg(web)]
    {
        let (tx, rx) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            if tx.send(future.await).is_err() {
                tracing::debug!("run_detached: receiver dropped before result was delivered");
            }
        });
        rx.await
            .expect("spawned task dropped without sending its result")
    }
}

/// Awaits a detached task, propagating its panic but not its cancellation.
///
/// [`run_detached`] never lets the `JoinHandle` escape, so nothing can abort the task: a
/// cancellation means `spawn` bound the task to an already-closed runtime, which hands back a
/// handle that is dead on arrival. There is no value left to return and the shutdown that closed
/// that list is about to drop this future too, so it waits rather than reporting a teardown as a
/// failure — but it says so first, because that reasoning only holds while the awaiting task
/// lives on the runtime that died.
#[cfg(not(web))]
async fn join_detached<R>(handle: tokio::task::JoinHandle<R>) -> R {
    match handle.await {
        Ok(output) => output,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => {
            tracing::warn!(%error, "detached task cancelled; waiting for the shutdown that caused it");
            future::pending().await
        }
    }
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
        spawn(task);
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
        // We just want to wait for the task to finish unwinding; an `Aborted` error is the expected outcome.
        self.output.await.ok();
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

#[cfg(all(test, not(web)))]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A cancelled detached task must not be reported as a panic.
    ///
    /// Cancellation is provoked with `abort` because the runtime shutdown that causes it in
    /// production cannot be staged inside a test that still needs the runtime alive. Reverting
    /// `join_detached` to `into_panic` makes this fail with "`JoinError` reason is not a panic".
    #[tokio::test]
    async fn a_cancelled_detached_task_waits_instead_of_panicking() {
        let handle = tokio::task::spawn(future::pending::<()>());
        handle.abort();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), join_detached(handle))
                .await
                .is_err(),
            "a cancelled task has no value to yield, so joining it must not resolve",
        );
    }

    /// The panic of a detached task must still reach whoever awaited it.
    #[tokio::test]
    async fn a_panicking_detached_task_still_propagates() {
        let joined = tokio::task::spawn(async {
            run_detached(async { panic!("the detached task failed") }).await
        })
        .await;
        let error = joined.expect_err("the panic must not be swallowed");
        let payload = error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("the detached task failed"),
            "the original panic payload must survive the join",
        );
    }
}
