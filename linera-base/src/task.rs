// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over tasks that can be used natively or on the Web.
 */

use std::future::Future;

#[cfg(not(web))]
mod implementation {
    use super::*;

    /// Types that can be _explicitly_ sent to a new thread.
    /// This differs from `Send` in that we can provide an explicit post step
    /// (e.g. `postMessage` on the Web).
    pub trait Post: Send + Sync {}

    impl<T: Send + Sync> Post for T {}

    /// The type of errors that can result from awaiting a task to completion.
    pub type Error = tokio::task::JoinError;
    /// The type of a future awaiting another task.
    pub type NonBlockingFuture<R> = tokio::task::JoinHandle<R>;
    /// The type of a future awaiting another thread.
    pub type BlockingFuture<R> = tokio::task::JoinHandle<R>;

    /// Spawns a new task, potentially on the current thread.
    pub fn spawn<F: Future<Output: Send> + Send + 'static>(
        future: F,
    ) -> NonBlockingFuture<F::Output> {
        tokio::task::spawn(future)
    }

    /// Spawns a blocking task on a new thread.
    pub fn spawn_blocking<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
        future: F,
    ) -> BlockingFuture<R> {
        tokio::task::spawn_blocking(future)
    }
}

#[cfg(web)]
mod implementation {
    use futures::channel::oneshot;
    use wasm_bindgen_futures::wasm_bindgen::JsValue;

    use super::*;
    use crate::dyn_convert;

    /// Types that can be _explicitly_ sent to a new thread.
    /// This differs from `Send` in that we can provide an explicit post step
    /// (e.g. `postMessage` on the Web).
    // TODO(#2809): this trait is overly liberal.
    pub trait Post: dyn_convert::DynInto<JsValue> {}

    impl<T: dyn_convert::DynInto<JsValue>> Post for T {}

    /// The type of errors that can result from awaiting a task to completion.
    pub type Error = oneshot::Canceled;
    /// The type of a future awaiting another task.
    pub type NonblockingFuture<R> = oneshot::Receiver<R>;
    /// The type of a future awaiting another thread.
    pub type BlockingFuture<R> = oneshot::Receiver<R>;

    /// Spawns a new task on the current thread.
    pub fn spawn<F: Future + 'static>(future: F) -> NonblockingFuture<F::Output> {
        let (send, recv) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async {
            let _ = send.send(future.await);
        });
        recv
    }

    /// Spawns a blocking task on a new Web Worker.
    pub fn spawn_blocking<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
        task: F,
    ) -> BlockingFuture<R> {
        let (send, recv) = oneshot::channel();
        wasm_thread::spawn(move || {
            let _ = send.send(task());
        });
        recv
    }
}

pub use implementation::*;
