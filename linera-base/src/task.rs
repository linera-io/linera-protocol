// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over tasks that can be used natively or on the Web.
 */

use std::future::Future;

#[cfg(not(web))]
mod implementation {
    use super::*;

    /// The type of a future awaiting another task.
    pub type NonBlockingFuture<R> = tokio::task::JoinHandle<R>;

    /// Spawns a new task, potentially on the current thread.
    pub fn spawn<F: Future<Output: Send> + Send + 'static>(
        future: F,
    ) -> NonBlockingFuture<F::Output> {
        tokio::task::spawn(future)
    }
}

#[cfg(web)]
mod implementation {
    use futures::{future, FutureExt as _};

    use super::*;

    /// The type of a future awaiting another task.
    pub type NonBlockingFuture<R> = future::RemoteHandle<R>;

    /// Spawns a new task on the current thread.
    pub fn spawn<F: Future + 'static>(future: F) -> NonBlockingFuture<F::Output> {
        let (future, remote_handle) = future.remote_handle();
        wasm_bindgen_futures::spawn_local(future);
        remote_handle
    }
}

pub use implementation::*;
