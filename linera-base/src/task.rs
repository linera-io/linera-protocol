// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over tasks that can be used natively or on the Web.
 */

use std::future::Future;
use tokio::sync::mpsc;

#[cfg(not(web))]
mod implementation {
    use super::*;

    /// The type of errors that can result from awaiting a task to completion.
    pub type Error = tokio::task::JoinError;
    /// The type of a future awaiting another task.
    pub type NonBlockingFuture<R> = tokio::task::JoinHandle<R>;
    /// The type of a future awaiting another thread.
    pub type BlockingFuture<R> = tokio::task::JoinHandle<R>;
    /// A channel that can be used to send messages to the spawned task.
    pub type InputSender<T> = mpsc::UnboundedSender<T>;
    /// The stream of inputs available to the spawned task.
    pub type InputReceiver<T> = tokio_stream::wrappers::UnboundedReceiverStream<T>;
    /// The type of errors that can result from sending a message to the spawned task.
    pub use mpsc::error::SendError;

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

    /// Spawns a blocking task on a new Web Worker with a stream of input messages.
    pub fn spawn_blocking_with_input<T: Send + 'static, F: Future<Output: Send + 'static>>(
        task: impl FnOnce(InputReceiver<T>) -> F + Send + 'static,
    ) -> (InputSender<T>, BlockingFuture<F::Output>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            sender,
            tokio::task::spawn_blocking(|| futures::executor::block_on(task(receiver.into()))),
        )
    }
}

#[cfg(web)]
mod implementation {
    use std::convert::TryFrom;
    use futures::{channel::oneshot, future, stream, StreamExt as _};
    use wasm_bindgen::prelude::*;
    use web_sys::js_sys;
    use std::rc::Rc;

    use super::*;

    /// A type that satisfies the send/receive bounds, but can never be sent or received.
    pub enum NoInput { }

    impl TryFrom<JsValue> for NoInput {
        type Error = JsValue;
        fn try_from(value: JsValue) -> Result<Self, JsValue> {
            Err(value)
        }
    }

    impl Into<JsValue> for NoInput {
        fn into(self) -> JsValue {
            match self { }
        }
    }

    /// The type of errors that can result from sending a message to the spawned task.
    pub struct SendError<T> {
        value: JsValue,
        _phantom: std::marker::PhantomData<T>,
    }

    impl<T> std::fmt::Debug for SendError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.value, f)
        }
    }

    impl<T> From<JsValue> for SendError<T> {
        fn from(value: JsValue) -> Self {
            Self {
                value,
                _phantom: Default::default(),
            }
        }
    }

    /// A channel that can be used to send messages to the spawned task.
    pub struct InputSender<T> {
        worker: Rc<web_sys::Worker>,
        _phantom: std::marker::PhantomData<fn(T)>,
    }

    impl<T> From<Rc<web_sys::Worker>> for InputSender<T> {
        fn from(worker: Rc<web_sys::Worker>) -> Self {
            Self {
                worker,
                _phantom: Default::default(),
            }
        }
    }

    impl<T: Into<JsValue>> InputSender<T> {
        /// Send a message to the task using
        /// [`postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Worker/postMessage).
        pub fn send(&self, message: T) -> Result<(), SendError<T>> {
            self.worker.post_message(&message.into()).map_err(Into::into)
        }
    }

    /// The stream of inputs available to the spawned task.
    pub type InputReceiver<T> = stream::FilterMap<
        tokio_stream::wrappers::UnboundedReceiverStream<JsValue>,
        future::Ready<Option<T>>,
        fn(JsValue) -> future::Ready<Option<T>>,
    >;

    fn convert_or_discard<V, T: TryFrom<V>>(value: V) -> future::Ready<Option<T>> {
        future::ready(T::try_from(value).ok())
    }

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

    /// Spawns a blocking task on a new Web Worker with a stream of input messages.
    pub fn spawn_blocking_with_input<T: Into<JsValue> + TryFrom<JsValue, Error: std::error::Error>, F: Future<Output: Send + 'static>>(
        task: impl FnOnce(InputReceiver<T>) -> F + Send + 'static,
    ) -> (InputSender<T>, BlockingFuture<F::Output>) {
        let (done_sender, done_receiver) = oneshot::channel();
        let (worker, _join_handle) = wasm_thread::Builder::new().spawn(|| async move {
            let (input_sender, input_receiver) = mpsc::unbounded_channel::<JsValue>();
            let input_receiver = tokio_stream::wrappers::UnboundedReceiverStream::new(input_receiver);
            let onmessage = wasm_bindgen::closure::Closure::<dyn FnMut(JsValue) -> Result<(), JsError>>::new(move |v: JsValue| -> Result<(), JsError> {
                input_sender.send(v)?;
                Ok(())
            });
            js_sys::global().dyn_into::<web_sys::DedicatedWorkerGlobalScope>().unwrap().set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
            onmessage.forget(); // doesn't truly forget it, but lets the JS GC take care of it
            let _ = done_sender.send(task(input_receiver.filter_map(convert_or_discard::<JsValue, T>)).await);
        }).unwrap();
        (worker.into(), done_receiver)
    }
}

pub use implementation::*;
