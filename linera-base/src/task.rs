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

    /// Types that can be _explicitly_ sent to a new thread.
    /// This differs from `Send` in that we can provide an explicit post step
    /// (e.g. `postMessage` on the Web).
    pub trait Post: Send + Sync {}

    impl<T: Send + Sync> Post for T {}

    /// A type that satisfies the send/receive bounds, but can never be sent or received.
    pub type NoInput = std::convert::Infallible;

    /// The type of a future awaiting another task.
    pub type NonBlockingFuture<R> = tokio::task::JoinHandle<R>;
    /// The type of a future awaiting another thread.
    pub type BlockingFuture<R> = tokio::task::JoinHandle<R>;
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

    /// A new task running in a different thread.
    pub struct Blocking<Input = NoInput, Output = ()> {
        sender: mpsc::UnboundedSender<Input>,
        join_handle: tokio::task::JoinHandle<Output>,
    }

    impl<Input: Send + 'static, Output: Send + 'static> Blocking<Input, Output> {
        /// Spawns a blocking task on a new thread with a stream of input messages.
        pub async fn spawn<F: Future<Output = Output>>(
            work: impl FnOnce(InputReceiver<Input>) -> F + Send + 'static,
        ) -> Self {
            let (sender, receiver) = mpsc::unbounded_channel();
            Self {
                sender,
                join_handle: tokio::task::spawn_blocking(|| {
                    futures::executor::block_on(work(receiver.into()))
                }),
            }
        }

        /// Waits for the task to complete and returns its output.
        pub async fn join(self) -> Output {
            self.join_handle.await.expect("task shouldn't be cancelled")
        }

        /// Sends a message to the task.
        pub fn send(&self, message: Input) -> Result<(), SendError<Input>> {
            self.sender.send(message)
        }
    }
}

#[cfg(web)]
mod implementation {
    use std::convert::TryFrom;

    use futures::{channel::oneshot, stream, StreamExt as _};
    use wasm_bindgen::prelude::*;
    use web_sys::js_sys;

    use super::*;
    use crate::dyn_convert;

    /// Types that can be _explicitly_ sent to a new thread.
    /// This differs from `Send` in that we can provide an explicit post step
    /// (e.g. `postMessage` on the Web).
    // TODO(#2809): this trait is overly liberal.
    pub trait Post: dyn_convert::DynInto<JsValue> {}

    impl<T: dyn_convert::DynInto<JsValue>> Post for T {}

    /// A type that satisfies the send/receive bounds, but can never be sent or received.
    pub enum NoInput {}

    impl TryFrom<JsValue> for NoInput {
        type Error = JsValue;
        fn try_from(value: JsValue) -> Result<Self, JsValue> {
            Err(value)
        }
    }

    impl From<NoInput> for JsValue {
        fn from(no_input: NoInput) -> Self {
            match no_input {}
        }
    }

    /// The type of errors that can result from sending a message to the spawned task.
    pub struct SendError<T>(T);

    impl<T> std::fmt::Debug for SendError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.debug_struct("SendError").finish_non_exhaustive()
        }
    }

    impl<T> std::fmt::Display for SendError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "send error")
        }
    }

    impl<T> std::error::Error for SendError<T> {}

    /// A new task running in a different thread.
    pub struct Blocking<Input = NoInput, Output = ()> {
        join_handle: wasm_thread::JoinHandle<Output>,
        _phantom: std::marker::PhantomData<fn(Input)>,
    }

    /// The stream of inputs available to the spawned task.
    pub type InputReceiver<T> =
        stream::Map<tokio_stream::wrappers::UnboundedReceiverStream<JsValue>, fn(JsValue) -> T>;

    fn convert_or_panic<V, T: TryFrom<V, Error: std::fmt::Debug>>(value: V) -> T {
        T::try_from(value).expect("type correctness should ensure this can be deserialized")
    }

    /// The type of a future awaiting another task.
    pub type NonblockingFuture<R> = oneshot::Receiver<R>;

    /// Spawns a new task on the current thread.
    pub fn spawn<F: Future + 'static>(future: F) -> NonblockingFuture<F::Output> {
        let (send, recv) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async {
            let _ = send.send(future.await);
        });
        recv
    }

    impl<Input, Output> Blocking<Input, Output> {
        /// Spawns a blocking task on a new Web Worker with a stream of input messages.
        pub async fn spawn<F: Future<Output = Output>>(
            work: impl FnOnce(InputReceiver<Input>) -> F + Send + 'static,
        ) -> Self
        where
            Input: Into<JsValue> + TryFrom<JsValue, Error: std::fmt::Debug>,
            Output: Send + 'static,
        {
            let (ready_sender, ready_receiver) = oneshot::channel();
            let join_handle = wasm_thread::Builder::new()
                .spawn(|| async move {
                    let (input_sender, input_receiver) = mpsc::unbounded_channel::<JsValue>();
                    let input_receiver =
                        tokio_stream::wrappers::UnboundedReceiverStream::new(input_receiver);
                    let onmessage = wasm_bindgen::closure::Closure::<
                        dyn FnMut(web_sys::MessageEvent) -> Result<(), JsError>,
                    >::new(
                        move |event: web_sys::MessageEvent| -> Result<(), JsError> {
                            input_sender.send(event.data())?;
                            Ok(())
                        },
                    );
                    js_sys::global()
                        .dyn_into::<web_sys::DedicatedWorkerGlobalScope>()
                        .unwrap()
                        .set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
                    onmessage.forget(); // doesn't truly forget it, but lets the JS GC take care of it
                    ready_sender.send(()).unwrap();
                    work(input_receiver.map(convert_or_panic::<JsValue, Input>)).await
                })
                .expect("should successfully start Web Worker");

            ready_receiver
                .await
                .expect("should successfully initialize the worker thread");
            Self {
                join_handle,
                _phantom: Default::default(),
            }
        }

        /// Sends a message to the task using
        /// [`postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Worker/postMessage).
        pub fn send(&self, message: Input) -> Result<(), SendError<Input>>
        where
            Input: Into<JsValue> + TryFrom<JsValue> + Clone,
        {
            self.join_handle
                .thread()
                .post_message(&message.clone().into())
                .map_err(|_| SendError(message))
        }

        /// Waits for the task to complete and returns its output.
        pub async fn join(self) -> Output {
            match self.join_handle.join_async().await {
                Ok(output) => output,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
    }
}

pub use implementation::*;
