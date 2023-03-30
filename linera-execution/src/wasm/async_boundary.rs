// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to handle async code between the host WebAssembly runtime and guest WebAssembly
//! modules.

use super::{
    common::{ApplicationRuntimeContext, WasmRuntimeContext},
    ExecutionError,
};
use futures::{future::BoxFuture, ready, stream::StreamExt};
use std::{
    any::type_name,
    fmt::{self, Debug, Formatter},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use tokio::sync::Mutex;

/// A host future that can be called by a WASM guest module.
pub struct HostFuture<'future, Output> {
    future: Mutex<BoxFuture<'future, Output>>,
}

impl<Output> Debug for HostFuture<'_, Output> {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(
            formatter,
            "HostFuture<'_, {}> {{ .. }}",
            type_name::<Output>()
        )
    }
}

impl<'future, Output> HostFuture<'future, Output> {
    /// Wrap a given `future` so that it can be called from guest WASM modules.
    pub fn new(future: impl Future<Output = Output> + Send + 'future) -> Self {
        HostFuture {
            future: Mutex::new(Box::pin(future)),
        }
    }

    /// Poll a future from a WASM module.
    ///
    /// Requires the task [`Waker`] to have been saved in the provided `waker`. If it hasn't, or if
    /// the waker for a task other than the task used to call the WASM module code is provided, the
    /// call may panic or the future may not be scheduled to resume afterwards, leading the module
    /// to hang.
    ///
    /// # Panics
    ///
    /// If the `context` does not contain a valid exclusive task [`Waker`] reference, or if this
    /// future is polled concurrently in different tasks.
    pub fn poll(&self, waker: &mut WakerForwarder) -> Poll<Output> {
        let waker_reference = waker
            .0
            .try_lock()
            .expect("Unexpected concurrent application call");

        let mut context = Context::from_waker(
            waker_reference
                .as_ref()
                .expect("Application called without an async task context"),
        );

        let mut future = self
            .future
            .try_lock()
            .expect("Application can't call the future concurrently because it's single threaded");

        future.as_mut().poll(&mut context)
    }
}

/// A future implemented in a WASM module.
pub enum GuestFuture<'context, Future, Application>
where
    Application: ApplicationRuntimeContext,
{
    /// The WASM module failed to create an instance of the future.
    ///
    /// The error will be returned when this [`GuestFuture`] is polled.
    FailedToCreate(Option<Application::Error>),

    /// The WASM future type and the runtime context to poll it.
    Active {
        /// A WIT resource type implementing a [`GuestFutureInterface`] so that it can be polled.
        future: Future,

        /// Types necessary to call the guest WASM module in order to poll the future.
        context: WasmRuntimeContext<'context, Application>,
    },
}

impl<'context, Future, Application> GuestFuture<'context, Future, Application>
where
    Application: ApplicationRuntimeContext,
{
    /// Create a [`GuestFuture`] instance with `creation_result` of a future resource type.
    ///
    /// If the guest resource type could not be created by the WASM module, the error is stored so
    /// that it can be returned when the [`GuestFuture`] is polled.
    pub fn new(
        creation_result: Result<Future, Application::Error>,
        context: WasmRuntimeContext<'context, Application>,
    ) -> Self {
        match creation_result {
            Ok(future) => GuestFuture::Active { future, context },
            Err(error) => GuestFuture::FailedToCreate(Some(error)),
        }
    }
}

impl<InnerFuture, Application> Future for GuestFuture<'_, InnerFuture, Application>
where
    InnerFuture: GuestFutureInterface<Application> + Unpin,
    Application: ApplicationRuntimeContext + Unpin,
    Application::Store: Unpin,
    Application::Error: Unpin,
    Application::Extra: Unpin,
{
    type Output = Result<InnerFuture::Output, ExecutionError>;

    /// Polls the guest future after the [`HostFutureQueue`] in the [`WasmRuntimeContext`] indicates
    /// that it's safe to do so without breaking determinism.
    ///
    /// Uses the runtime context to call the WASM future's `poll` method, as implemented in the
    /// [`GuestFutureInterface`]. The `task_context` is stored in the runtime context's
    /// [`WakerForwarder`], so that any host futures the guest calls can use the correct task
    /// context.
    fn poll(self: Pin<&mut Self>, task_context: &mut Context) -> Poll<Self::Output> {
        match self.get_mut() {
            GuestFuture::FailedToCreate(runtime_error) => {
                let error = runtime_error.take().expect("Unexpected poll after error");
                Poll::Ready(Err(error.into()))
            }
            GuestFuture::Active { future, context } => {
                ready!(context.future_queue.poll_next_unpin(task_context));

                let _context_guard = context.waker_forwarder.forward(task_context);
                future.poll(&context.application, &mut context.store)
            }
        }
    }
}

/// Interface to poll a future implemented in a WASM module.
pub trait GuestFutureInterface<Application>
where
    Application: ApplicationRuntimeContext,
{
    /// The output of the guest future.
    type Output;

    /// Poll the guest future to attempt to progress it.
    ///
    /// May return an [`ExecutionError`] if the guest WASM module panics, for example.
    fn poll(
        &self,
        application: &Application,
        store: &mut Application::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>>;
}

/// A type to keep track of a [`Waker`] so that it can be forwarded to any async code called from
/// the guest WASM module.
///
/// When a [`Future`] is polled, a [`Waker`] is used so that the task can be scheduled to be
/// woken up and polled again if it's still awaiting something.
///
/// The problem is that calling a WASM module from an async task can lead to that guest code
/// calling back some host async code. A [`Context`] for the new host code must be created with the
/// same [`Waker`] to ensure that the wake events are forwarded back correctly to the host code
/// that called the guest.
///
/// Because the context has a lifetime and that forwarding lifetimes through the runtime calls is
/// not possible, this type erases the lifetime of the context and stores it in an `Arc<Mutex<_>>`
/// so that the context can be obtained again later. To ensure that this is safe, an
/// [`ActiveContextGuard`] instance is used to remove the context from memory before the lifetime
/// ends.
#[derive(Clone, Default)]
pub struct WakerForwarder(Arc<Mutex<Option<Waker>>>);

impl WakerForwarder {
    /// Forwards the waker from the task `context` into shared memory so that it can be obtained
    /// later.
    pub fn forward<'context>(&mut self, context: &mut Context) -> ActiveContextGuard<'context> {
        let mut waker_reference = self
            .0
            .try_lock()
            .expect("Unexpected concurrent task context access");

        assert!(
            waker_reference.is_none(),
            "`WakerForwarder` accessed by concurrent tasks"
        );

        *waker_reference = Some(context.waker().clone());

        ActiveContextGuard {
            waker: self.0.clone(),
            lifetime: PhantomData,
        }
    }
}

/// A guard type responsible for ensuring the [`Waker`] stored in shared memory does not outlive
/// the task [`Context`] it was obtained from.
pub struct ActiveContextGuard<'context> {
    waker: Arc<Mutex<Option<Waker>>>,
    lifetime: PhantomData<&'context mut ()>,
}

impl Drop for ActiveContextGuard<'_> {
    fn drop(&mut self) {
        let mut waker_reference = self
            .waker
            .try_lock()
            .expect("Unexpected concurrent task context access");

        *waker_reference = None;
    }
}
