// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A way to export a future from a guest WASM module.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

/// A future that's exported from a guest WASM module to the host.
///
/// This enables a future created in the guest module to be polled from the host.
pub struct ExportedFuture<Output> {
    future: RefCell<Pin<Box<dyn Future<Output = Output>>>>,
    should_wake: Arc<AtomicBool>,
}

impl<Output> ExportedFuture<Output> {
    /// Creates a new [`ExportedFuture`] by exporting the provided `future`.
    pub fn new(future: impl Future<Output = Output> + 'static) -> Self {
        ExportedFuture {
            future: RefCell::new(Box::pin(future)),
            should_wake: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Polls the future from the host.
    ///
    /// A fake task context is created so that it can capture any scheduled awakes, and poll again
    /// immediately in that case.
    ///
    /// The exported future can be scheduled to be awakened while this method is executing. In that
    /// case, it should be pooled again immediately.
    ///
    /// No host task contexts are used inside the guest WASM module, so it is impossible to have the
    /// exported future scheduled to be awakened from the host. Instead, the exported future should
    /// be called from a real task context on the host side, so that it can capture scheduled awakes
    /// on the host side if or when the guest calls host futures. When the awake happens on the
    /// host side, the guest module can be called again and the exported future polled.
    pub fn poll<CustomPoll>(&self) -> CustomPoll
    where
        CustomPoll: From<Poll<Output>>,
    {
        let should_wake = ShouldWake::new(self.should_wake.clone());
        let waker = should_wake.into_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = self.future.borrow_mut();

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Pending if self.should_wake.swap(false, Ordering::AcqRel) => continue,
                poll => return CustomPoll::from(poll),
            }
        }
    }
}

/// A helper type that tracks if the future was scheduled to awake.
#[allow(clippy::redundant_allocation)]
#[derive(Clone)]
struct ShouldWake(Box<Arc<AtomicBool>>);

impl ShouldWake {
    /// Creates a new [`ShouldWake`] instance using the reference to the `should_wake` flag.
    pub fn new(should_wake: Arc<AtomicBool>) -> Self {
        ShouldWake(Box::new(should_wake))
    }

    /// Creates a [`Waker`] object from this [`ShouldWake`] instance.
    pub fn into_waker(self) -> Waker {
        let raw_waker = RawWaker::new(unsafe { self.stay_alive() }, &WAKER_VTABLE);
        unsafe { Waker::from_raw(raw_waker) }
    }

    /// Restores this [`ShouldWake`] instance from a "cookie" pointer.
    unsafe fn unwrap_from(pointer: *const ()) -> Self {
        let payload = Box::from_raw(pointer as *mut _);
        ShouldWake(payload)
    }

    /// Converts this [`ShouldWake`] instance into a "cookie" pointer, so that it can be used again
    /// later.
    unsafe fn stay_alive(self) -> *const () {
        Box::leak(self.0) as *const _ as *const ()
    }

    /// Flags that the [`ExportedFuture`] needs to be polled again.
    fn wake(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// A static function pointer table for the custom [`Waker`] type.
const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

/// Clones the [`ShouldWake`] instance.
unsafe fn clone(internal_waker: *const ()) -> RawWaker {
    let should_wake = ShouldWake::unwrap_from(internal_waker);
    let new_internal_waker = should_wake.clone().stay_alive();
    should_wake.stay_alive();
    RawWaker::new(new_internal_waker, &WAKER_VTABLE)
}

/// Uses the [`ShouldWake`] instance to flag that the future needs to be polled again.
unsafe fn wake(internal_waker: *const ()) {
    let should_wake = ShouldWake::unwrap_from(internal_waker);
    should_wake.wake();
}

/// Uses the [`ShouldWake`] instance to flag that the future needs to be polled again, without
/// consuming the waker so that it can be used again later.
unsafe fn wake_by_ref(internal_waker: *const ()) {
    let should_wake = ShouldWake::unwrap_from(internal_waker);
    should_wake.wake();
    should_wake.stay_alive();
}

/// Finalizes a [`ShouldWake`] instance.
unsafe fn drop(internal_waker: *const ()) {
    let _ = ShouldWake::unwrap_from(internal_waker);
}

/// Declares a type exporting a future declared as a WIT type.
///
/// Used internally to generate the boilerplate to export the asynchronous endpoints from the
/// guest.
///
/// # Parameters
///
/// - `module`: specifies which WIT interface the future is declared in (either `service` or
///   `contract`
/// - `future`: the name of the exported future type
/// - `application`: the type that implements the WIT interface trait
/// - `parameters`: the names of the parameters necessary for creating the exported future resource
/// - `parameter_types`: the types of the parameters necessary for creating the exported future
///   resource
/// - `return_type`: the WIT poll type returned by the exported future's `poll` method (must be in
///   the previously specified `module`)
#[macro_export]
macro_rules! instance_exported_future {
    (
        $module:ident :: $future:ident < $application:ty > (
            $( $parameters:ident : $parameter_types:ty ),* $(,)*
        ) -> $return_type:ident
    ) => {
        pub struct $future($crate::$module::exported_futures::$future<$application>);

        impl $crate::$module::wit_types::$future for $future {
            fn new(
                $( $parameters: $parameter_types ),*
            ) -> $crate::wit_bindgen_guest_rust::Handle<Self> {
                $crate::wit_bindgen_guest_rust::Handle::new(
                    $future($crate::$module::exported_futures::$future::new(
                        $( $parameters ),*
                    ))
                )
            }

            fn poll(&self) -> $crate::$module::wit_types::$return_type {
                self.0.poll()
            }
        }
    };
}
