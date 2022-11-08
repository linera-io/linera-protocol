// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to handle async code between the host WebAssembly runtime and guest WebAssembly
//! modules.

use std::{marker::PhantomData, mem, sync::Arc, task::Context};
use tokio::sync::Mutex;

/// A type to keep track of a [`std::task::Context`] so that it can be forwarded to any async code
/// called from the guest WASM module.
///
/// When a [`Future`] is polled, a [`Context`] is used so that the task can be scheduled to be
/// woken up and polled again if it's still awaiting something. The context has a lifetime, and can
/// only be used during the call to the future's poll method.
///
/// The problem is that calling a WASM module from an async task can lead to that guest code
/// calling back some host async code. The task context must then be forwarded from the host code
/// that called the guest code to the host code that was called from the guest code.
///
/// Because the context has a lifetime and that forwarding lifetimes through the runtime calls is
/// not possible, this type erases the lifetime of the context and stores it in an `Arc<Mutex<_>>`
/// so that the context can be obtained again later. To ensure that this is safe, an
/// [`ActiveContextGuard`] instance is used to remove the context from memory before the lifetime
/// ends.
#[derive(Clone, Default)]
pub struct ContextForwarder(Arc<Mutex<Option<&'static mut Context<'static>>>>);

impl ContextForwarder {
    /// Forwards the task `context` into shared memory so that it can be obtained later.
    ///
    /// # Safety
    ///
    /// This method uses a [`mem::transmute`] call to erase the lifetime of the `context`
    /// reference. However, this is safe because the lifetime is transfered to the returned
    /// [`ActiveContextGuard`], which removes the unsafe reference from memory when it is dropped,
    /// ensuring the lifetime is respected.
    pub fn forward<'context>(
        &mut self,
        context: &'context mut Context,
    ) -> ActiveContextGuard<'context> {
        let mut context_reference = self
            .0
            .try_lock()
            .expect("Unexpected concurrent task context access");

        assert!(
            context_reference.is_none(),
            "`ContextForwarder` accessed by concurrent tasks"
        );

        *context_reference = Some(unsafe { mem::transmute(context) });

        ActiveContextGuard {
            context: self.0.clone(),
            lifetime: PhantomData,
        }
    }
}

/// A guard type responsible for ensuring the context stored in shared memory does not outlive its
/// lifetime.
pub struct ActiveContextGuard<'context> {
    context: Arc<Mutex<Option<&'static mut Context<'static>>>>,
    lifetime: PhantomData<&'context mut ()>,
}

impl Drop for ActiveContextGuard<'_> {
    fn drop(&mut self) {
        let mut context_reference = self
            .context
            .try_lock()
            .expect("Unexpected concurrent task context access");

        *context_reference = None;
    }
}
