// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to enforce determinism on asynchronous code called from a guest WASM module.
//!
//! To ensure that asynchronous calls from a guest WASM module are deterministic, the following
//! rules are enforced:
//!
//! - Futures are completed in the exact same order that they were created;
//! - The guest WASM module is only polled when the next future to be completed has finished;
//! - Every time the guest WASM module is polled, exactly one future will return [`Poll::Ready`];
//! - All other futures will return [`Poll::Pending`].
//!
//! To enforce these rules, the futures have to be polled separately from the guest WASM module.
//! The traditional asynchronous behavior is for the host to poll the guest, and for the guest to
//! poll the host futures again. This is problematic because the number of times the host futures
//! need to be polled might not be deterministic. So even if the futures are made to finish
//! sequentially, the number of times the guest is polled would not be deterministic.
//!
//! For the guest to be polled separately from the host futures it calls, two types are used:
//! [`HostFutureQueue`] and [`QueuedHostFutureFactory`]. The [`QueuedHostFutureFactory`] is what is
//! used by the guest WASM module handle to enqueue futures for deterministic execution (i.e.,
//! normally stored in the application's exported API handler). For every future that's enqueued, a
//! [`HostFuture`] is returned that contains only a [`oneshot::Receiver`] for the future's result.
//! The future itself is actually sent to the [`HostFutureQueue`] to be polled separately from the
//! guest.
//!
//! The [`HostFutureQueue`] implements [`Stream`], and produces a marker `()` item every time the
//! next future in the queue is ready for completion. Therefore, the [`GuestFuture`] is responsible
//! for always polling the [`HostFutureQueue`] before polling the guest WASM module.
