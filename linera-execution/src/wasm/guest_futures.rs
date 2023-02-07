// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of [`GuestFutureInterface`] for future types imported from a guest WebAssembly
//! application module.

#![allow(clippy::duplicate_mod)]

use super::{
    super::{
        async_boundary::GuestFutureInterface,
        common::{Contract, Service},
        WasmExecutionError,
    },
    contract::{
        CallApplication, CallSession, ExecuteEffect, ExecuteOperation, Initialize,
        PollCallApplication, PollCallSession, PollExecutionResult,
    },
    service::{PollQuery, QueryApplication},
};
use crate::{ApplicationCallResult, RawExecutionResult, SessionCallResult};
use std::task::Poll;

/// Implement [`GuestFutureInterface`] for a `future` type implemented by a guest WASM module.
///
/// The future is then polled by calling the guest `poll_func`. The return type of that function is
/// a `poll_type` that must be convertible into the `output` type wrapped in a
/// `Poll<Result<_, _>>`.
macro_rules! impl_guest_future_interface {
    ( $( $future:ident : $poll_func:ident -> $poll_type:ident -> $trait:ident => $output:ty ),* $(,)* ) => {
        $(
            impl<'storage, A> GuestFutureInterface<A> for $future
            where
                A: $trait,
                WasmExecutionError: From<A::Error>,
            {
                type Output = $output;

                fn poll(
                    &self,
                    application: &A,
                    store: &mut A::Store,
                ) -> Poll<Result<Self::Output, WasmExecutionError>> {
                    match application.$poll_func(store, self)? {
                        $poll_type::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
                        $poll_type::Ready(Err(message)) => {
                            Poll::Ready(Err(WasmExecutionError::UserApplication(message)))
                        }
                        $poll_type::Pending => Poll::Pending,
                    }
                }
            }
        )*
    }
}

impl_guest_future_interface! {
    Initialize: initialize_poll -> PollExecutionResult -> Contract => RawExecutionResult<Vec<u8>>,
    ExecuteOperation: execute_operation_poll -> PollExecutionResult -> Contract => RawExecutionResult<Vec<u8>>,
    ExecuteEffect: execute_effect_poll -> PollExecutionResult -> Contract => RawExecutionResult<Vec<u8>>,
    CallApplication: call_application_poll -> PollCallApplication -> Contract => ApplicationCallResult,
    CallSession: call_session_poll -> PollCallSession -> Contract => (SessionCallResult, Vec<u8>),
    QueryApplication: query_application_poll -> PollQuery -> Service => Vec<u8>,
}
