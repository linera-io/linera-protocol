// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of [`GuestFutureInterface`] for future types imported from a guest WebAssembly
//! application module.

#![allow(clippy::duplicate_mod)]

use super::{
    super::{
        async_boundary::GuestFutureInterface,
        common::{Contract, Service},
        ExecutionError,
    },
    contract::{
        ExecuteMessage, ExecuteOperation, HandleApplicationCall, HandleSessionCall, Initialize,
        PollApplicationCallResult, PollExecutionResult, PollSessionCallResult,
    },
    service::{HandleQuery, PollApplicationQueryResult},
};
use crate::{ApplicationCallResult, RawExecutionResult, SessionCallResult};
use std::task::Poll;

/// Implements [`GuestFutureInterface`] for a `future` type implemented by a guest Wasm module.
///
/// The future is then polled by calling the guest `poll_func`. The return type of that function is
/// a `poll_type` that must be convertible into the `output` type wrapped in a
/// `Poll<Result<_, _>>`.
macro_rules! impl_guest_future_interface {
    (
        $( $future:ident : {
            application_trait = $trait:ident,
            poll_function = $poll_func:ident,
            poll_type = $poll_type:ident,
            output_type = $output:ty $(,)*
        } ),* $(,)*
    ) => {
        $(
            impl<'storage, A> GuestFutureInterface<A> for $future
            where
                A: $trait<$poll_type = $poll_type, $future = Self>,
            {
                type Output = $output;

                fn poll(
                    &self,
                    application: &A,
                    store: &mut A::Store,
                ) -> Poll<Result<Self::Output, ExecutionError>> {
                    match application.$poll_func(store, self) {
                        Ok($poll_type::Ready(Ok(result))) => {
                            Poll::Ready(Ok(result.into()))
                        }
                        Ok($poll_type::Ready(Err(message))) => {
                            Poll::Ready(Err(ExecutionError::UserError(message).into()))
                        }
                        Ok($poll_type::Pending) => Poll::Pending,
                        Err(error) => Poll::Ready(Err(error.into())),
                    }
                }
            }
        )*
    }
}

impl_guest_future_interface! {
    Initialize: {
        application_trait = Contract,
        poll_function = initialize_poll,
        poll_type = PollExecutionResult,
        output_type = RawExecutionResult<Vec<u8>>,
    },

    ExecuteOperation: {
        application_trait = Contract,
        poll_function = execute_operation_poll,
        poll_type = PollExecutionResult,
        output_type = RawExecutionResult<Vec<u8>>,
    },

    ExecuteMessage: {
        application_trait = Contract,
        poll_function = execute_message_poll,
        poll_type = PollExecutionResult,
        output_type = RawExecutionResult<Vec<u8>>,
    },

    HandleApplicationCall: {
        application_trait = Contract,
        poll_function = handle_application_call_poll,
        poll_type = PollApplicationCallResult,
        output_type = ApplicationCallResult,
    },

    HandleSessionCall: {
        application_trait = Contract,
        poll_function = handle_session_call_poll,
        poll_type = PollSessionCallResult,
        output_type = (SessionCallResult, Vec<u8>),
    },

    HandleQuery: {
        application_trait = Service,
        poll_function = handle_query_poll,
        poll_type = PollApplicationQueryResult,
        output_type = Vec<u8>,
    },
}
