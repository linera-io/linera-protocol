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
        HandleApplicationCall, HandleSessionCall, PollApplicationCallResult, PollSessionCallResult,
    },
    service::{HandleQuery, PollApplicationQueryResult},
};
use crate::{ApplicationCallResult, CalleeContext, QueryContext, SessionCallResult};
use linera_base::identifiers::SessionId;
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
            new_function = $new_func:ident(
                $( $parameter_names:ident : $parameter_types:ty ),* $(,)?
            ),
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
                type Parameters = ($( $parameter_types, )*);
                type Output = $output;

                fn new(
                    ($( $parameter_names, )*): Self::Parameters,
                    application: &A,
                    store: &mut A::Store,
                ) -> Result<Self, ExecutionError> {
                    application.$new_func(store, $( $parameter_names ),*)
                        .map_err(|error| error.into())
                }

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
    HandleApplicationCall: {
        application_trait = Contract,
        new_function = handle_application_call_new(
            context: CalleeContext,
            argument: Vec<u8>,
            forwarded_sessions: Vec<SessionId>,
        ),
        poll_function = handle_application_call_poll,
        poll_type = PollApplicationCallResult,
        output_type = ApplicationCallResult,
    },

    HandleSessionCall: {
        application_trait = Contract,
        new_function = handle_session_call_new(
            context: CalleeContext,
            session_state: Vec<u8>,
            argument: Vec<u8>,
            forwarded_sessions: Vec<SessionId>,
        ),
        poll_function = handle_session_call_poll,
        poll_type = PollSessionCallResult,
        output_type = (SessionCallResult, Vec<u8>),
    },

    HandleQuery: {
        application_trait = Service,
        new_function = handle_query_new(context: QueryContext, query: Vec<u8>),
        poll_function = handle_query_poll,
        poll_type = PollApplicationQueryResult,
        output_type = Vec<u8>,
    },
}
