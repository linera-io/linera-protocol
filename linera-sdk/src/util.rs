// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Module with helper types and functions used by the SDK.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Yields the current asynchronous task so that other tasks may progress if possible.
///
/// After other tasks progress, this task resumes as soon as possible. More explicitly, it is
/// scheduled to be woken up as soon as possible.
pub fn yield_once() -> YieldOnce {
    YieldOnce::default()
}

/// A [`Future`] that returns [`Poll::Pending`] once and immediately schedules itself to wake up.
#[derive(Default)]
pub struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut this = self.as_mut();

        if this.yielded {
            Poll::Ready(())
        } else {
            this.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
