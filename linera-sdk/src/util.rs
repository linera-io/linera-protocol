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

/// Unit tests for the helpers defined in the `util` module.
#[cfg(test)]
mod tests {
    use super::yield_once;
    use futures::{task::noop_waker, FutureExt};
    use std::task::Context;

    /// Tests the behavior of the [`YieldOnce`] future.
    ///
    /// Checks the internal state before and after the first and second polls, and ensures that
    /// only the first poll returns [`Poll::Pending`].
    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn yield_once_returns_pending_only_on_first_call() {
        let mut future = yield_once();

        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert_eq!(future.yielded, false);
        assert!(future.poll_unpin(&mut context).is_pending());
        assert_eq!(future.yielded, true);
        assert!(future.poll_unpin(&mut context).is_ready());
        assert_eq!(future.yielded, true);
    }
}
