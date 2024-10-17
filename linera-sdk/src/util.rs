// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Module with helper types and functions used by the SDK.

use std::{
    future::Future,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use futures::task;

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

/// An extension trait to block on a [`Future`] until it completes.
pub trait BlockingWait {
    /// The type returned by the [`Future`].
    type Output;

    /// Waits for the [`Future`] to complete in a blocking manner.
    ///
    /// Effectively polls the [`Future`] repeatedly until it returns [`Poll::Ready`].
    fn blocking_wait(self) -> Self::Output;
}

impl<AnyFuture> BlockingWait for AnyFuture
where
    AnyFuture: Future,
{
    type Output = AnyFuture::Output;

    fn blocking_wait(mut self) -> Self::Output {
        let waker = task::noop_waker();
        let mut task_context = Context::from_waker(&waker);
        let mut future = pin!(self);

        loop {
            match future.as_mut().poll(&mut task_context) {
                Poll::Pending => continue,
                Poll::Ready(output) => return output,
            }
        }
    }
}

/// Unit tests for the helpers defined in the `util` module.
#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use futures::{future::poll_fn, task::noop_waker, FutureExt as _};

    use super::{yield_once, BlockingWait};

    /// Tests the behavior of the [`YieldOnce`] future.
    ///
    /// Checks the internal state before and after the first and second polls, and ensures that
    /// only the first poll returns [`Poll::Pending`].
    #[test]
    #[expect(clippy::bool_assert_comparison)]
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

    /// Tests the behavior of the [`BlockingWait`] extension.
    #[test]
    fn blocking_wait_blocks_until_future_is_ready() {
        let mut remaining_polls = 100;

        let future = poll_fn(|_context| {
            if remaining_polls == 0 {
                Poll::Ready(())
            } else {
                remaining_polls -= 1;
                Poll::Pending
            }
        });

        future.blocking_wait();

        assert_eq!(remaining_polls, 0);
    }
}
