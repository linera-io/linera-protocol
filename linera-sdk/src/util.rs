// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Module with helper types and functions used by the SDK.

use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll},
};

use futures::task;

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

    fn blocking_wait(self) -> Self::Output {
        let mut context = Context::from_waker(task::noop_waker_ref());
        let mut future = pin!(self);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Pending => continue,
                Poll::Ready(output) => return output,
            }
        }
    }
}

/// Unit tests for the helpers defined in the `util` module.
#[cfg(test)]
mod tests {
    /// Tests the behavior of the [`BlockingWait`] extension.
    #[test]
    fn blocking_wait_blocks_until_future_is_ready() {
        use std::task::Poll;

        use super::BlockingWait as _;

        let mut remaining_polls = 100;

        let future = futures::future::poll_fn(|_context| {
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
