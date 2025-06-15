// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, pin::Pin};

use sync_wrapper::SyncFuture;

/// An extension trait to box futures and make them `Sync`.
pub trait FutureSyncExt: Future + Sized {
    /// Wrap the future so that it implements `Sync`
    fn make_sync(self) -> SyncFuture<Self> {
        SyncFuture::new(self)
    }

    /// Box the future without losing `Sync`ness
    fn boxed_sync(self) -> Pin<Box<SyncFuture<Self>>> {
        Box::pin(self.make_sync())
    }
}

impl<F: Future> FutureSyncExt for F {}
