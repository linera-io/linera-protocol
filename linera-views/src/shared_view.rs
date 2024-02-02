// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::Context,
    views::{RootView, View, ViewError},
};
use async_lock::{Semaphore, SemaphoreGuardArc};
use async_trait::async_trait;
use futures::channel::oneshot;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[cfg(not(target_arch = "wasm32"))]
use crate::{increment_counter, SAVE_VIEW_COUNTER};

/// A way to safely share a [`View`] among multiple readers and at most one writer.
///
/// [`View`]s represent some data persisted in storage, but it also contains some state in
/// memory that caches the storage state and that queues changes to the persisted state to
/// be sent later. This means that two views referencing the same data in storage may have
/// state conflicts in memory, and that's why they can't be trivially shared (using
/// [`Clone`] for example).
///
/// The [`SharedView`] provides a way to share an inner [`View`] more safely, by ensuring
/// that only one writer is staging changes to the view, and than when it is writing those
/// changes to storage there aren't any more readers for the same view which would have
/// their internal state become invalid. The readers are not able to see the changes the
/// writer is staging, and the writer can only save its staged changes after all readers
/// have finished.
pub struct SharedView<C, V> {
    view: V,
    reader_count: Arc<AtomicUsize>,
    writer_semaphore: Arc<Semaphore>,
    writer_notifier: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _context: PhantomData<C>,
}

impl<C, V> SharedView<C, V>
where
    V: View<C>,
{
    /// Wraps a `view` in a [`SharedView`].
    pub fn new(view: V) -> Self {
        SharedView {
            view,
            reader_count: Arc::new(AtomicUsize::new(0)),
            writer_semaphore: Arc::new(Semaphore::new(1)),
            writer_notifier: Arc::new(Mutex::new(None)),
            _context: PhantomData,
        }
    }

    /// Returns a [`ReadOnlyViewReference`] to the inner [`View`].
    ///
    /// If there is a writer with a [`ReadWriteViewReference`] to the inner [`View`], waits
    /// until that writer is finished.
    pub async fn inner(&mut self) -> Result<ReadOnlyViewReference<V>, ViewError> {
        let _no_writer_check = self.writer_semaphore.acquire().await;

        self.reader_count.fetch_add(1, Ordering::SeqCst);

        Ok(ReadOnlyViewReference {
            view: self.view.share_unchecked()?,
            reader_count: self.reader_count.clone(),
            writer_notifier: self.writer_notifier.clone(),
        })
    }

    /// Returns a [`ReadWriteViewReference`] to the inner [`View`].
    ///
    /// Waits until the previous writer is finished if there is one. There can only be one
    /// [`ReadWriteViewReference`] to the same inner [`View`].
    pub async fn inner_mut(&mut self) -> Result<ReadWriteViewReference<V>, ViewError> {
        let writer_permit = self.writer_semaphore.acquire_arc().await;
        let (sender, receiver) = oneshot::channel();
        let mut writer_notifier = self
            .writer_notifier
            .lock()
            .expect("No panics should happen while holding the `write_notifier` lock");

        if let Some(notifier) = writer_notifier.take() {
            assert!(
                notifier.send(()).is_err(),
                "`writer_notifier` should either be already used or the receiving writer should \
                have already finished"
            );
        }

        *writer_notifier = Some(sender);

        Ok(ReadWriteViewReference {
            view: self.view.share_unchecked()?,
            write_barrier: Some(receiver),
            _writer_permit: writer_permit,
        })
    }
}

/// A read-only reference to a [`SharedView`].
pub struct ReadOnlyViewReference<V> {
    view: V,
    reader_count: Arc<AtomicUsize>,
    writer_notifier: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl<V> Deref for ReadOnlyViewReference<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl<V> Drop for ReadOnlyViewReference<V> {
    fn drop(&mut self) {
        if self.reader_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            let mut writer_notifier = self
                .writer_notifier
                .lock()
                .expect("No panics should happen while holding the `write_notifier` lock");

            if self.reader_count.load(Ordering::Acquire) == 0 {
                if let Some(notifier) = writer_notifier.take() {
                    let _ = notifier.send(());
                }
            }
        }
    }
}

/// A read-write reference to a [`SharedView`].
pub struct ReadWriteViewReference<V> {
    view: V,
    write_barrier: Option<oneshot::Receiver<()>>,
    _writer_permit: SemaphoreGuardArc,
}

impl<V> Deref for ReadWriteViewReference<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl<V> DerefMut for ReadWriteViewReference<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.view
    }
}

#[async_trait]
impl<C, V> View<C> for ReadWriteViewReference<V>
where
    C: Send + 'static,
    V: View<C>,
{
    fn context(&self) -> &C {
        self.deref().context()
    }

    async fn load(_context: C) -> Result<Self, ViewError> {
        unreachable!("`ReadWriteViewReference` should not be loaded directly");
    }

    fn rollback(&mut self) {
        self.deref_mut().rollback();
    }

    fn clear(&mut self) {
        self.deref_mut().clear();
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.deref_mut().flush(batch)
    }

    fn share_unchecked(&mut self) -> Result<Self, ViewError> {
        unreachable!(
            "`ReadWriteViewReference` should not be shared without going through its parent \
            `SharedView`"
        );
    }
}

#[async_trait]
impl<C, V> RootView<C> for ReadWriteViewReference<V>
where
    C: Context + Send + 'static,
    V: View<C> + Send,
    ViewError: From<C::Error>,
{
    async fn save(&mut self) -> Result<(), ViewError> {
        if let Some(barrier) = self.write_barrier.take() {
            let () = barrier
                .await
                .expect("`writer_notifier` should never be dropped without notifying first");
        }

        #[cfg(not(target_arch = "wasm32"))]
        increment_counter(&SAVE_VIEW_COUNTER, "SharedView", &self.context().base_key());

        let mut batch = Batch::new();
        self.flush(&mut batch)?;
        self.context().write_batch(batch).await?;
        Ok(())
    }
}
