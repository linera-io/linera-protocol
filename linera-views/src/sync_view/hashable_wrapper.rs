// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Mutex,
};

use allocative::Allocative;
use linera_base::visit_allocative_simple;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option,
    context::Context,
    sync_view::{
        SyncClonableView, SyncHashableView, SyncReplaceContext, SyncView, MIN_VIEW_TAG,
    },
    ViewError,
};

/// Wrapping a view to memoize its hash.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, O, W: Allocative")]
pub struct WrappedHashableContainerView<C, W, O> {
    /// Phantom data for the context type.
    #[allocative(skip)]
    _phantom: PhantomData<C>,
    /// The hash persisted in storage.
    #[allocative(visit = visit_allocative_simple)]
    stored_hash: Option<O>,
    /// Memoized hash, if any.
    #[allocative(visit = visit_allocative_simple)]
    hash: Mutex<Option<O>>,
    /// The wrapped view.
    inner: W,
}

/// Key tags to create the sub-keys of a `WrappedHashableContainerView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Inner = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

impl<C, W, O, C2> SyncReplaceContext<C2> for WrappedHashableContainerView<C, W, O>
where
    W: SyncHashableView<Hasher: crate::sync_view::Hasher<Output = O>, Context = C>
        + SyncReplaceContext<C2>,
    <W as SyncReplaceContext<C2>>::Target: SyncHashableView<Hasher: crate::sync_view::Hasher<Output = O>>,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    C: Context,
    C2: Context,
{
    type Target = WrappedHashableContainerView<C2, <W as SyncReplaceContext<C2>>::Target, O>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let hash = *self.hash.lock().unwrap();
        WrappedHashableContainerView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            hash: Mutex::new(hash),
            inner: self.inner.with_context(ctx),
        }
    }
}

impl<W: SyncHashableView, O> SyncView for WrappedHashableContainerView<W::Context, W, O>
where
    W: SyncHashableView<Hasher: crate::sync_view::Hasher<Output = O>>,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
{
    const NUM_INIT_KEYS: usize = 1 + W::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> Self::Context {
        // The inner context has our base key + the KeyTag::Inner byte
        self.inner.context().clone_with_trimmed_key(1)
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut v = vec![context.base_key().base_tag(KeyTag::Hash as u8)];
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        v.extend(W::pre_load(&context)?);
        Ok(v)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let hash = from_bytes_option(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        let inner = W::post_load(
            context,
            values.get(1..).ok_or(ViewError::PostLoadValuesError)?,
        )?;
        Ok(Self {
            _phantom: PhantomData,
            stored_hash: hash,
            hash: Mutex::new(hash),
            inner,
        })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
        *self.hash.get_mut().unwrap() = self.stored_hash;
    }

    fn has_pending_changes(&self) -> bool {
        if self.inner.has_pending_changes() {
            return true;
        }
        let hash = self.hash.lock().unwrap();
        self.stored_hash != *hash
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let delete_view = self.inner.pre_save(batch)?;
        let hash = *self.hash.lock().unwrap();
        if delete_view {
            let mut key_prefix = self.inner.context().base_key().bytes.clone();
            key_prefix.pop();
            batch.delete_key_prefix(key_prefix);
        } else if self.stored_hash != hash {
            let mut key = self.inner.context().base_key().bytes.clone();
            let tag = key.last_mut().unwrap();
            *tag = KeyTag::Hash as u8;
            match hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        self.inner.post_save();
        let hash = *self.hash.get_mut().unwrap();
        self.stored_hash = hash;
    }

    fn clear(&mut self) {
        self.inner.clear();
        *self.hash.get_mut().unwrap() = None;
    }
}

impl<W, O> SyncClonableView for WrappedHashableContainerView<W::Context, W, O>
where
    W: SyncHashableView + SyncClonableView,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: crate::sync_view::Hasher<Output = O>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(WrappedHashableContainerView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            hash: Mutex::new(*self.hash.get_mut().unwrap()),
            inner: self.inner.clone_unchecked()?,
        })
    }
}

impl<W, O> SyncHashableView for WrappedHashableContainerView<W::Context, W, O>
where
    W: SyncHashableView,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: crate::sync_view::Hasher<Output = O>,
{
    type Hasher = W::Hasher;

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        let hash = *self.hash.get_mut().unwrap();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.inner.hash_mut()?;
                let hash = self.hash.get_mut().unwrap();
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        let hash = *self.hash.lock().unwrap();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.inner.hash()?;
                let mut hash = self.hash.lock().unwrap();
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}

impl<C, W, O> Deref for WrappedHashableContainerView<C, W, O> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.inner
    }
}

impl<C, W, O> DerefMut for WrappedHashableContainerView<C, W, O> {
    fn deref_mut(&mut self) -> &mut W {
        *self.hash.get_mut().unwrap() = None;
        &mut self.inner
    }
}

