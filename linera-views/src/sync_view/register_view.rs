// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option_or_default,
    context::Context,
    sync_view::{SyncReplaceContext, SyncView},
    ViewError,
};

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncRegisterView<C, T> {
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The value persisted in storage.
    stored_value: Box<T>,
    /// Pending update not yet persisted to storage.
    update: Option<Box<T>>,
}

impl<C, T, C2> SyncReplaceContext<C2> for SyncRegisterView<C, T>
where
    C: Context,
    C2: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = SyncRegisterView<C2, T>;

    fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SyncRegisterView {
            delete_storage_first: self.delete_storage_first,
            context: ctx(&self.context),
            stored_value: self.stored_value.clone(),
            update: self.update.clone(),
        }
    }
}

impl<C, T> SyncView for SyncRegisterView<C, T>
where
    C: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().bytes.clone()])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let stored_value = Box::new(value);
        Ok(Self {
            delete_storage_first: false,
            context,
            stored_value,
            update: None,
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.update = None;
    }

    fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        self.update.is_some()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key(self.context.base_key().bytes.clone());
            delete_view = true;
        } else if let Some(value) = &self.update {
            let key = self.context.base_key().bytes.clone();
            batch.put_key_value(key, value)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.delete_storage_first {
            *self.stored_value = Default::default();
        } else if let Some(value) = self.update.take() {
            self.stored_value = value;
        }
        self.delete_storage_first = false;
        self.update = None;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.update = Some(Box::default());
    }
}
