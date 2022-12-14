use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, HashingContext, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Clone)]
pub struct RegisterView<C, T> {
    context: C,
    stored_value: T,
    update: Option<T>,
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Default + Serialize + DeserializeOwned,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let base = context.base_key();
        let stored_value = context.read_key(&base).await?.unwrap_or_default();
        Ok(Self {
            context,
            stored_value,
            update: None,
        })
    }

    fn rollback(&mut self) {
        self.update = None
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if let Some(value) = self.update.take() {
            batch.put_key_value(self.context.base_key(), &value)?;
            self.stored_value = value;
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key(self.context.base_key());
    }

    fn clear(&mut self) {
        self.update = Some(T::default())
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
{
    /// Access the current value in the register.
    pub fn get(&self) -> &T {
        match &self.update {
            None => &self.stored_value,
            Some(value) => value,
        }
    }

    /// Set the value in the register.
    pub fn set(&mut self, value: T) {
        self.update = Some(value);
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
    T: Clone,
{
    /// Obtain a mutable reference to the value in the register.
    pub fn get_mut(&mut self) -> &mut T {
        match &mut self.update {
            Some(value) => value,
            update => {
                *update = Some(self.stored_value.clone());
                update.as_mut().unwrap()
            }
        }
    }
}

#[async_trait]
impl<C, T> HashView<C> for RegisterView<C, T>
where
    C: HashingContext + Send + Sync,
    ViewError: From<C::Error>,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = C::Hasher;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        hasher.update_with_bcs_bytes(self.get())?;
        Ok(hasher.finalize())
    }
}
