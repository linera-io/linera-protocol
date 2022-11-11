use async_trait::async_trait;
use serde::Serialize;
use std::{
    fmt::Debug,
};
use crate::views::{View, ViewError};
use serde::de::DeserializeOwned;
use crate::common::{Context, Batch};

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Clone)]
pub struct RegisterView<C, T> {
    context: C,
    stored_value: T,
    update: Option<T>,
}

/// The context operations supporting [`RegisterView`].
#[async_trait]
pub trait RegisterOperations<T>: Context {
    /// Obtain the value in the register.
    async fn get(&mut self) -> Result<T, Self::Error>;

    /// Set the value in the register. Crash-resistant implementations should only write to `batch`.
    fn set(&mut self, batch: &mut Batch, value: &T) -> Result<(), Self::Error>;

    /// Delete the register. Crash-resistant implementations should only write to `batch`.
    fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send> RegisterOperations<T> for C
where
    T: Default + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&mut self) -> Result<T, Self::Error> {
        let base = self.base_key();
        let value = self.read_key(&base).await?.unwrap_or_default();
        Ok(value)
    }

    fn set(&mut self, batch: &mut Batch, value: &T) -> Result<(), Self::Error> {
        batch.put_key_value(self.base_key(), value)?;
        Ok(())
    }

    fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error> {
        batch.delete_key(self.base_key());
        Ok(())
    }
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: RegisterOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Default,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, ViewError> {
        let stored_value = context.get().await?;
        Ok(Self {
            context,
            stored_value,
            update: None,
        })
    }

    fn rollback(&mut self) {
        self.update = None
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if let Some(value) = self.update.take() {
            self.context.set(batch, &value)?;
            self.stored_value = value;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(batch)?;
        Ok(())
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

