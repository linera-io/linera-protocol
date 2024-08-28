// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::Deref;

pub use gloo_storage::errors::StorageError as Error;
use gloo_storage::Storage as _;

use super::{Dirty, Persist};

#[derive(DerefMut)]
pub struct LocalStorage<T> {
    key: String,
    #[deref_mut]
    value: T,
    dirty: Dirty,
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> LocalStorage<T> {
    pub fn read_or_create(
        key: &str,
        value: impl FnOnce() -> Result<T, Error>,
    ) -> Result<Self, Error> {
        match gloo_storage::LocalStorage::get(key) {
            Err(Error::KeyNotFound(key)) => {
                let mut this = Self {
                    key,
                    value: value()?,
                };
                Persist::persist(&mut this)?;
                Ok(this)
            }
            result => Ok(Self {
                key: key.to_owned(),
                value: result?,
            }),
        }
    }

    pub fn read(key: &str) -> Result<Self, Error> {
        Self::read_or_create(key, || Err(Error::KeyNotFound(key.to_owned())))
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> Persist for LocalStorage<T> {
    type Error = Error;

    fn as_mut(this: &mut Self) -> &mut T {
        &mut this.value
    }

    fn into_value(this: Self) -> T {
        this.value
    }

    fn persist(this: &mut Self) -> Result<(), Error> {
        gloo_storage::LocalStorage::set(&this.key, &this.value)
    }
}
