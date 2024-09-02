// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use indexed_db_futures::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::wasm_bindgen;
use web_sys::DomException;

use super::LocalPersist;

/// An implementation of [`Persist`] based on an IndexedDB record with a given key.
#[derive(derive_more::Deref)]
pub struct IndexedDb<T> {
    key: String,
    #[deref]
    value: T,
    database: IdbDatabase,
}

const DATABASE_NAME: &str = "linera-client";
const STORE_NAME: &str = "linera-wallet";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("marshalling error: {0}")]
    Marshalling(#[source] gloo_utils::errors::JsError),
    #[error("key not found: {0}")]
    KeyNotFound(String),
    #[error("DOM exception: {0:?}")]
    DomException(#[source] gloo_utils::errors::JsError),
}

impl From<serde_wasm_bindgen::Error> for Error {
    fn from(e: serde_wasm_bindgen::Error) -> Self {
        Self::Marshalling(JsValue::from(e).try_into().unwrap())
    }
}

impl From<DomException> for Error {
    fn from(e: DomException) -> Self {
        Self::DomException(JsValue::from(e).try_into().unwrap())
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> IndexedDb<T> {
    pub async fn read_or_create<E>(
        key: &str,
        value: impl FnOnce() -> Result<T, E>,
    ) -> Result<Result<Self, E>, Error> {
        let mut db_req = IdbDatabase::open_u32(DATABASE_NAME, 1)?;
        db_req.set_on_upgrade_needed(Some(|evt: &IdbVersionChangeEvent| -> Result<(), JsValue> {
            if !evt.db().object_store_names().any(|n| n == STORE_NAME) {
                evt.db().create_object_store(STORE_NAME)?;
            }
            Ok(())
        }));

        let database: IdbDatabase = db_req.await?;
        let tx =
            database.transaction_on_one_with_mode(STORE_NAME, IdbTransactionMode::Readwrite)?;
        let store: IdbObjectStore = tx.object_store(STORE_NAME)?;

        Ok(if let Some(value) = store.get_owned(key)?.await? {
            drop(tx);
            Ok(Self {
                key: key.to_owned(),
                value: serde_wasm_bindgen::from_value(value)?,
                database,
            })
        } else {
            drop(tx);
            match value() {
                Ok(value) => {
                    let mut this = Self {
                        key: key.to_owned(),
                        value,
                        database,
                    };
                    this.persist().await?;
                    Ok(this)
                }
                Err(e) => Err(e),
            }
        })
    }

    pub async fn read(key: &str) -> Result<Self, Error> {
        Self::read_or_create(key, || Err(Error::KeyNotFound(key.to_owned()))).await?
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> LocalPersist for IndexedDb<T> {
    type Error = Error;

    fn as_mut(&mut self) -> &mut T {
        &mut self.value
    }

    fn into_value(self) -> T {
        self.value
    }

    async fn persist(&mut self) -> Result<(), Error> {
        self.database
            .transaction_on_one_with_mode(STORE_NAME, IdbTransactionMode::Readwrite)?
            .object_store(STORE_NAME)?
            .put_key_val_owned(&self.key, &serde_wasm_bindgen::to_value(&self.value)?)?
            .await?;
        Ok(())
    }
}
