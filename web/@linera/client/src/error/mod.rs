// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use wasm_bindgen::{prelude::wasm_bindgen, JsError, JsValue};

use crate::lock;

#[wasm_bindgen(module = "/src/error/index.ts")]
extern "C" {
    type LockError;

    #[wasm_bindgen(constructor)]
    fn new(message: String) -> LockError;
}

pub enum Error {
    Lock(lock::Error),
    Other(JsError),
}

impl From<lock::Error> for Error {
    fn from(error: lock::Error) -> Self {
        Self::Lock(error)
    }
}

impl<E: std::error::Error> From<E> for Error {
    fn from(error: E) -> Self {
        Self::Other(error.into())
    }
}

impl Error {
    pub fn new(message: &str) -> Self {
        Self::Other(JsError::new(message))
    }
}

impl From<Error> for JsValue {
    fn from(error: Error) -> Self {
        match error {
            Error::Lock(error) => LockError::new(error.to_string()).into(),
            Error::Other(error) => error.into(),
        }
    }
}
