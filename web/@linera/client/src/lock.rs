// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Browser-based wallet locking using the Web Locks API.
//!
//! This module provides a mechanism to ensure exclusive access to a wallet within a
//! browser context. It uses the [Web Locks API] to coordinate between multiple tabs or
//! windows that might try to access the same wallet simultaneously.
//!
//! The lock is automatically released when the [`Lock`] is dropped.
//!
//! # Example
//!
//! ```ignore
//! let lock = Lock::try_acquire("my-wallet").await?;
//! // Wallet is now exclusively locked for this context
//! // Lock is automatically released when `lock` goes out of scope
//! ```
//!
//! [Web Locks API]: https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API

use wasm_bindgen::{JsCast as _, JsValue, UnwrapThrowExt as _};
use wasm_bindgen_futures::JsFuture;

/// Errors that can occur when acquiring a lock.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The lock could not be acquired because another context already holds it.
    #[error("wallet {name} already in use")]
    Contended { name: String },
}

/// An exclusive lock on a named resource, backed by the Web Locks API.
///
/// This struct represents an acquired lock. The lock is held for as long as this value
/// exists and is automatically released when dropped.
///
/// The struct is serializable to allow transfer across WASM boundaries while preserving
/// the underlying JavaScript objects.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct Lock {
    /// The underlying Web Locks API lock object.
    #[serde(with = "serde_wasm_bindgen::preserve")]
    lock: web_sys::Lock,
    /// A JavaScript function that, when called, resolves the promise and releases the lock.
    #[serde(with = "serde_wasm_bindgen::preserve")]
    release: js_sys::Function,
}

impl Lock {
    /// Attempts to acquire an exclusive lock on the given name without blocking.
    ///
    /// This uses the Web Locks API with the `ifAvailable` option, meaning it will
    /// immediately return an error if the lock is already held rather than waiting.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lock to acquire. This should uniquely identify the
    ///   resource being protected (e.g., a wallet identifier).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Contended`] if the lock is already held by another context.
    ///
    /// # Panics
    ///
    /// Panics if not running in a browser context with access to `window.navigator.locks`.
    pub async fn try_acquire(name: &str) -> Result<Self, Error> {
        let options = web_sys::LockOptions::new();
        options.set_if_available(true);

        let value = JsFuture::from(js_sys::Promise::new(&mut |resolve, _reject| {
            let callback =
                wasm_bindgen::closure::Closure::once(move |lock: Option<web_sys::Lock>| {
                    js_sys::Promise::new(&mut |release, _reject| {
                        let value = if let Some(lock) = &lock {
                            Some(Self {
                                lock: lock.clone(),
                                release,
                            })
                        } else {
                            release.call0(&JsValue::NULL).unwrap_throw();
                            None
                        };

                        resolve
                            .call1(
                                &JsValue::NULL,
                                &serde_wasm_bindgen::to_value(&value).unwrap_throw(),
                            )
                            .unwrap_throw();

                        std::mem::forget(value);
                    })
                });

            let _: js_sys::Promise = web_sys::window()
                .expect("we need to run in a document context")
                .navigator()
                .locks()
                .request_with_options_and_callback(
                    name,
                    &options,
                    callback.as_ref().unchecked_ref(),
                );

            callback.forget();
        }))
        .await
        .unwrap_throw();

        serde_wasm_bindgen::from_value::<Option<Self>>(value)
            .unwrap_throw()
            .ok_or_else(|| Error::Contended { name: name.into() })
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _: JsValue = self.release.call0(&JsValue::UNDEFINED).unwrap_throw();
    }
}
