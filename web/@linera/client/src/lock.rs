// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use wasm_bindgen::{JsCast as _, JsValue, UnwrapThrowExt as _};
use wasm_bindgen_futures::JsFuture;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("wallet {name} already in use")]
    Contended { name: String },
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Lock {
    #[serde(with = "serde_wasm_bindgen::preserve")]
    lock: web_sys::Lock,
    #[serde(with = "serde_wasm_bindgen::preserve")]
    release: js_sys::Function,
}

impl Lock {
    pub async fn try_acquire(name: &str) -> Result<Self, Error> {
        let options = web_sys::LockOptions::new();
        options.set_if_available(true);

        let value = JsFuture::from(js_sys::Promise::new(&mut |resolve, _reject| {
            let callback =
                wasm_bindgen::closure::Closure::once(move |lock: Option<web_sys::Lock>| {
                    js_sys::Promise::new(&mut |release, _reject| {
                        let value = if let Some(lock) = &lock {
                            tracing::debug!(name = lock.name(), "acquired lock");
                            Some(Self {
                                lock: lock.clone(),
                                release,
                            })
                        } else {
                            tracing::debug!(name = lock.name(), "failed to acquire lock");
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
        tracing::debug!(name = self.lock.name(), "releasing lock");
        let _: JsValue = self.release.call0(&JsValue::UNDEFINED).unwrap_throw();
    }
}
