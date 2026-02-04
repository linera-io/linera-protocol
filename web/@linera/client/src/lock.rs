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
                        resolve
                            .call1(
                                &JsValue::NULL,
                                &serde_wasm_bindgen::to_value(&if let Some(lock) = &lock {
                                    Some(Self {
                                        lock: lock.clone(),
                                        release,
                                    })
                                } else {
                                    release.call0(&JsValue::NULL).unwrap_throw();
                                    None
                                })
                                .unwrap_throw(),
                            )
                            .unwrap_throw();
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
