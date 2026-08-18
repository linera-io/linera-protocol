// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use wasm_bindgen::{prelude::wasm_bindgen, JsError, JsValue};

use crate::lock;

#[wasm_bindgen(raw_module = "../error/index.js")]
extern "C" {
    pub type LockError;

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
    #[must_use]
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

/// The depth at which a `cause` chain stops being followed. A chain can be cyclic, since
/// nothing stops JavaScript from making an error its own cause.
const MAX_CAUSE_DEPTH: usize = 8;

/// A value thrown by JavaScript, captured on the Rust side.
///
/// JavaScript can throw any value at all, so rather than mapping the throw onto a fixed
/// set of Rust variants this records what it carried: its `name` and `message`, its stack
/// trace, and its `cause`, recursively. Properties are read reflectively, so
/// `DOMException`s, user-defined error classes and errors thrown in another realm are
/// captured too — none of those are `instanceof Error` on this side.
#[derive(Debug)]
pub struct Thrown {
    description: String,
    stack: Option<String>,
    cause: Option<Box<Thrown>>,
}

impl Thrown {
    /// The JavaScript stack trace of the thrown value, if it carried one.
    #[must_use]
    pub fn stack(&self) -> Option<&str> {
        self.stack.as_deref()
    }

    fn capture(value: &JsValue, depth: usize) -> Self {
        let property = |key: &str| {
            js_sys::Reflect::get(value, &JsValue::from_str(key))
                .ok()
                .filter(|value| !value.is_undefined() && !value.is_null())
        };

        // Anything without a string `message` is not error-like: `throw 42`,
        // `throw "boom"`, `throw {code: 3}` are all legal. Report the value itself, as
        // JSON where that is possible and as its Rust-side debug form otherwise
        // (`undefined`, symbols, anything cyclic).
        let Some(message) = property("message").and_then(|message| message.as_string()) else {
            return Self {
                description: value
                    .as_string()
                    .or_else(|| {
                        js_sys::JSON::stringify(value)
                            .ok()
                            .and_then(|json| json.as_string())
                    })
                    .unwrap_or_else(|| format!("{value:?}")),
                stack: None,
                cause: None,
            };
        };

        // `name` is `"Error"` for the base class, where repeating it says nothing.
        let description = match property("name").and_then(|name| name.as_string()) {
            Some(name) if message.is_empty() => name,
            Some(name) if name != "Error" => format!("{name}: {message}"),
            _ => message,
        };

        Self {
            description: if description.is_empty() {
                format!("{value:?}")
            } else {
                description
            },
            stack: property("stack").and_then(|stack| stack.as_string()),
            cause: (depth < MAX_CAUSE_DEPTH)
                .then(|| property("cause"))
                .flatten()
                .map(|cause| Box::new(Self::capture(&cause, depth + 1))),
        }
    }
}

impl From<JsValue> for Thrown {
    fn from(value: JsValue) -> Self {
        Self::capture(&value, 0)
    }
}

impl std::fmt::Display for Thrown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.description)
    }
}

impl std::error::Error for Thrown {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .map(|cause| cause.as_ref() as &(dyn std::error::Error + 'static))
    }
}
