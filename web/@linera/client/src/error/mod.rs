// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

use crate::lock;

#[wasm_bindgen(raw_module = "../error/index.js")]
extern "C" {
    pub type LockError;

    #[wasm_bindgen(constructor)]
    fn new(message: String) -> LockError;
}

#[wasm_bindgen]
extern "C" {
    /// JavaScript's `String` function, which stringifies `undefined`, symbols and bigints
    /// where `JSON.stringify` cannot. It does throw for an object whose `toString` throws
    /// or is absent, so the result is caught rather than left to unwind through wasm.
    #[wasm_bindgen(catch, js_name = "String")]
    fn js_string(value: &JsValue) -> Result<String, JsValue>;
}

pub enum Error {
    Lock(lock::Error),
    Other(js_sys::Error),
}

impl From<lock::Error> for Error {
    fn from(error: lock::Error) -> Self {
        Self::Lock(error)
    }
}

impl<E: std::error::Error + 'static> From<E> for Error {
    fn from(error: E) -> Self {
        Self::Other(to_js_error(&error))
    }
}

impl Error {
    #[must_use]
    pub fn new(message: &str) -> Self {
        Self::Other(js_sys::Error::new(message))
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

/// Builds the JavaScript `Error` thrown in place of a Rust error.
///
/// Each layer of a Rust error chain prints the layer below it, so the message already
/// carries the whole chain. What it cannot carry is where the failure happened in
/// JavaScript: the error constructed here records only the wasm frames that built it. So
/// the `source` chain is walked for the first [`Thrown`], and its stack is used instead,
/// pointing at the code that actually threw.
///
/// The stack's first line is replaced with this error's own message, because JavaScript
/// tooling — the browser console, and Sentry — reads a stack as `Name: message` followed by
/// frames, and would otherwise report a message contradicting `.message`.
fn to_js_error(error: &(dyn std::error::Error + 'static)) -> js_sys::Error {
    let js_error = js_sys::Error::new(&error.to_string());

    let mut next = Some(error);
    while let Some(error) = next {
        if let Some(stack) = error.downcast_ref::<Thrown>().and_then(Thrown::stack) {
            let frames = stack.split_once('\n').map_or("", |(_, frames)| frames);
            let rewritten = format!("Error: {}\n{frames}", js_error.message());
            let _ = js_sys::Reflect::set(
                &js_error,
                &JsValue::from_str("stack"),
                &JsValue::from_str(&rewritten),
            );
            break;
        }
        next = error.source();
    }

    js_error
}

/// The depth at which a `cause` chain stops being followed. A chain can be cyclic, since
/// nothing stops JavaScript from making an error its own cause.
const MAX_CAUSE_DEPTH: usize = 8;

/// A value thrown by JavaScript, captured on the Rust side.
///
/// JavaScript can throw any value at all, so rather than mapping the throw onto a fixed
/// set of Rust variants this records what it carried: its `name` and `message`, its stack
/// trace, and its `cause`, recursively. Properties are read reflectively, so `DOMException`s,
/// user-defined error classes and errors thrown in another realm are captured too — none
/// of those are `instanceof Error` on this side.
///
/// What is captured is owned, rather than the `JsValue` it came from, so the error is
/// `Send + Sync` and satisfies `TaskSendable` on both sides of the `cfg(web)` split.
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
        // `throw "boom"`, `throw {code: 3}` are all legal.
        let Some(message) = property("message").and_then(|message| message.as_string()) else {
            return Self::leaf(describe(value));
        };

        // `name` is `"Error"` for the base class, where repeating it says nothing.
        let description = match property("name").and_then(|name| name.as_string()) {
            Some(name) if message.is_empty() => name,
            Some(name) if name != "Error" => format!("{name}: {message}"),
            _ => message,
        };

        Self {
            description: if description.is_empty() {
                describe(value)
            } else {
                description
            },
            stack: property("stack").and_then(|stack| stack.as_string()),
            cause: property("cause").map(|cause| {
                Box::new(if depth < MAX_CAUSE_DEPTH {
                    Self::capture(&cause, depth + 1)
                } else {
                    Self::leaf("(further causes omitted)".to_owned())
                })
            }),
        }
    }

    fn leaf(description: String) -> Self {
        Self {
            description,
            stack: None,
            cause: None,
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
        f.write_str(&self.description)?;
        if let Some(cause) = &self.cause {
            write!(f, ": {cause}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Thrown {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .map(|cause| cause.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Describes a thrown value that is not error-like, preferring JSON so that its contents
/// survive, and falling back to `String(value)` for what JSON cannot represent:
/// `undefined`, symbols, bigints, and anything cyclic.
///
/// Never fails: a value that defeats every rung — an object that throws from both
/// `toJSON` and `toString`, say — is still described, just not by its contents.
fn describe(value: &JsValue) -> String {
    value
        .as_string()
        .or_else(|| {
            js_sys::JSON::stringify(value)
                .ok()
                .and_then(|json| json.as_string())
        })
        .or_else(|| js_string(value).ok())
        .unwrap_or_else(|| "(unrepresentable value)".to_owned())
}
