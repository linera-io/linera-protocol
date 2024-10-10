// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types used when performing HTTP requests.

use linera_witty::{WitLoad, WitStore, WitType};

/// The method used in an HTTP request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitLoad, WitStore, WitType)]
#[witty(name = "http-method")]
pub enum Method {
    /// A GET request.
    Get,

    /// A POST request.
    Post,

    /// A PUT request.
    Put,

    /// A DELETE request.
    Delete,

    /// A HEAD request.
    Head,

    /// A OPTIONS request.
    Options,

    /// A CONNECT request.
    Connect,

    /// A PATCH request.
    Patch,

    /// A TRACE request.
    Trace,
}

#[cfg(with_reqwest)]
impl From<Method> for reqwest::Method {
    fn from(method: Method) -> Self {
        match method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
            Method::Put => reqwest::Method::PUT,
            Method::Delete => reqwest::Method::DELETE,
            Method::Head => reqwest::Method::HEAD,
            Method::Options => reqwest::Method::OPTIONS,
            Method::Connect => reqwest::Method::CONNECT,
            Method::Patch => reqwest::Method::PATCH,
            Method::Trace => reqwest::Method::TRACE,
        }
    }
}
