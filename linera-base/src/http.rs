// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types used when performing HTTP requests.

use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};

/// An HTTP request.
#[derive(Clone, Debug, Eq, PartialEq, WitLoad, WitStore, WitType)]
pub struct Request {
    /// The [`Method`] used for the HTTP request.
    pub method: Method,

    /// The URL this request is intended to.
    pub url: String,

    /// The headers that should be included in the request.
    pub headers: Vec<(String, Vec<u8>)>,

    /// The body of the request.
    pub body: Vec<u8>,
}

impl Request {
    /// Creates an HTTP GET [`Request`] for a `url`.
    pub fn get(url: impl Into<String>) -> Self {
        Request {
            method: Method::Get,
            url: url.into(),
            headers: vec![],
            body: vec![],
        }
    }

    /// Creates an HTTP POST [`Request`] for a `url` with a `payload` that's an arbitrary bytes.
    pub fn post(url: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Request {
            method: Method::Post,
            url: url.into(),
            headers: vec![],
            body: payload.into(),
        }
    }

    /// Creates an HTTP POST [`Request`] for a `url` with a body that's the `payload` serialized to
    /// JSON.
    pub fn post_json(
        url: impl Into<String>,
        payload: &impl Serialize,
    ) -> Result<Self, serde_json::Error> {
        Ok(Request {
            method: Method::Post,
            url: url.into(),
            headers: vec![("Content-Type".to_owned(), b"application/json".to_vec())],
            body: serde_json::to_vec(payload)?,
        })
    }

    /// Adds a header to this [`Request`].
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// The method used in an HTTP request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitLoad, WitStore, WitType)]
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

/// A response for an HTTP request.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, WitLoad, WitStore, WitType)]
pub struct Response {
    /// The status code of the HTTP response.
    pub status: u16,

    /// The headers included in the response.
    pub headers: Vec<(String, Vec<u8>)>,

    /// The body of the response.
    pub body: Vec<u8>,
}

#[cfg(with_reqwest)]
impl Response {
    /// Creates a [`Response`] from a [`reqwest::Response`], waiting for it to be fully
    /// received.
    pub async fn from_reqwest(response: reqwest::Response) -> reqwest::Result<Self> {
        let headers = response
            .headers()
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.as_bytes().to_owned()))
            .collect();

        Ok(Response {
            status: response.status().as_u16(),
            headers,
            body: response.bytes().await?.to_vec(),
        })
    }
}
