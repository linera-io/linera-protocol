// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use aws_smithy_http::endpoint::Endpoint;
use http::uri::InvalidUri;
use std::env;
use thiserror::Error;

/// Name of the environment variable with the address to a LocalStack instance.
pub const LOCALSTACK_ENDPOINT: &str = "LOCALSTACK_ENDPOINT";

/// Get the [`Endpoint`] to connect to a LocalStack instance.
///
/// The endpoint must be configured through a [`LOCALSTACK_ENDPOINT`] environment variable.
pub fn get_endpoint() -> Result<Endpoint, EndpointError> {
    let endpoint_address = env::var(LOCALSTACK_ENDPOINT)?.parse()?;
    Ok(Endpoint::immutable(endpoint_address))
}

/// Failure to get the LocalStack endpoint.
#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("Missing LocalStack endpoint address in {LOCALSTACK_ENDPOINT:?} environment variable")]
    MissingEndpoint(#[from] env::VarError),

    #[error("LocalStack endpoint address is not a valid URI")]
    InvalidUri(#[from] InvalidUri),
}
