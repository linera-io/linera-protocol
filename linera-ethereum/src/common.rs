// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::ParseIntError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EthereumServiceError {
    /// Parsing error
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),

    #[error("Failed to deploy the smart contract")]
    DeployError,

    #[error("Json error occurred")]
    JsonError,

    /// Url Pare error
    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),

    /// Provider error
    #[error(transparent)]
    ProviderError(#[from] ethers_providers::ProviderError),

    /// Url Pare error
    #[error(transparent)]
    FromHexError(#[from] rustc_hex::FromHexError),
}
