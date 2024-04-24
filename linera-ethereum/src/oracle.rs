// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use num_bigint::BigUint;
use thiserror::Error;

use crate::{
    client::EthereumEndpoint,
    common::{EthereumEvent, EthereumServiceError},
};

// The Oracle requests

pub struct EthereumBalanceRequest {
    pub address: String,
    pub block_number: Option<u64>,
}

pub struct EthereumBlockNumberRequest {}

pub struct EthereumEventsRequest {
    pub contract_address: String,
    pub event_name_expanded: String,
    pub starting_block: u64,
}

pub enum OracleRequest {
    EthereumBalance(EthereumBalanceRequest),
    EthereumBlockNumber(EthereumBlockNumberRequest),
    EthereumEvents(EthereumEventsRequest),
}

pub struct OracleEndpoints {
    ethereum_endpoint: String,
}

// The Oracle answers

pub struct EthereumBalanceAnswer {
    pub balance: BigUint,
}

pub struct EthereumBlockNumberAnswer {
    pub block_number: u64,
}

pub struct EthereumEventsAnswer {
    pub events: Vec<EthereumEvent>,
}

pub enum OracleAnswer {
    EthereumBalance(EthereumBalanceAnswer),
    EthereumBlockNumber(EthereumBlockNumberAnswer),
    EthereumEvents(EthereumEventsAnswer),
}

#[derive(Debug, Error)]
pub enum OracleError {
    /// Ethereum service error
    #[error(transparent)]
    EthereumError(#[from] EthereumServiceError),
}

// Evaluating the Oracle request and returning the result
pub async fn evaluate_oracle(
    request: OracleRequest,
    oracle_endpoint: OracleEndpoints,
) -> Result<OracleAnswer, OracleError> {
    let url = oracle_endpoint.ethereum_endpoint;
    match request {
        OracleRequest::EthereumBalance(request) => {
            let ethereum_endpoint = EthereumEndpoint::new(url)?;
            let EthereumBalanceRequest {
                address,
                block_number,
            } = request;
            let balance = ethereum_endpoint
                .get_balance(&address, block_number)
                .await?;
            let answer = EthereumBalanceAnswer { balance };
            Ok(OracleAnswer::EthereumBalance(answer))
        }
        OracleRequest::EthereumBlockNumber(request) => {
            let ethereum_endpoint = EthereumEndpoint::new(url)?;
            let EthereumBlockNumberRequest {} = request;
            let block_number = ethereum_endpoint.get_block_number().await?;
            let answer = EthereumBlockNumberAnswer { block_number };
            Ok(OracleAnswer::EthereumBlockNumber(answer))
        }
        OracleRequest::EthereumEvents(request) => {
            let ethereum_endpoint = EthereumEndpoint::new(url)?;
            let EthereumEventsRequest {
                contract_address,
                event_name_expanded,
                starting_block,
            } = request;
            let events = ethereum_endpoint
                .read_events(&contract_address, &event_name_expanded, starting_block)
                .await?;
            let answer = EthereumEventsAnswer { events };
            Ok(OracleAnswer::EthereumEvents(answer))
        }
    }
}
