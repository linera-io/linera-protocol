// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ethers::types::U256;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    client::EthereumEndpoint,
    common::{EthereumEvent, EthereumServiceError},
};

// The Oracle requests

#[derive(Serialize, Deserialize)]
pub struct EthereumBalanceRequest {
    pub address: String,
    pub block_number: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct EthereumBlockNumberRequest {}

#[derive(Serialize, Deserialize)]
pub struct EthereumEventsRequest {
    pub contract_address: String,
    pub event_name_expanded: String,
    pub starting_block: u64,
}

#[derive(Serialize, Deserialize)]
pub enum OracleRequest {
    EthereumBalance(EthereumBalanceRequest),
    EthereumBlockNumber(EthereumBlockNumberRequest),
    EthereumEvents(EthereumEventsRequest),
}

pub struct OracleEndpoints {
    ethereum_endpoint: String,
}

// The Oracle answers

#[derive(Serialize, Deserialize)]
pub struct EthereumBalanceAnswer {
    pub balance: U256,
}

#[derive(Serialize, Deserialize)]
pub struct EthereumBlockNumberAnswer {
    pub block_number: u64,
}

#[derive(Serialize, Deserialize)]
pub struct EthereumEventsAnswer {
    pub events: Vec<EthereumEvent>,
}

#[derive(Serialize, Deserialize)]
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
