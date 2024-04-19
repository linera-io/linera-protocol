// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use ethers::{
    prelude::{Http, Provider},
    types::{Log, U256},
};
use ethers_core::types::{Address, BlockId, BlockNumber, Filter, U64};
use ethers_middleware::Middleware;

use crate::common::EthereumServiceError;

pub async fn get_accounts(url: &str) -> Result<Vec<String>, EthereumServiceError> {
    let provider = Provider::<Http>::try_from(url)?;
    Ok(provider
        .get_accounts()
        .await?
        .into_iter()
        .map(|x| format!("{:?}", x))
        .collect::<Vec<_>>())
}

pub async fn get_block_number(url: &str) -> Result<u64, EthereumServiceError> {
    let provider = Provider::<Http>::try_from(url)?;
    let block_number = provider.get_block_number().await?;
    Ok(block_number.as_u64())
}

pub async fn get_balance(
    url: &str,
    address: &str,
    block_nr: Option<u64>,
) -> Result<U256, EthereumServiceError> {
    let provider = Provider::<Http>::try_from(url)?;
    let address = address.parse::<Address>()?;
    let block_nr = match block_nr {
        None => None,
        Some(val) => {
            let val: U64 = val.into();
            let val: BlockNumber = BlockNumber::Number(val);
            Some(BlockId::Number(val))
        }
    };
    let balance = provider.get_balance(address, block_nr).await?;
    Ok(balance)
}

pub async fn read_events(
    url: &str,
    address: &str,
    event_name: &str,
    starting_block: u64,
) -> Result<Vec<Log>, EthereumServiceError> {
    let provider = Provider::<Http>::try_from(url)?;
    let client = Arc::new(provider);
    let address = address.parse::<Address>()?;
    let filter = Filter::new()
        .address(address)
        .event(event_name)
        .from_block(starting_block);
    Ok(client.get_logs(&filter).await?)
}
