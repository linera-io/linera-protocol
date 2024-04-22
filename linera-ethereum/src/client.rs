// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use ethers::{
    prelude::{Http, Provider},
    types::{Log, U256},
};
use ethers_core::types::{Address, BlockId, BlockNumber, Filter, U64};
use ethers_middleware::Middleware;

use crate::common::EthereumServiceError;

pub struct EthereumEndpoint {
    pub url: String,
    pub provider: Provider<Http>,
}

impl EthereumEndpoint {
    pub fn new(url: String) -> Result<Self, EthereumServiceError> {
        let provider = Provider::<Http>::try_from(&url)?;
        let provider = provider.interval(Duration::from_millis(10u64));
        Ok(Self { url, provider })
    }

    pub async fn get_accounts(&self) -> Result<Vec<String>, EthereumServiceError> {
        Ok(self
            .provider
            .get_accounts()
            .await?
            .into_iter()
            .map(|x| format!("{:?}", x))
            .collect::<Vec<_>>())
    }

    pub async fn get_block_number(&self) -> Result<u64, EthereumServiceError> {
        let block_number = self.provider.get_block_number().await?;
        Ok(block_number.as_u64())
    }

    pub async fn get_balance(
        &self,
        address: &str,
        block_nr: Option<u64>,
    ) -> Result<U256, EthereumServiceError> {
        let address = address.parse::<Address>()?;
        let block_nr = match block_nr {
            None => None,
            Some(val) => {
                let val: U64 = val.into();
                let val: BlockNumber = BlockNumber::Number(val);
                Some(BlockId::Number(val))
            }
        };
        let balance = self.provider.get_balance(address, block_nr).await?;
        Ok(balance)
    }

    pub async fn read_events(
        &self,
        contract_address: &str,
        event_name: &str,
        starting_block: u64,
    ) -> Result<Vec<Log>, EthereumServiceError> {
        let address = contract_address.parse::<Address>()?;
        let filter = Filter::new()
            .address(address)
            .event(event_name)
            .from_block(starting_block);
        Ok(self.provider.get_logs(&filter).await?)
    }
}
