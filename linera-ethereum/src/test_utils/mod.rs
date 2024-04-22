// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use ethers::{
    providers::{Http, Provider},
    signers::LocalWallet,
    types::Address,
};
use ethers_core::utils::{Anvil, AnvilInstance};
use ethers_signers::Signer;
use linera_storage_service::child::get_free_port;

use crate::client::EthereumEndpoint;

pub async fn get_provider(url: &str) -> Provider<Http> {
    Provider::try_from(url).unwrap()
}

pub struct AnvilTest {
    pub anvil_instance: AnvilInstance,
    pub endpoint: String,
    pub ethereum_endpoint: EthereumEndpoint,
}

pub async fn get_anvil() -> Result<AnvilTest> {
    let port = get_free_port().await?;
    let anvil_instance = Anvil::new()
        .port(port)
        .mnemonic("abstract vacuum mammal awkward pudding scene penalty purchase dinner depart evoke puzzle")
        .spawn();
    let endpoint = anvil_instance.endpoint();
    let ethereum_endpoint = EthereumEndpoint::new(endpoint.clone())?;
    Ok(AnvilTest {
        anvil_instance,
        endpoint,
        ethereum_endpoint,
    })
}

impl AnvilTest {
    pub fn get_wallet(&self, index: usize) -> (LocalWallet, Address) {
        let address = self.anvil_instance.addresses()[index];
        let wallet: LocalWallet = self.anvil_instance.keys()[index].clone().into();
        let wallet = wallet.with_chain_id(self.anvil_instance.chain_id());
        (wallet, address)
    }
}
