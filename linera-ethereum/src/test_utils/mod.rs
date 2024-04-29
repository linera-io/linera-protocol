// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc};

use anyhow::Result;
use ethers::{
    contract::abigen,
    prelude::{ContractFactory, SignerMiddleware},
    providers::{Http, Provider},
    signers::LocalWallet,
    solc::Solc,
    types::{Address, U256},
};
use ethers::core::types::Bytes;
use ethers::abi::Abi;
use ethers_core::utils::{Anvil, AnvilInstance};
use ethers_signers::Signer;
use linera_storage_service::child::get_free_port;

use crate::client::EthereumEndpoint;

abigen!(
    SimpleContract,
    "./contracts/SimpleToken.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

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

pub struct ContractEndpoints {
    pub url: String,
    pub addr_contract: String,
    pub addr0: String,
    pub addr1: String,
    pub instance: AnvilTest,
}

pub fn get_abi_bytecode(contract_file: &str, contract_name: &str) -> (Abi, Bytes) {
    let full_contract_file = format!("contracts/{}", contract_file);
    let source = Path::new(&env!("CARGO_MANIFEST_DIR")).join(&full_contract_file);
    let compiled = Solc::default()
        .compile_source(source)
        .expect("Could not compile contracts");

    // 2. Access to the contract that interests us
    let (abi, bytecode, _runtime_bytecode) = compiled
        .find(contract_name)
        .expect("could not find contract")
        .into_parts_or_default();
    (abi, bytecode)
}



pub async fn get_test_contract_endpoints() -> anyhow::Result<ContractEndpoints> {
    // 1. Compile the code
    let (abi, bytecode) = get_abi_bytecode("simple_token.sol", "SimpleToken");

    // 3. Access to the wallets
    let anvil_test = get_anvil().await.unwrap();
    let url = anvil_test.endpoint.clone();
    let (wallet0, addr0) = anvil_test.get_wallet(0);
    let (_wallet1, addr1) = anvil_test.get_wallet(1);

    // 4. instantiate the client with the wallet
    let client0 = SignerMiddleware::new(anvil_test.ethereum_endpoint.provider.clone(), wallet0);
    let client0 = Arc::new(client0);

    // 5. create a factory which will be used to deploy instances of the contract
    let factory = ContractFactory::new(abi, bytecode, client0.clone());

    // 6. deploy it with the constructor arguments, note the `legacy` call
    let initial_supply = U256::from_dec_str("1000")?;
    let contract = factory.deploy(initial_supply)?.legacy().send().await?;

    // 7. get the contract's address
    let addr_contract = contract.address();

    // 8. instantiate the contract
    let contract = SimpleContract::new(addr_contract, client0.clone());

    // 9. call the `setValue` method
    // (first `await` returns a PendingTransaction, second wait for the mining)
    let value = U256::from_dec_str("10")?;
    let _receipt = contract
        .transfer(addr1, value)
        .legacy()
        .send()
        .await?
        .await?;
    // Testing of balances
    let value_read = contract.balance_of(addr1).call().await?;
    assert_eq!(value_read, value);
    let value_read = contract.balance_of(addr_contract).call().await?;
    assert_eq!(value_read, U256::from_dec_str("0")?);
    let value_read = contract.balance_of(addr0).call().await?;
    assert_eq!(value_read, U256::from_dec_str("990")?);
    // Returning of output
    let addr_contract = format!("{:?}", addr_contract);
    let addr0 = format!("{:?}", addr0);
    let addr1 = format!("{:?}", addr1);

    Ok(ContractEndpoints {
        url,
        addr_contract,
        addr0,
        addr1,
        instance: anvil_test,
    })
}
