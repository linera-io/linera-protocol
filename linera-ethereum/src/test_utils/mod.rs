// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc};

use anyhow::Result;
use ethers::{
    abi::Abi,
    contract::abigen,
    core::{k256::ecdsa::SigningKey, types::Bytes},
    prelude::{ContractFactory, SignerMiddleware},
    providers::{Http, Provider},
    signers::LocalWallet,
    solc::Solc,
    types::{transaction::eip2718::TypedTransaction, Address, U256},
};
use ethers_core::utils::{Anvil, AnvilInstance};
use ethers_signers::{Signer, Wallet};
use linera_storage_service::child::get_free_port;

use crate::client::EthereumEndpoint;

abigen!(
    SimpleTokenContract,
    "./contracts/SimpleToken.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

abigen!(
    EventNumericsContract,
    "./contracts/EventNumerics.json",
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
    pub fn get_wallet(&self, index: usize) -> (LocalWallet, String) {
        let address = self.anvil_instance.addresses()[index];
        let address = format!("{:?}", address);
        let wallet: LocalWallet = self.anvil_instance.keys()[index].clone().into();
        let wallet = wallet.with_chain_id(self.anvil_instance.chain_id());
        (wallet, address)
    }

    pub fn get_address(&self, index: usize) -> String {
        let address = self.anvil_instance.addresses()[index];
        format!("{:?}", address)
    }
}

pub fn get_abi_bytecode(contract_file: &str, contract_name: &str) -> (Abi, Bytes) {
    let full_contract_file = format!("contracts/{}", contract_file);
    let source = Path::new(&env!("CARGO_MANIFEST_DIR")).join(full_contract_file);
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

pub struct SimpleTokenContractFunction {
    pub simple_token: SimpleTokenContract<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
    pub contract_address: String,
    pub wallet_info: (LocalWallet, String),
    pub anvil_test: AnvilTest,
}

impl SimpleTokenContractFunction {
    pub async fn new(anvil_test: AnvilTest) -> Result<Self> {
        // 1. Getting the code
        let (abi, bytecode) = get_abi_bytecode("simple_token.sol", "SimpleToken");

        // 2. Reading the client
        let wallet_info = anvil_test.get_wallet(0);
        let client0 = SignerMiddleware::new(
            anvil_test.ethereum_endpoint.provider.clone(),
            wallet_info.0.clone(),
        );
        let client0 = Arc::new(client0);

        // 3. Factory
        let factory = ContractFactory::new(abi, bytecode, client0.clone());

        // 6. deploy it with the constructor arguments, note the `legacy` call
        let initial_supply = U256::from_dec_str("1000")?;
        let contract = factory.deploy(initial_supply)?.legacy().send().await?;

        // 7. get the contract's address
        let contract_address = contract.address();

        // 8. instantiate the contract
        let simple_token = SimpleTokenContract::new(contract_address, client0.clone());
        let contract_address = format!("{:?}", contract_address);

        Ok(Self {
            simple_token,
            contract_address,
            wallet_info,
            anvil_test,
        })
    }

    // Only the balanceOf operation is of interest for this contract
    pub async fn balance_of(&self, to: &str) -> Result<U256> {
        let to_address = to.parse::<Address>()?;
        let function_call = self.simple_token.balance_of(to_address).legacy();
        let TypedTransaction::Legacy(transact) = function_call.tx else {
            unreachable!();
        };
        let data = transact.data.unwrap();
        let answer = self
            .anvil_test
            .ethereum_endpoint
            .non_executive_call(&self.contract_address, data, to)
            .await?;
        let balance = U256::from_big_endian(&answer.0);
        Ok(balance)
    }
}

pub struct EventNumericsContractFunction {
    pub event_numerics: EventNumericsContract<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
    pub contract_address: String,
    pub anvil_test: AnvilTest,
}

impl EventNumericsContractFunction {
    pub async fn new(anvil_test: AnvilTest) -> Result<Self> {
        // 1. Getting the code
        let (abi, bytecode) = get_abi_bytecode("event_numerics.sol", "EventNumerics");

        // 2. Reading the client
        let wallet_info = anvil_test.get_wallet(0);
        let client0 = SignerMiddleware::new(
            anvil_test.ethereum_endpoint.provider.clone(),
            wallet_info.0.clone(),
        );
        let client0 = Arc::new(client0);

        // 3. Factory
        let factory = ContractFactory::new(abi, bytecode, client0.clone());

        // 6. deploy it with the constructor arguments, note the `legacy` call
        let initial_supply = U256::zero();
        let contract = factory.deploy(initial_supply)?.legacy().send().await?;

        // 7. get the contract's address
        let contract_address = contract.address();

        // 8. instantiate the contract
        let event_numerics = EventNumericsContract::new(contract_address, client0.clone());
        let contract_address = format!("{:?}", contract_address);

        Ok(Self {
            event_numerics,
            contract_address,
            anvil_test,
        })
    }
}
