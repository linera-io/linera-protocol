// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use alloy::{
    network::EthereumSigner, node_bindings::{Anvil, AnvilInstance}, primitives::U256, providers::ProviderBuilder,
    signers::wallet::LocalWallet, sol,
};
use alloy::signers::Signer;
use linera_storage_service::child::get_free_port;
//use alloy::providers::Provider;
use crate::client::HttpProvider;
use alloy_primitives::Bytes;
use alloy_primitives::Address;
//use crate::test_utils::SimpleTokenContract::SimpleTokenContractInstance;
//use reqwest::Client;
//use alloy::providers::fillers::FillProvider;
//use alloy::providers::fillers::JoinFill;
//use alloy::providers::fillers::GasFiller;
//use alloy::providers::fillers::NonceFiller;
//use alloy::providers::fillers::ChainIdFiller;
//use alloy::providers::fillers::SignerFiller;
//use alloy::providers::RootProvider;
use crate::client::EthereumEndpoint;

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    SimpleTokenContract,
    "./contracts/SimpleToken.json"
);


sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    EventNumericsContract,
    "./contracts/EventNumerics.json"
);

pub struct AnvilTest {
    pub anvil_instance: AnvilInstance,
    pub endpoint: String,
    pub ethereum_endpoint: EthereumEndpoint<HttpProvider>,
}

pub async fn get_anvil() -> Result<AnvilTest> {
    let port = get_free_port().await?;
    let anvil_instance = Anvil::new()
        .port(port)
        .try_spawn()?;
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
        let wallet = wallet.with_chain_id(Some(self.anvil_instance.chain_id()));
        (wallet, address)
    }

    pub fn get_address(&self, index: usize) -> String {
        let address = self.anvil_instance.addresses()[index];
        format!("{:?}", address)
    }
}

/*
pub fn get_abi_bytecode(contract_file: &str, contract_name: &str) -> (usize, Bytes) {
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
*/

pub struct SimpleTokenContractFunction {
    pub contract_address: String,
    pub anvil_test: AnvilTest,
}

impl SimpleTokenContractFunction {
    pub async fn new(anvil_test: AnvilTest) -> Result<Self> {
        // 2. Reading the client
        let wallet_info = anvil_test.get_wallet(0);
	let rpc_url = reqwest::Url::parse(&anvil_test.endpoint)?;
        let provider = ProviderBuilder::new()
            .with_recommended_fillers()
            .signer(EthereumSigner::from(wallet_info.0))
            .on_http(rpc_url);

        let initial_supply = U256::from(1000);
        let simple_token = SimpleTokenContract::deploy(&provider, initial_supply).await?;
        let contract_address = simple_token.address();
        let contract_address = format!("{:?}", contract_address);

        Ok(Self {
            contract_address,
            anvil_test,
        })
    }

    // Only the balanceOf operation is of interest for this contract
    pub async fn balance_of(&self, to: &str) -> Result<U256> {
        // 1: getting the provider
        let wallet_info = self.anvil_test.get_wallet(0);
	let rpc_url = reqwest::Url::parse(&self.anvil_test.endpoint)?;
        let provider = ProviderBuilder::new()
            .with_recommended_fillers()
            .signer(EthereumSigner::from(wallet_info.0))
            .on_http(rpc_url);
        // 2: getting the simple_token
        let contract_address = self.contract_address.parse::<Address>()?;
        let simple_token = SimpleTokenContract::new(contract_address, provider);

        // 3: gettting the balance transaction stuff
        let to_address = to.parse::<Address>()?;
        let data : Bytes = simple_token.balanceOf(to_address).calldata().clone();
        // 4: transmitting it
        let answer = self
            .anvil_test
            .ethereum_endpoint
            .non_executive_call(&self.contract_address, data, to)
            .await?;
        let mut vec = [0_u8; 32];
        for (i, val) in vec.iter_mut().enumerate() {
            *val = answer.0[i];
        }
        let balance = U256::from_be_bytes(vec);
        Ok(balance)
    }
}

/*
pub struct EventNumericsContractFunction {
    pub event_numerics: usize,
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
*/
