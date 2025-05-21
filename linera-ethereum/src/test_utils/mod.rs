// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_primitives::{Address, Bytes, U256};
use alloy::{
    network::{Ethereum, EthereumWallet},
    providers::{
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller},
        Provider, ProviderBuilder, RootProvider,
    },
    rpc::types::eth::{
        request::{TransactionInput, TransactionRequest},
        Filter,
    },
    signers::local::PrivateKeySigner,
    node_bindings::{Anvil, AnvilInstance},
    sol,
};
use linera_base::port::get_free_port;
use url::Url;
use async_trait::async_trait;

use crate::{
    client::get_block_id,
    common::{event_name_from_expanded, parse_log, EthereumEvent, EthereumServiceError},
};


use crate::{
    client::EthereumQueries,
    provider::EthereumClientSimplified,
};

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

#[derive(Clone)]
pub struct EthereumClient {
    pub provider: FillProvider<JoinFill<JoinFill<alloy::providers::Identity, JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>>, WalletFiller<EthereumWallet>>, RootProvider<Ethereum>>,
}

#[async_trait]
impl EthereumQueries for EthereumClient {
    type Error = EthereumServiceError;

    async fn get_accounts(&self) -> Result<Vec<String>, EthereumServiceError> {
        Ok(self
            .provider
            .get_accounts()
            .await?
            .into_iter()
            .map(|x| format!("{:?}", x))
            .collect::<Vec<_>>())
    }

    async fn get_block_number(&self) -> Result<u64, EthereumServiceError> {
        Ok(self.provider.get_block_number().await?)
    }

    async fn get_balance(
        &self,
        address: &str,
        block_number: u64,
    ) -> Result<U256, EthereumServiceError> {
        let address = address.parse::<Address>()?;
        let block_id = get_block_id(block_number);
        let request = self.provider.get_balance(address).block_id(block_id);
        Ok(request.await?)
    }

    async fn read_events(
        &self,
        contract_address: &str,
        event_name_expanded: &str,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<EthereumEvent>, EthereumServiceError> {
        let contract_address = contract_address.parse::<Address>()?;
        let event_name = event_name_from_expanded(event_name_expanded);
        let filter = Filter::new()
            .address(contract_address)
            .event(&event_name)
            .from_block(from_block)
            .to_block(to_block - 1);
        let events = self.provider.get_logs(&filter).await?;
        events
            .into_iter()
            .map(|x| parse_log(event_name_expanded, x))
            .collect::<Result<_, _>>()
    }

    async fn non_executive_call(
        &self,
        contract_address: &str,
        data: Bytes,
        from: &str,
        block: u64,
    ) -> Result<Bytes, EthereumServiceError> {
        let contract_address = contract_address.parse::<Address>()?;
        let from = from.parse::<Address>()?;
        let input = TransactionInput::new(data);
        let tx = TransactionRequest::default()
            .from(from)
            .to(contract_address)
            .input(input);
        let block_id = get_block_id(block);
        let eth_call = self.provider.call(tx).block(block_id);
        Ok(eth_call.await?)
    }
}

impl EthereumClient {
    /// Connects to an existing Ethereum node and creates an `EthereumClient`
    /// if successful.
    pub fn new(url: String) -> Result<Self, EthereumServiceError> {
        println!("EthereumClient, new, step 1");
        let rpc_url = Url::parse(&url)?;
        println!("EthereumClient, new, step 2");
        let pk: PrivateKeySigner =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".parse().unwrap();
        println!("EthereumClient, new, step 3");
        let wallet = EthereumWallet::from(pk);
        println!("EthereumClient, new, step 4");
        let provider = ProviderBuilder::new()
            .wallet(wallet.clone())
            .connect_http(rpc_url);
        println!("EthereumClient, new, step 5");
        let endpoint = Self { provider };
        println!("EthereumClient, new, step 6");
        Ok(endpoint)
    }
}

pub struct AnvilTest {
    pub anvil_instance: AnvilInstance,
    pub endpoint: String,
    pub ethereum_client: EthereumClient,
    pub rpc_url: Url,
}

pub async fn get_anvil() -> anyhow::Result<AnvilTest> {
    let port = get_free_port().await?;
    let anvil_instance = Anvil::new().port(port).try_spawn()?;
    let endpoint = anvil_instance.endpoint();
    let ethereum_client = EthereumClient::new(endpoint.clone())?;
    let rpc_url = Url::parse(&endpoint)?;
    Ok(AnvilTest {
        anvil_instance,
        endpoint,
        ethereum_client,
        rpc_url,
    })
}

impl AnvilTest {
    pub fn get_address(&self, index: usize) -> String {
        let address = self.anvil_instance.addresses()[index];
        format!("{:?}", address)
    }
}

pub struct SimpleTokenContractFunction {
    pub contract_address: String,
    pub anvil_test: AnvilTest,
}

impl SimpleTokenContractFunction {
    pub async fn new(anvil_test: AnvilTest) -> anyhow::Result<Self> {
        // 2: initializing the contract
        let initial_supply = U256::from(1000);
        println!("SimpleTokenContract, step 1");
        let simple_token =
            SimpleTokenContract::deploy(&anvil_test.ethereum_client.provider, initial_supply).await?;
        println!("SimpleTokenContract, step 2");
        let contract_address = simple_token.address();
        let contract_address = format!("{:?}", contract_address);
        Ok(Self {
            contract_address,
            anvil_test,
        })
    }

    // Only the balanceOf operation is of interest for this contract
    pub async fn balance_of(&self, to: &str, block: u64) -> anyhow::Result<U256> {
        // Getting the simple_token
        let contract_address = self.contract_address.parse::<Address>()?;
        let simple_token =
            SimpleTokenContract::new(contract_address, self.anvil_test.ethereum_client.provider.clone());
        // Creating the calldata
        let to_address = to.parse::<Address>()?;
        let data = simple_token.balanceOf(to_address).calldata().clone();
        // Using the Ethereum client simplified.
        let ethereum_client_simp = EthereumClientSimplified::new(self.anvil_test.endpoint.clone());
        let answer = ethereum_client_simp
            .non_executive_call(&self.contract_address, data, to, block)
            .await?;
        // Converting the output
        let mut vec = [0_u8; 32];
        for (i, val) in vec.iter_mut().enumerate() {
            *val = answer.0[i];
        }
        let balance = U256::from_be_bytes(vec);
        Ok(balance)
    }

    pub async fn transfer(&self, from: &str, to: &str, value: U256) -> anyhow::Result<()> {
        // Getting the simple_token
        let contract_address = self.contract_address.parse::<Address>()?;
        let to_address = to.parse::<Address>()?;
        let from_address = from.parse::<Address>()?;
        let simple_token =
            SimpleTokenContract::new(contract_address, self.anvil_test.ethereum_client.provider.clone());
        // Doing the transfer
        let builder = simple_token.transfer(to_address, value).from(from_address);
        let _receipt = builder.send().await?.get_receipt().await?;
        Ok(())
    }
}

pub struct EventNumericsContractFunction {
    pub contract_address: String,
    pub anvil_test: AnvilTest,
}

impl EventNumericsContractFunction {
    pub async fn new(anvil_test: AnvilTest) -> anyhow::Result<Self> {
        // Deploying the event numerics contract
        println!("EventNumericsContractFunction, new, step 1");
        let initial_supply = U256::from(0);
        println!("EventNumericsContractFunction, new, step 2");
        let event_numerics =
            EventNumericsContract::deploy(&anvil_test.ethereum_client.provider, initial_supply).await?;
        println!("EventNumericsContractFunction, new, step 3");
        // Getting the contract address
        let contract_address = event_numerics.address();
        println!("EventNumericsContractFunction, new, step 4");
        let contract_address = format!("{:?}", contract_address);
        println!("EventNumericsContractFunction, new, step 5");
        Ok(Self {
            contract_address,
            anvil_test,
        })
    }
}
