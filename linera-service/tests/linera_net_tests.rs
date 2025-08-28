// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "dynamodb",
    feature = "scylladb",
    feature = "storage-service",
    feature = "kubernetes",
    feature = "remote-net"
))]

mod guard;

use std::env;

use anyhow::Result;
use async_graphql::InputType;
use futures::{
    channel::mpsc,
    future::{self, Either},
    SinkExt, StreamExt,
};
use guard::INTEGRATION_TEST_GUARD;
use linera_base::{
    crypto::{CryptoHash, Secp256k1SecretKey},
    data_types::Amount,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId},
    time::{Duration, Instant},
    vm::VmRuntime,
};
use linera_core::worker::{Notification, Reason};
use linera_sdk::{
    abis::fungible::NativeFungibleTokenAbi,
    linera_base_types::{AccountSecretKey, BlobContent, BlockHeight, DataBlobHash},
};
#[cfg(any(
    feature = "dynamodb",
    feature = "scylladb",
    feature = "storage-service",
))]
use linera_service::cli_wrappers::local_net::{Database, LocalNetConfig};
#[cfg(feature = "remote-net")]
use linera_service::cli_wrappers::remote_net::RemoteNetTestingConfig;
#[cfg(any(
    feature = "dynamodb",
    feature = "scylladb",
    feature = "storage-service",
    feature = "kubernetes",
))]
use linera_service::cli_wrappers::Network;
#[cfg(feature = "kubernetes")]
use linera_service::cli_wrappers::{
    docker::BuildArg, local_kubernetes_net::SharedLocalKubernetesNetTestingConfig,
};
use linera_service::{
    cli_wrappers::{
        local_net::{get_node_port, ProcessInbox},
        ApplicationWrapper, ClientWrapper, LineraNet, LineraNetConfig,
    },
    test_name,
    util::eventually,
};
use serde_json::{json, Value};
use test_case::test_case;

/// The environment variable name to specify the number of iterations in the performance-related
/// tests.
const LINERA_TEST_ITERATIONS: &str = "LINERA_TEST_ITERATIONS";

fn test_iterations() -> Option<usize> {
    match env::var(LINERA_TEST_ITERATIONS) {
        Ok(var) => Some(var.parse().unwrap_or_else(|error| {
            panic!("{LINERA_TEST_ITERATIONS} is not a valid number: {error}")
        })),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            panic!("{LINERA_TEST_ITERATIONS} must be valid Unicode")
        }
    }
}

fn get_account_owner(client: &ClientWrapper) -> AccountOwner {
    client.get_owner().unwrap()
}

struct NativeFungibleApp(ApplicationWrapper<NativeFungibleTokenAbi>);

impl NativeFungibleApp {
    async fn get_amount(&self, account_owner: &AccountOwner) -> Amount {
        let query = format!(
            "accounts {{ entry(key: {}) {{ value }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await.unwrap();
        let amount_option = serde_json::from_value::<Option<Amount>>(
            response_body["accounts"]["entry"]["value"].clone(),
        )
        .unwrap();

        amount_option.unwrap_or(Amount::ZERO)
    }

    async fn assert_balances(&self, accounts: impl IntoIterator<Item = (AccountOwner, Amount)>) {
        for (account_owner, amount) in accounts {
            let value = self.get_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    async fn entries(&self) -> Vec<native_fungible::AccountEntry> {
        let query = "accounts { entries { key, value } }";
        let response_body = self.0.query(&query).await.unwrap();
        serde_json::from_value(response_body["accounts"]["entries"].clone()).unwrap()
    }

    async fn assert_entries(&self, accounts: impl IntoIterator<Item = (AccountOwner, Amount)>) {
        let entries: std::collections::BTreeMap<AccountOwner, Amount> = self
            .entries()
            .await
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();

        for (account_owner, amount) in accounts {
            assert_eq!(entries[&account_owner], amount);
        }
    }

    async fn keys(&self) -> Vec<AccountOwner> {
        let query = "accounts { keys }";
        let response_body = self.0.query(&query).await.unwrap();
        serde_json::from_value(response_body["accounts"]["keys"].clone()).unwrap()
    }

    async fn assert_keys(&self, accounts: impl IntoIterator<Item = AccountOwner>) {
        let keys = self.keys().await;
        for account_owner in accounts {
            assert!(keys.contains(&account_owner));
        }
    }

    async fn transfer(
        &self,
        account_owner: &AccountOwner,
        amount_transfer: Amount,
        destination: Account,
    ) -> Value {
        let mutation = format!(
            "transfer(owner: {}, amount: \"{}\", targetAccount: {})",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn claim(&self, source: Account, target: Account, amount: Amount) {
        // Claiming tokens from chain1 to chain2.
        let mutation = format!(
            "claim(sourceAccount: {}, amount: \"{}\", targetAccount: {})",
            source.to_value(),
            amount,
            target.to_value()
        );

        self.0.mutate(mutation).await.unwrap();
    }
}

struct FungibleApp(ApplicationWrapper<fungible::FungibleTokenAbi>);

impl FungibleApp {
    async fn get_amount(&self, account_owner: &AccountOwner) -> Amount {
        let query = format!(
            "accounts {{ entry(key: {}) {{ value }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await.unwrap();
        let amount_option = serde_json::from_value::<Option<Amount>>(
            response_body["accounts"]["entry"]["value"].clone(),
        )
        .unwrap();

        amount_option.unwrap_or(Amount::ZERO)
    }

    async fn assert_balances(&self, accounts: impl IntoIterator<Item = (AccountOwner, Amount)>) {
        for (account_owner, amount) in accounts {
            let value = self.get_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    async fn get_allowance(&self, owner: &AccountOwner, spender: &AccountOwner) -> Amount {
        let owner_spender = fungible::OwnerSpender::new(*owner, *spender);
        let query = format!(
            "allowances {{ entry(key: {}) {{ value }} }}",
            owner_spender.to_value()
        );
        let response_body = self.0.query(&query).await.unwrap();
        let amount_option = serde_json::from_value::<Option<Amount>>(
            response_body["allowances"]["entry"]["value"].clone(),
        )
        .unwrap();

        amount_option.unwrap_or(Amount::ZERO)
    }

    async fn assert_allowance(
        &self,
        owner: &AccountOwner,
        spender: &AccountOwner,
        allowance: Amount,
    ) {
        let value = self.get_allowance(owner, spender).await;
        assert_eq!(value, allowance);
    }

    async fn approve(
        &self,
        owner: &AccountOwner,
        spender: &AccountOwner,
        allowance: Amount,
    ) -> Value {
        let mutation = format!(
            "approve(owner: {}, spender: {}, allowance: \"{}\")",
            owner.to_value(),
            spender.to_value(),
            allowance,
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn transfer(
        &self,
        account_owner: &AccountOwner,
        amount_transfer: Amount,
        destination: Account,
    ) -> Value {
        let mutation = format!(
            "transfer(owner: {}, amount: \"{}\", targetAccount: {})",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn transfer_from(
        &self,
        owner: &AccountOwner,
        spender: &AccountOwner,
        amount_transfer: Amount,
        destination: Account,
    ) -> Value {
        let mutation = format!(
            "transferFrom(owner: {}, spender: {}, amount: \"{}\", targetAccount: {})",
            owner.to_value(),
            spender.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }
}

struct NonFungibleApp(ApplicationWrapper<non_fungible::NonFungibleTokenAbi>);

impl NonFungibleApp {
    pub fn create_token_id(
        chain_id: &ChainId,
        application_id: &ApplicationId,
        name: &String,
        minter: &AccountOwner,
        hash: &DataBlobHash,
        num_minted_nfts: u64,
    ) -> String {
        use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
        let token_id_vec = non_fungible::Nft::create_token_id(
            chain_id,
            application_id,
            name,
            minter,
            hash,
            num_minted_nfts,
        )
        .expect("Creating token ID should not fail");
        STANDARD_NO_PAD.encode(token_id_vec.id)
    }

    async fn get_nft(&self, token_id: &String) -> Result<non_fungible::NftOutput> {
        let query = format!(
            "nft(tokenId: {}) {{ tokenId, owner, name, minter, payload }}",
            token_id.to_value()
        );
        let response_body = self.0.query(&query).await?;
        Ok(serde_json::from_value(response_body["nft"].clone())?)
    }

    async fn get_owned_nfts(&self, owner: &AccountOwner) -> Result<Vec<String>> {
        let query = format!("ownedTokenIdsByOwner(owner: {})", owner.to_value());
        let response_body = self.0.query(&query).await?;
        Ok(serde_json::from_value(
            response_body["ownedTokenIdsByOwner"].clone(),
        )?)
    }

    async fn mint(&self, minter: &AccountOwner, name: &String, blob_hash: &DataBlobHash) -> Value {
        let mutation = format!(
            "mint(minter: {}, name: {}, blobHash: {})",
            minter.to_value(),
            name.to_value(),
            blob_hash.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn transfer(
        &self,
        source_owner: &AccountOwner,
        token_id: &String,
        target_account: &Account,
    ) -> Value {
        let mutation = format!(
            "transfer(sourceOwner: {}, tokenId: {}, targetAccount: {})",
            source_owner.to_value(),
            token_id.to_value(),
            target_account.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn claim(
        &self,
        source_account: &Account,
        token_id: &String,
        target_account: &Account,
    ) -> Value {
        // Claiming tokens from chain1 to chain2.
        let mutation = format!(
            "claim(sourceAccount: {}, tokenId: {}, targetAccount: {})",
            source_account.to_value(),
            token_id.to_value(),
            target_account.to_value()
        );

        self.0.mutate(mutation).await.unwrap()
    }
}

struct MatchingEngineApp(ApplicationWrapper<matching_engine::MatchingEngineAbi>);

impl MatchingEngineApp {
    async fn get_account_info(
        &self,
        account_owner: &AccountOwner,
    ) -> Vec<matching_engine::OrderId> {
        let query = format!(
            "accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(query).await.unwrap();
        serde_json::from_value(response_body["accountInfo"]["entry"]["value"]["orders"].clone())
            .unwrap()
    }

    async fn order(&self, order: matching_engine::Order) -> Value {
        let mutation = format!("executeOrder(order: {})", order.to_value());
        self.0.mutate(mutation).await.unwrap()
    }
}

struct AmmApp(ApplicationWrapper<amm::AmmAbi>);

impl AmmApp {
    async fn swap(
        &self,
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    ) -> Result<Value> {
        let mutation = format!(
            "swap(owner: {}, inputTokenIdx: {}, inputAmount: \"{}\")",
            owner.to_value(),
            input_token_idx,
            input_amount
        );
        self.0.mutate(mutation).await
    }

    async fn add_liquidity(
        &self,
        owner: AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    ) -> Result<Value> {
        let mutation = format!(
            "addLiquidity(owner: {}, maxToken0Amount: \"{}\", maxToken1Amount: \"{}\")",
            owner.to_value(),
            max_token0_amount,
            max_token1_amount
        );
        self.0.mutate(mutation).await
    }

    async fn remove_liquidity(
        &self,
        owner: AccountOwner,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
    ) -> Result<Value> {
        let mutation = format!(
            "removeLiquidity(owner: {}, tokenToRemoveIdx: {}, tokenToRemoveAmount: \"{}\")",
            owner.to_value(),
            token_to_remove_idx,
            token_to_remove_amount
        );
        self.0.mutate(mutation).await
    }

    async fn remove_all_added_liquidity(&self, owner: AccountOwner) -> Result<Value> {
        let mutation = format!("removeAllAddedLiquidity(owner: {})", owner.to_value(),);
        self.0.mutate(mutation).await
    }
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use alloy_sol_types::{sol, SolCall, SolValue};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u64_entry};
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    sol! {
        struct ConstructorArgs {
            uint64 initial_value;
        }
        function increment(uint64 input);
        function get_value();
    }

    let original_counter_value = 35;
    let constructor_argument = ConstructorArgs {
        initial_value: original_counter_value,
    };
    let constructor_argument = constructor_argument.abi_encode();

    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();

    let (evm_contract, _dir) = get_evm_contract_path("tests/fixtures/evm_example_counter.sol")?;

    let instantiation_argument = Vec::new();
    let application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let query = get_valueCall {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);
    let result = application.run_json_query(query.clone()).await?;

    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_counter_value);

    let mutation = incrementCall { input: increment };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application.run_json_query(mutation).await?;

    let result = application.run_json_query(query).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_event(config: impl LineraNetConfig) -> Result<()> {
    use alloy_primitives::{Bytes, Log, U256};
    use alloy_sol_types::{sol, SolCall, SolValue};
    use linera_base::{
        identifiers::{GenericApplicationId, StreamId, StreamName},
        vm::EvmQuery,
    };
    use linera_execution::test_utils::solidity::get_evm_contract_path;
    use linera_sdk::abis::evm::EvmAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    sol! {
        struct ConstructorArgs {
            uint64 start_value;
        }
        function increment(uint64 input);
    }

    let start_value = 35;
    let constructor_argument = ConstructorArgs { start_value };
    let constructor_argument = constructor_argument.abi_encode();

    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();

    let (evm_contract, _dir) = get_evm_contract_path("tests/fixtures/evm_example_log.sol")?;

    let instantiation_argument = Vec::new();
    let application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let application_id = GenericApplicationId::User(application_id.forget_abi());
    let stream_name = bcs::to_bytes("ethereum_event")?;
    let stream_name = StreamName(stream_name);

    let stream_id = StreamId {
        application_id,
        stream_name,
    };

    let mut start_index = 0;
    let indices_and_events = node_service
        .events_from_index(&chain, &stream_id, start_index)
        .await?;
    let index_and_event = indices_and_events[0].clone();
    assert_eq!(index_and_event.index, 0);
    let (origin, block_height, log) =
        bcs::from_bytes::<(String, u64, Log)>(&index_and_event.event)?;
    assert_eq!(&origin, "deploy");
    assert_eq!(block_height, 1);
    let value = U256::from(start_value);
    let bytes = Bytes::from(value.to_be_bytes::<32>().to_vec());
    assert_eq!(log.data.data, bytes);
    start_index += indices_and_events.len() as u32;
    assert_eq!(start_index, 1);

    let mutation = incrementCall { input: increment };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application.run_json_query(mutation).await?;

    let indices_and_events = node_service
        .events_from_index(&chain, &stream_id, start_index)
        .await?;
    let index_and_event = indices_and_events[0].clone();
    assert_eq!(index_and_event.index, 1);
    let (origin, block_height, log) =
        bcs::from_bytes::<(String, u64, Log)>(&index_and_event.event)?;
    assert_eq!(&origin, "operation");
    assert_eq!(block_height, 2);
    let value1 = U256::from(increment);
    let value2 = U256::from(start_value + increment);
    let mut bytes = Vec::new();
    bytes.extend(value1.to_be_bytes::<32>());
    bytes.extend(value2.to_be_bytes::<32>());
    let bytes = Bytes::from(bytes);
    assert_eq!(log.data.data, bytes);
    start_index += indices_and_events.len() as u32;
    assert_eq!(start_index, 2);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_call_evm_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use alloy_sol_types::{sol, SolValue};
    use call_evm_counter::{CallCounterAbi, CallCounterRequest};
    use linera_execution::test_utils::solidity::get_evm_contract_path;
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    // Creating the EVM contract

    sol! {
        struct ConstructorArgs {
            uint64 initial_value;
        }
    }

    let original_counter_value = 35;
    let constructor_argument = ConstructorArgs {
        initial_value: original_counter_value,
    };
    let constructor_argument = constructor_argument.abi_encode();

    let (evm_contract, _dir) = get_evm_contract_path("tests/fixtures/evm_example_counter.sol")?;

    let instantiation_argument = Vec::new();
    let evm_application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    // Creating the WASM contract

    let (wasm_contract, wasm_service) = client.build_example("call-evm-counter").await?;
    type WasmParameter = ApplicationId<EvmAbi>;
    let wasm_application_id = client
        .publish_and_create::<CallCounterAbi, WasmParameter, ()>(
            wasm_contract,
            wasm_service,
            VmRuntime::Wasm,
            &evm_application_id,
            &(),
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let wasm_application = node_service
        .make_application(&chain, &wasm_application_id)
        .await?;

    // Testing the queries

    let query = CallCounterRequest::Query;
    let counter_value = wasm_application.run_json_query(&query).await?;
    assert_eq!(counter_value, original_counter_value);

    let increment = 5;
    let query_increment = CallCounterRequest::Increment(increment);
    wasm_application.run_json_query(&query_increment).await?;

    let counter_value = wasm_application.run_json_query(&query).await?;
    assert_eq!(counter_value, original_counter_value + increment);

    // Testing the address

    let query = CallCounterRequest::TestCallAddress;
    let counter_value = wasm_application.run_json_query(&query).await?;
    assert_eq!(counter_value, 34);

    let query = CallCounterRequest::ContractTestCallAddress;
    wasm_application.run_json_query(query).await?;

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_call_evm_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use alloy_sol_types::{sol, SolCall, SolValue};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u64_entry};
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();
    let original_counter_value = 35;
    let increment = 5;

    // Creating the EVM contract

    let (evm_contract, _dir) = get_evm_contract_path("tests/fixtures/evm_example_counter.sol")?;

    let constructor_argument = {
        sol! {
            struct ConstructorArgs {
                uint64 initial_value;
            }
        }

        let constructor_argument = ConstructorArgs {
            initial_value: original_counter_value,
        };
        constructor_argument.abi_encode()
    };

    let instantiation_argument = Vec::new();
    let evm_application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    // Creating the nesting contract

    sol! {
        struct ConstructorArgs {
            address evm_contract;
        }
        function nest_increment(uint64 input);
        function nest_get_value();
    }

    let evm_contract = evm_application_id.evm_address();
    let nest_constructor_argument = ConstructorArgs { evm_contract };
    let nest_constructor_argument = nest_constructor_argument.abi_encode();

    let (nest_contract, _dir) =
        get_evm_contract_path("tests/fixtures/evm_call_evm_example_counter.sol")?;

    let nest_application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            nest_contract.clone(),
            nest_contract,
            VmRuntime::Evm,
            &nest_constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let nest_application = node_service
        .make_application(&chain, &nest_application_id)
        .await?;

    let query = nest_get_valueCall {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);
    let result = nest_application.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_counter_value);

    let mutation = nest_incrementCall { input: increment };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    nest_application.run_json_query(mutation).await?;

    let result = nest_application.run_json_query(query).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_call_wasm_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use alloy_sol_types::{sol, SolCall, SolValue};
    use counter_no_graphql::CounterNoGraphQlAbi;
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u64_entry};
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    // Creating the WASM smart contract
    let original_counter_value = 35;
    let increment = 5;
    let (contract, service) = client.build_example("counter-no-graphql").await?;
    let wasm_application_id = client
        .publish_and_create::<CounterNoGraphQlAbi, (), u64>(
            contract,
            service,
            VmRuntime::Wasm,
            &(),
            &original_counter_value,
            &[],
            None,
        )
        .await?;

    // Creating the EVM smart contract

    sol! {
        struct ConstructorArgs {
            bytes32 wasm_contract;
        }
        function nest_increment(uint64 input);
        function nest_get_value();
    }
    let query = nest_get_valueCall {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);

    let wasm_contract = wasm_application_id.bytes32();
    let nest_constructor_argument = ConstructorArgs { wasm_contract };
    let nest_constructor_argument = nest_constructor_argument.abi_encode();

    let (nest_contract, _dir) =
        get_evm_contract_path("tests/fixtures/evm_call_wasm_example_counter.sol")?;

    let instantiation_argument = Vec::new();
    let nest_application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            nest_contract.clone(),
            nest_contract,
            VmRuntime::Evm,
            &nest_constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let nest_application = node_service
        .make_application(&chain, &nest_application_id)
        .await?;

    let result = nest_application.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_counter_value);

    let mutation = nest_incrementCall { input: increment };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    nest_application.run_json_query(mutation).await?;

    let result = nest_application.run_json_query(query).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_execute_message_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use alloy_primitives::B256;
    use alloy_sol_types::{sol, SolCall, SolValue};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u64_entry};
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    let original_value = 35;
    let moved_value = 5;

    // Creating the API of the contracts

    sol! {
        struct ConstructorArgs {
            uint64 test_value;
        }
        function move_value_to_chain(bytes32 chain_id, uint64 moved_value);
        function get_value();
    }
    let query = get_valueCall {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);

    let constructor_argument = ConstructorArgs { test_value: 42 };
    let constructor_argument = constructor_argument.abi_encode();

    let instantiation_argument = u64::abi_encode(&original_value);

    let (evm_contract, _dir) =
        get_evm_contract_path("tests/fixtures/evm_example_execute_message.sol")?;

    let application_id = client1
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    // Creating the applications.

    let application1 = node_service1
        .make_application(&chain1, &application_id)
        .await?;

    let application2 = node_service2
        .make_application(&chain2, &application_id)
        .await?;

    // Now checking the APIs.
    // First: checking the initial value of the contracts.

    let result = application1.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_value);

    let result = application2.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, 0);

    // Second: executing the movement of assets

    let chain_id: [u64; 4] = <[u64; 4]>::from(chain2.0);
    let chain_id: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(chain_id);
    let chain_id: B256 = chain_id.into();
    let mutation = move_value_to_chainCall {
        chain_id,
        moved_value,
    };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application1.run_json_query(mutation).await?;

    node_service2.process_inbox(&chain2).await?;

    // Third: Checking the values after the move

    let result = application1.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, original_value - moved_value);

    let result = application2.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, moved_value);

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_empty_instantiate(config: impl LineraNetConfig) -> Result<()> {
    use alloy_sol_types::{sol, SolCall};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u64_entry};
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    sol! {
        function get_value();
    }
    let query = get_valueCall {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);

    let constructor_argument = Vec::new();
    let instantiation_argument = Vec::new();

    let (evm_contract, _dir) =
        get_evm_contract_path("tests/fixtures/evm_example_empty_instantiate.sol")?;

    let application_id = client1
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;
    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    // Creating the applications.

    let application1 = node_service1
        .make_application(&chain1, &application_id)
        .await?;

    let application2 = node_service2
        .make_application(&chain2, &application_id)
        .await?;

    // Checking the initial value of the contracts.
    let result = application1.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, 42);
    let result = application2.run_json_query(query).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, 37);

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_process_streams_end_to_end_counters(config: impl LineraNetConfig) -> Result<()> {
    use alloy_primitives::B256;
    use alloy_sol_types::{sol, SolCall};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u64_entry};
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;
    let chain_id1: [u64; 4] = <[u64; 4]>::from(chain1.0);
    let chain_id1: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(chain_id1);
    let chain_id1: B256 = chain_id1.into();

    let increment = 5;

    // Creating the API of the contracts

    sol! {
        function increment_value(uint64 increment);
        function subscribe(bytes32 chain_id, bytes32 application_id);
        function get_value(bytes32 chain_id);
    }
    let query = get_valueCall {
        chain_id: chain_id1,
    };
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);

    let constructor_argument = Vec::new();
    let instantiation_argument = Vec::new();

    let (evm_contract, _dir) =
        get_evm_contract_path("tests/fixtures/evm_example_process_streams.sol")?;

    let evm_application_id = client1
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;
    let application_id: [u64; 4] =
        <[u64; 4]>::from(evm_application_id.application_description_hash);
    let application_id: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(application_id);
    let application_id: B256 = application_id.into();

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    // Creating the applications.

    let application1 = node_service1
        .make_application(&chain1, &evm_application_id)
        .await?;

    let application2 = node_service2
        .make_application(&chain2, &evm_application_id)
        .await?;

    let result = application2.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, 0);

    // First: subscribing to the application

    let mutation = subscribeCall {
        chain_id: chain_id1,
        application_id,
    };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application2.run_json_query(mutation).await?;

    let result = application2.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, 0);

    // Second: increment the values

    let mutation = increment_valueCall { increment };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application1.run_json_query(mutation).await?;

    // Third: process the inbox on chain2

    node_service2.process_inbox(&chain2).await?;

    // Fourth: getting the value

    let result = application2.run_json_query(query.clone()).await?;
    let counter_value = read_evm_u64_entry(result);
    assert_eq!(counter_value, increment);

    // Fifth: winding down

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_msg_sender(config: impl LineraNetConfig) -> Result<()> {
    use alloy_primitives::Address;
    use alloy_sol_types::{sol, SolCall};
    use linera_base::{identifiers::AccountOwner, vm::EvmQuery};
    use linera_execution::test_utils::solidity::get_evm_contract_path;
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;
    let account_owner = client.get_owner();
    let Some(AccountOwner::Address20(address)) = account_owner else {
        panic!("The owner should be of the form Some(Address20(...))");
    };
    let owner = Address::from(address);
    let chain = client.load_wallet()?.default_chain().unwrap();

    sol! {
        function check_msg_sender(address remote_address);
        function remote_check(address remote_address);
    }

    let instantiation_argument = Vec::new();
    let constructor_argument = Vec::new();

    // Creating the inner EVM contract

    let (inner_contract, _dir) = get_evm_contract_path("tests/fixtures/evm_msg_sender_inner.sol")?;
    let application_id_inner = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            inner_contract.clone(),
            inner_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;
    let evm_contract_inner = application_id_inner.evm_address();

    // Creating the outer EVM contract

    let (outer_contract, _dir) = get_evm_contract_path("tests/fixtures/evm_msg_sender_outer.sol")?;
    let application_id_outer = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            outer_contract.clone(),
            outer_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    // Making the check

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application_inner = node_service
        .make_application(&chain, &application_id_inner)
        .await?;
    let application_outer = node_service
        .make_application(&chain, &application_id_outer)
        .await?;

    let mutation = check_msg_senderCall {
        remote_address: owner,
    };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application_inner.run_json_query(mutation).await?;

    let mutation = remote_checkCall {
        remote_address: evm_contract_inner,
    };
    let mutation = mutation.abi_encode();
    let mutation = EvmQuery::Mutation(mutation);
    application_outer.run_json_query(mutation).await?;

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_linera_features(config: impl LineraNetConfig) -> Result<()> {
    use alloy_primitives::{B256, U256};
    use alloy_sol_types::{sol, SolCall};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::get_evm_contract_path;
    use linera_sdk::abis::evm::EvmAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();
    let account_chain = Account::chain(chain);

    // Creating the EVM smart contract

    sol! {
        function test_chain_id();
        function test_read_data_blob(bytes32 hash, uint32 len);
        function test_assert_data_blob_exists(bytes32 hash);
        function test_chain_ownership();
        function test_authenticated_signer_caller_id();
        function test_chain_balance(uint256 expected_balance);
        function test_read_owners();
    }

    let (contract, _dir) = get_evm_contract_path("tests/fixtures/evm_test_linera_features.sol")?;

    let constructor_argument = Vec::new();
    let instantiation_argument = Vec::new();
    let application_id = client
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            contract.clone(),
            contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let nft_blob_bytes = b"nft1_data".to_vec();
    let len = nft_blob_bytes.len() as u32;
    let hash = node_service
        .publish_data_blob(&chain, nft_blob_bytes)
        .await?;
    let hash: B256 = <[u8; 32]>::from(hash).into();

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    // Testing the ChainId.

    let query = test_chain_idCall {};
    let query = EvmQuery::Query(query.abi_encode());
    application.run_json_query(query).await?;

    // Testing Chain Ownership

    let query = test_chain_ownershipCall {};
    let query = EvmQuery::Query(query.abi_encode());
    application.run_json_query(query).await?;

    // Testing existence of blob.

    let query = test_assert_data_blob_existsCall { hash };
    let query = EvmQuery::Query(query.abi_encode());
    application.run_json_query(query).await?;

    // Reading the blob

    let query = test_read_data_blobCall { hash, len };
    let query = EvmQuery::Query(query.abi_encode());
    application.run_json_query(query).await?;

    // Checking authenticated signer/caller_id

    let mutation = test_authenticated_signer_caller_idCall {};
    let mutation = EvmQuery::Mutation(mutation.abi_encode());
    application.run_json_query(mutation).await?;

    // Testing the chain balance

    let expected_balance = node_service.balance(&account_chain).await?;
    let expected_balance: U256 = expected_balance.into();
    let query = test_chain_balanceCall { expected_balance };
    let query = EvmQuery::Query(query.abi_encode());
    application.run_json_query(query).await?;

    // Testing the owner balances

    let query = test_read_ownersCall {};
    let query = EvmQuery::Query(query.abi_encode());
    application.run_json_query(query).await?;

    // Winding down

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(all(feature = "rocksdb", feature = "scylladb"), test_case(LocalNetConfig::new_test(Database::DualRocksDbScyllaDb, Network::Grpc) ; "dualrocksdbscylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let account_chain = Account::chain(chain);
    let (contract, service) = client.build_example("counter").await?;

    let application_id = client
        .publish_and_create::<CounterAbi, (), u64>(
            contract,
            service,
            VmRuntime::Wasm,
            &(),
            &original_counter_value,
            &[],
            None,
        )
        .await?;
    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let balance1 = node_service.balance(&account_chain).await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value);
    let balance2 = node_service.balance(&account_chain).await?;
    assert_eq!(balance1, balance2);

    let mutation = format!("increment(field0: {increment})");
    application.mutate(mutation).await?;
    let balance3 = node_service.balance(&account_chain).await?;
    assert!(balance3 < balance2);

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(with_revm)]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_evm_erc20_shared(config: impl LineraNetConfig) -> Result<()> {
    use alloy_primitives::{B256, U256};
    use alloy_sol_types::{sol, SolCall, SolValue};
    use linera_base::vm::EvmQuery;
    use linera_execution::test_utils::solidity::{get_evm_contract_path, read_evm_u256_entry};
    use linera_sdk::abis::evm::EvmAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;
    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;
    let owner1 = client1.get_owner().unwrap();
    let owner2 = client1.keygen().await?;
    let address1 = owner1.to_evm_address().unwrap();
    let address2 = owner2.to_evm_address().unwrap();

    sol! {
        struct ConstructorArgs {
            uint256 the_supply;
        }
        function totalSupply();
        function transfer(address to, uint256 value);
        function balanceOf(address account);
        function transferToChain(bytes32 chain_id, address destination, uint256 value);
    }

    let the_supply = U256::from(1000000000);
    let transfer1 = U256::from(1);
    let transfer2 = U256::from(6);
    let constructor_argument = ConstructorArgs { the_supply };
    let constructor_argument = constructor_argument.abi_encode();

    let instantiation_argument = U256::abi_encode(&the_supply);

    let (evm_contract, _dir) = get_evm_contract_path("tests/fixtures/erc20_shared.sol")?;

    let application_id = client1
        .publish_and_create::<EvmAbi, Vec<u8>, Vec<u8>>(
            evm_contract.clone(),
            evm_contract,
            VmRuntime::Evm,
            &constructor_argument,
            &instantiation_argument,
            &[],
            None,
        )
        .await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    let application1 = node_service1
        .make_application(&chain1, &application_id)
        .await?;

    let application2 = node_service2
        .make_application(&chain2, &application_id)
        .await?;

    // Checking the total supply

    let total_supply = totalSupplyCall {};
    let query = total_supply.abi_encode();
    let query = EvmQuery::Query(query);
    let result = application1.run_json_query(query).await?;
    assert_eq!(read_evm_u256_entry(result), the_supply);

    // Transferring to another user and checking the balances.

    let mutation = transferCall {
        to: address2,
        value: transfer1,
    };
    let mutation = EvmQuery::Mutation(mutation.abi_encode());
    application1.run_json_query(mutation).await?;

    let query = balanceOfCall { account: address1 };
    let query = EvmQuery::Query(query.abi_encode());
    let result = application1.run_json_query(query).await?;
    assert_eq!(read_evm_u256_entry(result), the_supply - transfer1);

    let query = balanceOfCall { account: address2 };
    let query = EvmQuery::Query(query.abi_encode());
    let result = application1.run_json_query(query).await?;
    assert_eq!(read_evm_u256_entry(result), transfer1);

    // Transferring to another chain and checking the balances.

    let chain_id: [u64; 4] = <[u64; 4]>::from(chain2.0);
    let chain_id: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(chain_id);
    let chain_id: B256 = chain_id.into();
    let mutation = transferToChainCall {
        chain_id,
        destination: address2,
        value: transfer2,
    };
    let mutation = EvmQuery::Mutation(mutation.abi_encode());
    application1.run_json_query(mutation).await?;

    node_service2.process_inbox(&chain2).await?;

    // Checking the balances on both chains.

    let query = balanceOfCall { account: address1 };
    let query = EvmQuery::Query(query.abi_encode());
    let result = application1.run_json_query(query.clone()).await?;
    assert_eq!(
        read_evm_u256_entry(result),
        the_supply - transfer1 - transfer2
    );

    let query = balanceOfCall { account: address2 };
    let query = EvmQuery::Query(query.abi_encode());
    let result = application2.run_json_query(query).await?;
    assert_eq!(read_evm_u256_entry(result), transfer2);

    // Winding down

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;
    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter_no_graphql(config: impl LineraNetConfig) -> Result<()> {
    use counter_no_graphql::{CounterNoGraphQlAbi, CounterRequest};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let (contract, service) = client.build_example("counter-no-graphql").await?;

    let application_id = client
        .publish_and_create::<CounterNoGraphQlAbi, (), u64>(
            contract,
            service,
            VmRuntime::Wasm,
            &(),
            &original_counter_value,
            &[],
            None,
        )
        .await?;
    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let query = CounterRequest::Query;
    let read_counter_value = application.run_json_query(&query).await?;
    let mut counter_value = original_counter_value;
    assert_eq!(read_counter_value, counter_value);

    // executing a query that mutates the state

    let query_increment = CounterRequest::Increment(increment);
    application.run_json_query(&query_increment).await?;

    let read_counter_value = application.run_json_query(&query).await?;
    counter_value += increment;
    assert_eq!(read_counter_value, counter_value);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter_publish_create(config: impl LineraNetConfig) -> Result<()> {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let (contract, service) = client.build_example("counter").await?;

    let module_id = client
        .publish_module::<CounterAbi, (), u64>(contract, service, VmRuntime::Wasm, None)
        .await?;
    let application_id = client
        .create_application(&module_id, &(), &original_counter_value, &[], None)
        .await?;
    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;
    let query = format!("query {{ applications(chainId:\"{chain}\") {{ id }} }}");
    let response = node_service.query_node(query).await?;
    assert_eq!(
        response["applications"][0]["id"].as_str().unwrap(),
        &application_id.forget_abi().to_string()
    );

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value);

    let mutation = format!("increment(field0: {increment})");
    application.mutate(mutation).await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_social_event_streams(config: impl LineraNetConfig) -> Result<()> {
    use linera_base::time::Instant;
    use social::SocialAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;
    client2.sync(chain2).await?;
    let (contract, service) = client1.build_example("social").await?;
    let module_id = client1
        .publish_module::<SocialAbi, (), ()>(contract, service, VmRuntime::Wasm, None)
        .await?;
    let application_id = client1
        .create_application(&module_id, &(), &(), &[], None)
        .await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1
        .run_node_service(port1, ProcessInbox::Automatic)
        .await?;
    let mut node_service2 = client2
        .run_node_service(port2, ProcessInbox::Automatic)
        .await?;

    let app2 = node_service2
        .make_application(&chain2, &application_id)
        .await?;
    app2.mutate(format!("subscribe(chainId: \"{chain1}\")"))
        .await?;

    let mut notifications = Box::pin(node_service2.notifications(chain2).await?);

    let app1 = node_service1
        .make_application(&chain1, &application_id)
        .await?;
    app1.mutate("post(text: \"Linera Social is the new Mastodon!\")")
        .await?;

    let query = "receivedPosts { keys { author, index } }";
    let expected_response = json!({
        "receivedPosts": {
            "keys": [
                { "author": chain1, "index": 0 }
            ]
        }
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let result =
            linera_base::time::timer::timeout(deadline - Instant::now(), notifications.next())
                .await?;
        anyhow::ensure!(result.transpose()?.is_some(), "Failed to confirm post");
        let response = app2.query(query).await?;
        if response == expected_response {
            tracing::info!("Confirmed post");
            break;
        }
        tracing::warn!("Waiting to confirm post: {}", response);
    }

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_allowances_fungible(config: impl LineraNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    use fungible::{FungibleTokenAbi, InitialState, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and three clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let client3 = net.make_client().await;
    client3.wallet_init(None).await?;

    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate keys for all clients.
    let owner1 = client1.keygen().await?;
    let owner2 = client2.keygen().await?;
    let owner3 = client3.keygen().await?;

    // Open a chain owned by both clients.
    let chain2 = client1
        .open_multi_owner_chain(
            chain1,
            vec![owner1, owner2, owner3],
            vec![100, 100, 100],
            u32::MAX,
            Amount::from_tokens(6),
            10_000,
        )
        .await?;

    // Assign chain2 to clients.
    client1.assign(owner1, chain2).await?;
    client2.assign(owner2, chain2).await?;
    client3.assign(owner3, chain2).await?;

    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (owner1, Amount::from_tokens(9)),
        (owner2, Amount::from_tokens(19)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let (contract, service) = client1.build_example("fungible").await?;
    let params = Parameters::new("DEL");
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
            contract,
            service,
            VmRuntime::Wasm,
            &params,
            &state,
            &[],
            Some(chain2),
        )
        .await?;

    // Synchronize the chain in clients 2 and 3, so they see the initialized application state.
    client2.sync(chain2).await?;
    client3.sync(chain2).await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let port3 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;
    let mut node_service3 = client3.run_node_service(port3, ProcessInbox::Skip).await?;

    let app1 = FungibleApp(
        node_service1
            .make_application(&chain2, &application_id)
            .await?,
    );
    let app2 = FungibleApp(
        node_service2
            .make_application(&chain2, &application_id)
            .await?,
    );
    let app3 = FungibleApp(
        node_service3
            .make_application(&chain2, &application_id)
            .await?,
    );

    let expected_balances = [
        (owner1, Amount::from_tokens(9)),
        (owner2, Amount::from_tokens(19)),
    ];
    app1.assert_balances(expected_balances).await;
    app2.assert_balances(expected_balances).await;
    app3.assert_balances(expected_balances).await;

    // Approving a transfer
    app1.approve(&owner1, &owner2, Amount::from_tokens(93))
        .await;

    app1.assert_allowance(&owner1, &owner2, Amount::from_tokens(93))
        .await;

    // Call process inbox in order to synchronize from validators
    node_service2.process_inbox(&chain2).await?;
    app2.assert_allowance(&owner1, &owner2, Amount::from_tokens(93))
        .await;

    // Doing the transfer from
    app2.transfer_from(
        &owner1,
        &owner2,
        Amount::from_tokens(2),
        Account {
            chain_id: chain2,
            owner: owner3,
        },
    )
    .await;

    // Checking the final values on chain1 and chain2.

    let expected_balances = [
        (owner1, Amount::from_tokens(7)),
        (owner2, Amount::from_tokens(19)),
        (owner3, Amount::from_tokens(2)),
    ];
    app2.assert_balances(expected_balances).await;
    app2.assert_allowance(&owner1, &owner2, Amount::from_tokens(91))
        .await;

    // Winding down the system

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;
    node_service3.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

async fn publish_and_create_native_fungible(
    client: &ClientWrapper,
    name: &str,
    params: &fungible::Parameters,
    state: &fungible::InitialState,
    chain_id: Option<ChainId>,
) -> Result<ApplicationId<NativeFungibleTokenAbi>> {
    let (contract, service) = client.build_example(name).await?;
    use fungible::{FungibleTokenAbi, InitialState, Parameters};
    if name == "native-fungible" {
        client
            .publish_and_create::<NativeFungibleTokenAbi, Parameters, InitialState>(
                contract,
                service,
                VmRuntime::Wasm,
                params,
                state,
                &[],
                chain_id,
            )
            .await
    } else {
        let application_id = client
            .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
                contract,
                service,
                VmRuntime::Wasm,
                params,
                state,
                &[],
                chain_id,
            )
            .await?;
        Ok(application_id.forget_abi().with_abi())
    }
}

// TODO(#2051): Enable the test `test_wasm_end_to_end_fungible::scylladb_grpc` that is frequently failing.
// The failure is `Error: Could not find application URI: .... after 15 tries`.
//#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "fungible" ; "scylladb_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "fungible" ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "native-fungible" ; "native_storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "native-fungible" ; "native_scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "fungible" ; "aws_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "native-fungible" ; "native_aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "fungible" ; "kubernetes_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "native-fungible" ; "native_kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None), "fungible" ; "remote_net_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None), "native-fungible" ; "native_remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_fungible(
    config: impl LineraNetConfig,
    example_name: &str,
) -> Result<()> {
    use std::collections::BTreeMap;

    use fungible::{InitialState, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    // The players
    let account_owner1 = get_account_owner(&client1);
    let account_owner2 = get_account_owner(&client2);
    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let params = if example_name == "native-fungible" {
        // Native Fungible has a fixed NAT ticker symbol, anything else will be rejected
        Parameters::new("NAT")
    } else {
        Parameters::new("FUN")
    };
    let application_id =
        publish_and_create_native_fungible(&client1, example_name, &params, &state, None).await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    let app1 = NativeFungibleApp(
        node_service1
            .make_application(&chain1, &application_id)
            .await?,
    );

    let expected_balances = [
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ];
    app1.assert_balances(expected_balances).await;
    app1.assert_entries(expected_balances).await;
    app1.assert_keys([account_owner1, account_owner2]).await;

    // Transferring
    app1.transfer(
        &account_owner1,
        Amount::ONE,
        Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // Checking the final values on chain1 and chain2.
    let expected_balances = [
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::from_tokens(2)),
    ];
    app1.assert_balances(expected_balances).await;
    app1.assert_entries(expected_balances).await;
    app1.assert_keys([account_owner1, account_owner2]).await;

    assert_eq!(node_service2.process_inbox(&chain2).await?.len(), 1);

    // Fungible didn't exist on chain2 initially but now it does and we can talk to it.
    let app2 = NativeFungibleApp(
        node_service2
            .make_application(&chain2, &application_id)
            .await?,
    );

    let expected_balances = [
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::ONE),
    ];
    let expected_entries = [(account_owner2, Amount::ONE)];
    app2.assert_balances(expected_balances).await;
    app2.assert_entries(expected_entries).await;
    app2.assert_keys([account_owner2]).await;

    // Claiming more money from chain1 to chain2.
    app2.claim(
        Account {
            chain_id: chain1,
            owner: account_owner2,
        },
        Account {
            chain_id: chain2,
            owner: account_owner2,
        },
        Amount::from_tokens(2),
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);
    assert_eq!(node_service2.process_inbox(&chain2).await?.len(), 1);

    // Checking the final value
    let expected_balances = [
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::ZERO),
    ];
    let expected_entries = [(account_owner1, Amount::from_tokens(4))];
    app1.assert_balances(expected_balances).await;
    app1.assert_entries(expected_entries).await;
    app1.assert_keys([account_owner1]).await;

    let expected_balances = [
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::from_tokens(3)),
    ];
    let expected_entries = [(account_owner2, Amount::from_tokens(3))];
    app2.assert_balances(expected_balances).await;
    app2.assert_entries(expected_entries).await;
    app2.assert_keys([account_owner2]).await;

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "fungible" ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "native-fungible" ; "native_storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "fungible" ; "scylladb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "native-fungible" ; "native_scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "fungible" ; "aws_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "native-fungible" ; "native_aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "fungible" ; "kubernetes_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "native-fungible" ; "native_kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None), "fungible" ; "remote_net_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None), "native-fungible" ; "native_remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_same_wallet_fungible(
    config: impl LineraNetConfig,
    example_name: &str,
) -> Result<()> {
    use std::collections::BTreeMap;

    use fungible::{InitialState, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    // Get a chain different than the default
    let chain2 = client1
        .load_wallet()?
        .chain_ids()
        .into_iter()
        .find(|chain_id| chain_id != &chain1)
        .expect("Failed to obtain a chain ID from the wallet");

    // The players
    let account_owner1 = get_account_owner(&client1);
    let account_owner2 = client1.keygen().await?;

    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let params = if example_name == "native-fungible" {
        // Native Fungible has a fixed NAT ticker symbol, anything else will be rejected
        Parameters::new("NAT")
    } else {
        Parameters::new("FUN")
    };
    let application_id =
        publish_and_create_native_fungible(&client1, example_name, &params, &state, None).await?;

    let port = get_node_port().await;
    let mut node_service = client1.run_node_service(port, ProcessInbox::Skip).await?;

    let app1 = NativeFungibleApp(
        node_service
            .make_application(&chain1, &application_id)
            .await?,
    );

    let expected_balances: Vec<(AccountOwner, Amount)> = state.accounts.into_iter().collect();

    app1.assert_balances(expected_balances.clone()).await;
    app1.assert_entries(expected_balances).await;
    app1.assert_keys([account_owner1, account_owner2]).await;
    // Transferring
    app1.transfer(
        &account_owner1,
        Amount::ONE,
        Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    assert_eq!(node_service.process_inbox(&chain2).await?.len(), 1);

    // Checking the final values on chain1 and chain2.
    let expected_balances = [
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::from_tokens(2)),
    ];
    app1.assert_balances(expected_balances).await;
    app1.assert_entries(expected_balances).await;
    app1.assert_keys([account_owner1, account_owner2]).await;

    let app2 = NativeFungibleApp(
        node_service
            .make_application(&chain2, &application_id)
            .await?,
    );

    let expected_balances = [(account_owner2, Amount::ONE)];
    app2.assert_balances(expected_balances).await;
    app2.assert_entries(expected_balances).await;
    app2.assert_keys([account_owner2]).await;

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_non_fungible(config: impl LineraNetConfig) -> Result<()> {
    use non_fungible::{NftOutput, NonFungibleTokenAbi};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    // The players
    let account_owner1 = get_account_owner(&client1);
    let account_owner2 = get_account_owner(&client2);

    // Setting up the application and verifying
    let (contract, service) = client1.build_example("non-fungible").await?;
    let application_id = client1
        .publish_and_create::<NonFungibleTokenAbi, (), ()>(
            contract,
            service,
            VmRuntime::Wasm,
            &(),
            &(),
            &[],
            None,
        )
        .await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    let app1 = NonFungibleApp(
        node_service1
            .make_application(&chain1, &application_id)
            .await?,
    );

    let nft1_name = "nft1".to_string();
    let nft1_minter = account_owner1;

    let nft1_blob_bytes = b"nft1_data".to_vec();
    let nft1_blob_hash = CryptoHash::new(&BlobContent::new_data(nft1_blob_bytes.clone()));
    let blob_hash = node_service1
        .publish_data_blob(&chain1, nft1_blob_bytes.clone())
        .await?;
    assert_eq!(nft1_blob_hash, blob_hash);

    let nft1_blob_hash = DataBlobHash(nft1_blob_hash);

    let nft1_id = NonFungibleApp::create_token_id(
        &chain1,
        &application_id.forget_abi(),
        &nft1_name,
        &nft1_minter,
        &nft1_blob_hash,
        0, // No NFTs are supposed to have been minted yet in this chain
    );

    app1.mint(&account_owner1, &nft1_name, &nft1_blob_hash)
        .await;

    let mut expected_nft1 = NftOutput {
        token_id: nft1_id.clone(),
        owner: account_owner1,
        name: nft1_name,
        minter: nft1_minter,
        payload: nft1_blob_bytes,
    };

    assert_eq!(app1.get_nft(&nft1_id).await?, expected_nft1);
    assert!(app1
        .get_owned_nfts(&account_owner1)
        .await?
        .contains(&nft1_id));

    // Transferring to different chain
    app1.transfer(
        &account_owner1,
        &nft1_id,
        &Account {
            chain_id: chain2,
            owner: account_owner1,
        },
    )
    .await;

    assert_eq!(node_service2.process_inbox(&chain2).await?.len(), 1);

    // Checking the NFT is removed from chain1
    assert!(app1.get_nft(&nft1_id).await.is_err());
    assert!(!app1
        .get_owned_nfts(&account_owner1)
        .await?
        .contains(&nft1_id));

    // Non Fungible didn't exist on chain2 initially but now it does and we can talk to it.
    let app2 = NonFungibleApp(
        node_service2
            .make_application(&chain2, &application_id)
            .await?,
    );

    // Checking that the NFT is on chain2 now, with the same owner
    assert_eq!(app2.get_nft(&nft1_id).await?, expected_nft1);
    assert!(app2
        .get_owned_nfts(&account_owner1)
        .await?
        .contains(&nft1_id));

    // Claiming another NFT from chain2 to chain1.
    app1.claim(
        &Account {
            chain_id: chain2,
            owner: account_owner1,
        },
        &nft1_id,
        &Account {
            chain_id: chain1,
            owner: account_owner1,
        },
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    assert_eq!(node_service2.process_inbox(&chain2).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    // Checking the NFT is removed from chain2
    assert!(app2.get_nft(&nft1_id).await.is_err());
    assert!(!app2
        .get_owned_nfts(&account_owner1)
        .await?
        .contains(&nft1_id));
    assert_eq!(app1.get_nft(&nft1_id).await?, expected_nft1);
    assert!(app1
        .get_owned_nfts(&account_owner1)
        .await?
        .contains(&nft1_id));

    // Transferring to different chain and owner
    app1.transfer(
        &account_owner1,
        &nft1_id,
        &Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // The transfer is received by chain2 and needs to be processed.
    assert_eq!(node_service2.process_inbox(&chain2).await?.len(), 1);

    // Checking the NFT is removed from chain1
    assert!(app1.get_nft(&nft1_id).await.is_err());
    assert!(!app1
        .get_owned_nfts(&account_owner1)
        .await?
        .contains(&nft1_id));

    expected_nft1.owner = account_owner2;
    // Checking that the NFT is on chain2 now, with the same updated owner
    assert_eq!(app2.get_nft(&nft1_id).await?, expected_nft1);
    assert!(app2
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft1_id));

    let nft2_name = "nft2".to_string();
    let nft2_minter = account_owner2;
    let nft2_blob_bytes = b"nft2_data".to_vec();
    let nft2_blob_hash = CryptoHash::new(&BlobContent::new_data(nft2_blob_bytes.clone()));
    let blob_hash = node_service2
        .publish_data_blob(&chain2, nft2_blob_bytes.clone())
        .await?;
    assert_eq!(nft2_blob_hash, blob_hash);

    let nft2_blob_hash = DataBlobHash(nft2_blob_hash);

    let nft2_id = NonFungibleApp::create_token_id(
        &chain2,
        &application_id.forget_abi(),
        &nft2_name,
        &nft2_minter,
        &nft2_blob_hash,
        0, // No NFTs are supposed to have been minted yet in this chain
    );

    // Minting NFT from chain2
    app2.mint(&account_owner2, &nft2_name, &nft2_blob_hash)
        .await;

    let expected_nft2 = NftOutput {
        token_id: nft2_id.clone(),
        owner: account_owner2,
        name: nft2_name,
        minter: nft2_minter,
        payload: nft2_blob_bytes,
    };

    // Confirm it's there
    assert_eq!(app2.get_nft(&nft2_id).await?, expected_nft2);
    assert!(app2
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft2_id));

    // Transferring to another chain, maintaining the owner
    app2.transfer(
        &account_owner2,
        &nft2_id,
        &Account {
            chain_id: chain1,
            owner: account_owner2,
        },
    )
    .await;

    // The transfer from chain2 has to be received from chain1.
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    // Checking the NFT is removed from chain2
    assert!(app2.get_nft(&nft2_id).await.is_err());
    assert!(!app2
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft2_id));
    // Checking the NFT is in chain1
    assert_eq!(app1.get_nft(&nft2_id).await?, expected_nft2);
    assert!(app1
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft2_id));

    // Claiming another NFT from chain1 to chain2.
    app2.claim(
        &Account {
            chain_id: chain1,
            owner: account_owner2,
        },
        &nft2_id,
        &Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);
    assert_eq!(node_service2.process_inbox(&chain2).await?.len(), 1);

    // Checking the final state

    // Checking the NFT is removed from chain1
    assert!(app1.get_nft(&nft2_id).await.is_err());
    assert!(!app1
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft2_id));
    assert_eq!(app2.get_nft(&nft2_id).await?, expected_nft2);
    assert!(app2
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft2_id));

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_crowd_funding(config: impl LineraNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    use crowd_funding::{CrowdFundingAbi, InstantiationArgument};
    use fungible::{FungibleTokenAbi, InitialState, Parameters};
    use linera_base::data_types::Timestamp;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    // The players
    let account_owner1 = get_account_owner(&client1); // operator
    let account_owner2 = get_account_owner(&client2); // contributor

    // The initial accounts on chain1
    let accounts = BTreeMap::from([(account_owner1, Amount::from_tokens(6))]);
    let state_fungible = InitialState { accounts };

    // Setting up the application fungible
    let (contract_fungible, service_fungible) = client1.build_example("fungible").await?;
    let params = Parameters::new("FUN");
    let application_id_fungible = client1
        .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
            contract_fungible,
            service_fungible,
            VmRuntime::Wasm,
            &params,
            &state_fungible,
            &[],
            None,
        )
        .await?;

    // Setting up the application crowd funding
    let deadline = Timestamp::from(u64::MAX);
    let target = Amount::ONE;
    let state_crowd = InstantiationArgument {
        owner: account_owner1,
        deadline,
        target,
    };
    let (contract_crowd, service_crowd) = client1.build_example("crowd-funding").await?;
    let application_id_crowd = client1
        .publish_and_create::<CrowdFundingAbi, ApplicationId<FungibleTokenAbi>, InstantiationArgument>(
            contract_crowd,
            service_crowd,
            VmRuntime::Wasm,
            &application_id_fungible,
            &state_crowd,
            &[application_id_fungible.forget_abi()],
            None,
        )
        .await?;

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
    let mut node_service2 = client2.run_node_service(port2, ProcessInbox::Skip).await?;

    let app_fungible1 = FungibleApp(
        node_service1
            .make_application(&chain1, &application_id_fungible)
            .await?,
    );

    let app_crowd1 = node_service1
        .make_application(&chain1, &application_id_crowd)
        .await?;

    // Transferring tokens to user2 on chain2
    app_fungible1
        .transfer(
            &account_owner1,
            Amount::ONE,
            Account {
                chain_id: chain2,
                owner: account_owner2,
            },
        )
        .await;

    // Make sure that the transfer is received before we try to pledge.
    node_service2.process_inbox(&chain2).await?;

    let app_crowd2 = node_service2
        .make_application(&chain2, &application_id_crowd)
        .await?;

    // Transferring
    let mutation = format!(
        "pledge(owner: {}, amount: \"{}\")",
        account_owner2.to_value(),
        Amount::ONE,
    );
    app_crowd2.mutate(mutation).await?;

    // Make sure that the pledge is processed fast enough by client1.
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    // Ending the campaign.
    app_crowd1.mutate("collect").await?;

    // The rich gets their money back.
    app_fungible1
        .assert_balances([(account_owner1, Amount::from_tokens(6))])
        .await;

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_matching_engine(config: impl LineraNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    use matching_engine::{MatchingEngineAbi, OrderNature, Parameters, Price};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client_admin) = config.instantiate().await?;

    let client_a = net.make_client().await;
    let client_b = net.make_client().await;

    client_a.wallet_init(None).await?;
    client_b.wallet_init(None).await?;

    // Create initial server and client config.
    let (contract_fungible_a, service_fungible_a) = client_a.build_example("fungible").await?;
    let (contract_fungible_b, service_fungible_b) = client_b.build_example("fungible").await?;
    let (contract_matching, service_matching) =
        client_admin.build_example("matching-engine").await?;

    let chain_admin = client_admin.load_wallet()?.default_chain().unwrap();
    let chain_a = client_admin.open_and_assign(&client_a, Amount::ONE).await?;
    let chain_b = client_admin.open_and_assign(&client_b, Amount::ONE).await?;

    // The players
    let owner_admin = get_account_owner(&client_admin);
    let owner_a = get_account_owner(&client_a);
    let owner_b = get_account_owner(&client_b);
    // The initial accounts on chain_a and chain_b
    let accounts0 = BTreeMap::from([(owner_a, Amount::from_tokens(10))]);
    let state_fungible0 = fungible::InitialState {
        accounts: accounts0,
    };
    let accounts1 = BTreeMap::from([(owner_b, Amount::from_tokens(9))]);
    let state_fungible1 = fungible::InitialState {
        accounts: accounts1,
    };

    // Setting up the application fungible on chain_a and chain_b
    let params0 = fungible::Parameters::new("ZERO");
    let token0 = client_a
        .publish_and_create::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            contract_fungible_a,
            service_fungible_a,
            VmRuntime::Wasm,
            &params0,
            &state_fungible0,
            &[],
            None,
        )
        .await?;
    let params1 = fungible::Parameters::new("ONE");
    let token1 = client_b
        .publish_and_create::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            contract_fungible_b,
            service_fungible_b,
            VmRuntime::Wasm,
            &params1,
            &state_fungible1,
            &[],
            None,
        )
        .await?;

    // Now creating the service and exporting the applications
    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let port3 = get_node_port().await;
    let mut node_service_admin = client_admin
        .run_node_service(port1, ProcessInbox::Skip)
        .await?;
    let mut node_service_a = client_a.run_node_service(port2, ProcessInbox::Skip).await?;
    let mut node_service_b = client_b.run_node_service(port3, ProcessInbox::Skip).await?;

    let app_fungible0_a = FungibleApp(node_service_a.make_application(&chain_a, &token0).await?);
    let app_fungible1_a = FungibleApp(node_service_a.make_application(&chain_a, &token1).await?);
    let app_fungible0_b = FungibleApp(node_service_b.make_application(&chain_b, &token0).await?);
    let app_fungible1_b = FungibleApp(node_service_b.make_application(&chain_b, &token1).await?);
    app_fungible0_a
        .assert_balances([
            (owner_a, Amount::from_tokens(10)),
            (owner_b, Amount::ZERO),
            (owner_admin, Amount::ZERO),
        ])
        .await;
    app_fungible1_b
        .assert_balances([
            (owner_a, Amount::ZERO),
            (owner_b, Amount::from_tokens(9)),
            (owner_admin, Amount::ZERO),
        ])
        .await;
    let app_fungible0_admin = FungibleApp(
        node_service_admin
            .make_application(&chain_admin, &token0)
            .await?,
    );
    let app_fungible1_admin = FungibleApp(
        node_service_admin
            .make_application(&chain_admin, &token1)
            .await?,
    );
    app_fungible0_admin
        .assert_balances([
            (owner_a, Amount::ZERO),
            (owner_b, Amount::ZERO),
            (owner_admin, Amount::ZERO),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner_a, Amount::ZERO),
            (owner_b, Amount::ZERO),
            (owner_admin, Amount::ZERO),
        ])
        .await;

    // Setting up the application matching engine.
    let parameter = Parameters {
        tokens: [token0, token1],
    };
    let module_id = node_service_admin
        .publish_module::<MatchingEngineAbi, Parameters, ()>(
            &chain_admin,
            contract_matching,
            service_matching,
            VmRuntime::Wasm,
        )
        .await?;
    let application_id_matching = node_service_admin
        .create_application(
            &chain_admin,
            &module_id,
            &parameter,
            &(),
            &[token0.forget_abi(), token1.forget_abi()],
        )
        .await?;
    let app_matching_admin = MatchingEngineApp(
        node_service_admin
            .make_application(&chain_admin, &application_id_matching)
            .await?,
    );

    let app_matching_a = MatchingEngineApp(
        node_service_a
            .make_application(&chain_a, &application_id_matching)
            .await?,
    );
    let app_matching_b = MatchingEngineApp(
        node_service_b
            .make_application(&chain_b, &application_id_matching)
            .await?,
    );

    // Now creating orders
    for price in [1, 2] {
        // 1 is expected not to match, but 2 is expected to match
        app_matching_a
            .order(matching_engine::Order::Insert {
                owner: owner_a,
                amount: Amount::from_tokens(3),
                nature: OrderNature::Bid,
                price: Price { price },
            })
            .await;
    }
    for price in [4, 2] {
        // price 2 is expected to match, but not 4.
        app_matching_b
            .order(matching_engine::Order::Insert {
                owner: owner_b,
                amount: Amount::from_tokens(4),
                nature: OrderNature::Ask,
                price: Price { price },
            })
            .await;
    }
    // The orders are sent on chain_a / chain_b. First they are
    // rerouted to the admin chain for processing. This leads
    // to order being sent to chain_a / chain_b.
    assert_eq!(
        node_service_admin.process_inbox(&chain_admin).await?.len(),
        1
    );
    assert_eq!(node_service_a.process_inbox(&chain_a).await?.len(), 1);
    assert_eq!(node_service_b.process_inbox(&chain_b).await?.len(), 1);

    // Now reading the order_ids
    let order_ids_a = app_matching_admin.get_account_info(&owner_a).await;
    let order_ids_b = app_matching_admin.get_account_info(&owner_b).await;
    // The deal that occurred is that 6 token0 were exchanged for 3 token1.
    assert_eq!(order_ids_a.len(), 1); // The order of price 2 is completely filled.
    assert_eq!(order_ids_b.len(), 2); // The order of price 2 is partially filled.

    // Now cancelling all the orders
    for order_id in order_ids_a {
        app_matching_a
            .order(matching_engine::Order::Cancel {
                owner: owner_a,
                order_id,
            })
            .await;
    }
    for order_id in order_ids_b {
        app_matching_b
            .order(matching_engine::Order::Cancel {
                owner: owner_b,
                order_id,
            })
            .await;
    }

    // Same logic as for the insertion of orders.
    assert_eq!(
        node_service_admin.process_inbox(&chain_admin).await?.len(),
        1
    );
    assert_eq!(node_service_a.process_inbox(&chain_a).await?.len(), 1);
    assert_eq!(node_service_b.process_inbox(&chain_b).await?.len(), 1);

    // Check balances
    app_fungible0_a
        .assert_balances([(owner_a, Amount::from_tokens(4)), (owner_b, Amount::ZERO)])
        .await;
    app_fungible1_a
        .assert_balances([(owner_a, Amount::from_tokens(3)), (owner_b, Amount::ZERO)])
        .await;
    app_fungible0_admin
        .assert_balances([(owner_a, Amount::ZERO), (owner_b, Amount::ZERO)])
        .await;
    app_fungible0_b
        .assert_balances([(owner_a, Amount::ZERO), (owner_b, Amount::from_tokens(6))])
        .await;
    app_fungible1_b
        .assert_balances([(owner_a, Amount::ZERO), (owner_b, Amount::from_tokens(6))])
        .await;
    app_fungible1_admin
        .assert_balances([(owner_a, Amount::ZERO), (owner_b, Amount::ZERO)])
        .await;

    node_service_admin.ensure_is_running()?;
    node_service_a.ensure_is_running()?;
    node_service_b.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(all(feature = "rocksdb", feature = "scylladb"), test_case(LocalNetConfig::new_test(Database::DualRocksDbScyllaDb, Network::Grpc) ; "dualrocksdbscylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_amm(config: impl LineraNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    use amm::{AmmAbi, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client_amm) = config.instantiate().await?;

    let client0 = net.make_client().await;
    let client1 = net.make_client().await;
    client0.wallet_init(None).await?;
    client1.wallet_init(None).await?;

    let (contract_fungible, service_fungible) = client_amm.build_example("fungible").await?;
    let (contract_amm, service_amm) = client_amm.build_example("amm").await?;

    // AMM chain
    let chain_amm = client_amm.load_wallet()?.default_chain().unwrap();

    // User chains
    let chain0 = client_amm.open_and_assign(&client0, Amount::ONE).await?;
    let chain1 = client_amm.open_and_assign(&client1, Amount::ONE).await?;

    // AMM user
    let owner_amm_chain = get_account_owner(&client_amm);

    // Users
    let owner0 = get_account_owner(&client0);
    let owner1 = get_account_owner(&client1);

    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let port3 = get_node_port().await;
    let mut node_service_amm = client_amm
        .run_node_service(port1, ProcessInbox::Skip)
        .await?;
    let mut node_service0 = client0.run_node_service(port2, ProcessInbox::Skip).await?;
    let mut node_service1 = client1.run_node_service(port3, ProcessInbox::Skip).await?;

    // Amounts of token0 that will be owned by each user
    let state_fungible0 = fungible::InitialState {
        accounts: BTreeMap::from([(owner_amm_chain, Amount::from_tokens(270))]),
    };

    // Amounts of token1 that will be owned by each user
    let state_fungible1 = fungible::InitialState {
        accounts: BTreeMap::from([(owner_amm_chain, Amount::from_tokens(250))]),
    };

    // Create fungible applications on the AMM chain, which will hold
    // the token0 and token1 amounts
    let fungible_module_id = node_service_amm
        .publish_module::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            &chain_amm,
            contract_fungible,
            service_fungible,
            VmRuntime::Wasm,
        )
        .await?;

    let params0 = fungible::Parameters::new("ZERO");
    let token0 = node_service_amm
        .create_application(
            &chain_amm,
            &fungible_module_id,
            &params0,
            &state_fungible0,
            &[],
        )
        .await?;
    let params1 = fungible::Parameters::new("ONE");
    let token1 = node_service_amm
        .create_application(
            &chain_amm,
            &fungible_module_id,
            &params1,
            &state_fungible1,
            &[],
        )
        .await?;

    // Create wrappers
    let app_fungible0_amm = FungibleApp(
        node_service_amm
            .make_application(&chain_amm, &token0)
            .await?,
    );
    let app_fungible1_amm = FungibleApp(
        node_service_amm
            .make_application(&chain_amm, &token1)
            .await?,
    );

    // Sending tokens to proper chains
    app_fungible0_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(100),
            Account {
                chain_id: chain0,
                owner: owner0,
            },
        )
        .await;
    app_fungible0_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(170),
            Account {
                chain_id: chain1,
                owner: owner1,
            },
        )
        .await;

    app_fungible1_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(150),
            Account {
                chain_id: chain0,
                owner: owner0,
            },
        )
        .await;
    app_fungible1_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(100),
            Account {
                chain_id: chain1,
                owner: owner1,
            },
        )
        .await;

    assert_eq!(node_service0.process_inbox(&chain0).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    let app_fungible0_0 = FungibleApp(node_service0.make_application(&chain0, &token0).await?);
    let app_fungible1_0 = FungibleApp(node_service0.make_application(&chain0, &token1).await?);

    let app_fungible0_1 = FungibleApp(node_service1.make_application(&chain1, &token0).await?);
    let app_fungible1_1 = FungibleApp(node_service1.make_application(&chain1, &token1).await?);

    // Check initial balances
    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::from_tokens(100)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(150)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(170)),
            (owner_amm_chain, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(100)),
            (owner_amm_chain, Amount::ZERO),
        ])
        .await;

    let parameters = Parameters {
        tokens: [token0, token1],
    };

    // Create AMM application on Admin chain
    let module_id = node_service_amm
        .publish_module::<AmmAbi, Parameters, ()>(
            &chain_amm,
            contract_amm,
            service_amm,
            VmRuntime::Wasm,
        )
        .await?;
    let application_id_amm = node_service_amm
        .create_application(
            &chain_amm,
            &module_id,
            &parameters,
            &(),
            &[token0.forget_abi(), token1.forget_abi()],
        )
        .await?;

    let owner_amm_app = application_id_amm.into();

    // Create AMM wrappers
    let app_amm = AmmApp(
        node_service_amm
            .make_application(&chain_amm, &application_id_amm)
            .await?,
    );

    let app_amm0 = AmmApp(
        node_service0
            .make_application(&chain0, &application_id_amm)
            .await?,
    );
    let app_amm1 = AmmApp(
        node_service1
            .make_application(&chain1, &application_id_amm)
            .await?,
    );

    // Initial balances for both tokens are 0
    app_amm
        .add_liquidity(owner0, Amount::from_tokens(100), Amount::from_tokens(100))
        .await
        .expect_err("Adding liquidity from the AMM chain should fail");

    // Adding liquidity for token0 and token1 by owner0 in chain0, with no refund
    app_amm0
        .add_liquidity(owner0, Amount::from_tokens(100), Amount::from_tokens(100))
        .await?;

    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);

    // Ownership of the used owner_amm_chain's tokens should be with the AMM now
    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(100)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(100)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(170)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(100)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    // Adding more liquidity, with refund
    app_amm1
        .add_liquidity(owner1, Amount::from_tokens(120), Amount::from_tokens(100))
        .await?;

    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(200)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(200)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(70)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_amm
        .swap(owner1, 0, Amount::from_tokens(50))
        .await
        .expect_err("Swapping from the AMM chain should fail");

    app_amm1.swap(owner1, 0, Amount::from_tokens(50)).await?;
    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(250)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(160)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(20)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(40)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_amm1
        .swap(owner1, 0, Amount::from_tokens(50))
        .await
        .expect_err("This swap is supposed to fail with not enough balance!");

    app_amm
        .remove_liquidity(owner1, 0, Amount::from_tokens(50))
        .await
        .expect_err("Can't remove liquidity locally!");

    // This operation is supposed to fail, as it's trying to remove more liquidity
    // than was added
    app_amm1
        .remove_liquidity(owner1, 0, Amount::from_tokens(500))
        .await?;
    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 0);

    // Balances will be unaltered
    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(250)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(160)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(20)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(40)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_amm1.swap(owner1, 1, Amount::from_tokens(40)).await?;
    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(200)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(200)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(70)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_amm1
        .remove_liquidity(owner1, 0, Amount::from_tokens(100))
        .await?;
    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service1.process_inbox(&chain1).await?.len(), 1);

    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(100)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(100)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(170)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(100)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_amm0.swap(owner0, 1, Amount::from_tokens(25)).await?;
    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service0.process_inbox(&chain0).await?.len(), 1);

    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(80)),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::from_tokens(125)),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::from_tokens(20)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(25)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(170)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(100)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_amm0.remove_all_added_liquidity(owner0).await?;
    assert_eq!(node_service_amm.process_inbox(&chain_amm).await?.len(), 1);
    assert_eq!(node_service0.process_inbox(&chain0).await?.len(), 1);

    app_fungible0_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_amm
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_0
        .assert_balances([
            (owner0, Amount::from_tokens(100)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_0
        .assert_balances([
            (owner0, Amount::from_tokens(150)),
            (owner1, Amount::ZERO),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    app_fungible0_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(170)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;
    app_fungible1_1
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(100)),
            (owner_amm_chain, Amount::ZERO),
            (owner_amm_app, Amount::ZERO),
        ])
        .await;

    node_service_amm.ensure_is_running()?;
    node_service0.ensure_is_running()?;
    node_service1.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_node_service(config: impl LineraNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let chain1 = client.load_wallet()?.default_chain().unwrap();
    let owner1 = client.load_wallet()?.get(chain1).unwrap().owner.unwrap();

    // Create a fungible token application with 10 tokens for owner 1.
    let owner = get_account_owner(&client);
    let accounts = BTreeMap::from([(owner, Amount::from_tokens(10))]);
    let state = fungible::InitialState { accounts };
    let (contract, service) = client.build_example("fungible").await?;
    let params = fungible::Parameters::new("FUN");
    let application_id = client
        .publish_and_create::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            contract,
            service,
            VmRuntime::Wasm,
            &params,
            &state,
            &[],
            None,
        )
        .await
        ?;

    let port = get_node_port().await;
    let node_service = client
        .run_node_service(port, ProcessInbox::Automatic)
        .await?;

    // Open a new chain with the same public key.
    // The node service should automatically create a client for it internally.
    let query = format!(
        "mutation {{ openChain(\
            chainId:\"{chain1}\", \
            owner:\"{owner1}\", \
            balance: \"1\"\
        ) }}"
    );
    node_service.query_node(query).await?;

    // Open another new chain.
    // This is a regression test; a PR had to be reverted because this was hanging:
    // https://github.com/linera-io/linera-protocol/pull/899
    // We use openMultiOwnerChain to test that mutation, too, and allow only the fungible app.
    let raw_app_id = application_id.forget_abi();
    let query = format!(
        "mutation {{ openMultiOwnerChain(\
            chainId: \"{chain1}\", \
            owners: [\"{owner1}\"], \
            applicationPermissions: {{ executeOperations: [\"{raw_app_id}\"] }}, \
            balance: \"1\"
        ) }}"
    );
    let data = node_service.query_node(query).await?;
    let chain2: ChainId = serde_json::from_value(data["openMultiOwnerChain"].clone())?;

    // Send 8 tokens to the new chain.
    let app1 = FungibleApp(
        node_service
            .make_application(&chain1, &application_id)
            .await?,
    );
    app1.transfer(
        &owner,
        Amount::from_tokens(8),
        Account {
            chain_id: chain2,
            owner,
        },
    )
    .await;

    // The chain2 must process the received transfer
    node_service.process_inbox(&chain2).await?;

    // Send 4 tokens back.
    let app2 = FungibleApp(
        node_service
            .make_application(&chain2, &application_id)
            .await?,
    );
    app2.transfer(
        &owner,
        Amount::from_tokens(4),
        Account {
            chain_id: chain1,
            owner,
        },
    )
    .await;

    // Verify that the default chain now has 6 and the new one has 4 tokens.
    assert!(
        eventually(|| async {
            let balance1 = app1.get_amount(&owner).await;
            let balance2 = app2.get_amount(&owner).await;
            balance1 == Amount::from_tokens(6) && balance2 == Amount::from_tokens(4)
        })
        .await,
        "Failed to receive new block"
    );
    net.ensure_is_running().await?;
    net.terminate().await?;
    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_multiple_wallets(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create net and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    // Get some chain owned by Client 1.
    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate a key for Client 2.
    let owner2 = client2.keygen().await?;

    // Open chain on behalf of Client 2.
    let (chain2, _) = client1
        .open_chain(chain1, Some(owner2), Amount::ZERO)
        .await?;

    // Assign chain2 to client2_key.
    client2.assign(owner2, chain2).await?;

    // Transfer a token to chain 2. Check that this increases the local balance, proving
    // that client 2 can create blocks on that chain.
    let account2 = Account::chain(chain2);
    assert_eq!(client2.local_balance(account2).await?, Amount::ZERO);
    client1.transfer(Amount::ONE, chain1, chain2).await?;
    client2.sync(chain2).await?;
    // chain2 must process the result
    client2.process_inbox(chain2).await?;
    assert!(client2.local_balance(account2).await? > Amount::ZERO);

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_open_multi_owner_chain(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate keys for both clients.
    let owner1 = client1.keygen().await?;
    let owner2 = client2.keygen().await?;

    // Open a chain owned by both clients.
    let chain2 = client1
        .open_multi_owner_chain(
            chain1,
            vec![owner1, owner2],
            vec![100, 100],
            u32::MAX,
            Amount::from_tokens(6),
            10_000,
        )
        .await?;

    // Assign chain2 to client1_key.
    client1.assign(owner1, chain2).await?;

    // Assign chain2 to client2_key.
    client2.assign(owner2, chain2).await?;

    client2.sync(chain2).await?;

    let account2 = Account::chain(chain2);
    assert_eq!(
        client1.local_balance(account2).await?,
        Amount::from_tokens(6),
    );
    assert_eq!(
        client2.local_balance(account2).await?,
        Amount::from_tokens(6),
    );

    // Transfer 2 + 1 units from Chain 2 to Chain 1 using both clients, leaving 3 (minus fees).
    client2
        .transfer(Amount::from_tokens(2), chain2, chain1)
        .await?;
    client1.transfer(Amount::ONE, chain2, chain1).await?;
    client1.sync(chain1).await?;
    client2.sync(chain2).await?;

    assert!(client1.query_balance(account2).await? <= Amount::from_tokens(3));
    assert!(client2.query_balance(account2).await? <= Amount::from_tokens(3));

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_change_ownership(config: impl LineraNetConfig) -> Result<()> {
    use linera_base::crypto::AccountPublicKey;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and client.
    let (mut net, client) = config.instantiate().await?;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let owner1 = {
        let wallet = client.load_wallet()?;
        let user_chain = wallet.get(chain).unwrap();
        user_chain.owner.unwrap()
    };
    // Generate an owner for which we don't have a secret key in the Signer.
    let owner2 = AccountPublicKey::test_key(2).into();

    // Make both keys owners.
    client
        .change_ownership(chain, vec![], vec![owner1, owner2])
        .await?;

    // Make owner2 the only (super) owner.
    client.change_ownership(chain, vec![owner2], vec![]).await?;
    client.set_preferred_owner(chain, Some(owner2)).await?;
    // Now we're not the owner anymore.
    let result = client.change_ownership(chain, vec![], vec![owner1]).await;
    assert_matches::assert_matches!(result, Err(_));

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_assign_greatgrandchild_chain(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let client3 = net.make_client().await;
    client3.wallet_init(None).await?;

    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate keys for client 2.
    let owner2 = client2.keygen().await?;

    // Open a great-grandchild chain on behalf of client 2.
    let (grandparent, _) = client1
        .open_chain(chain1, None, Amount::from_tokens(2))
        .await?;
    let (parent, _) = client1.open_chain(grandparent, None, Amount::ONE).await?;
    let (chain2, _) = client1
        .open_chain(parent, Some(owner2), Amount::ZERO)
        .await?;
    client2.assign(owner2, chain2).await?;

    // Transfer a token to chain 2. Check that this increases the local balance, proving
    // that client 2 can create blocks on that chain.
    let account2 = Account::chain(chain2);
    assert_eq!(client2.local_balance(account2).await?, Amount::ZERO);
    client1.transfer(Amount::ONE, chain1, chain2).await?;
    client2.sync(chain2).await?;
    client2.process_inbox(chain2).await?;
    assert!(client2.local_balance(account2).await? > Amount::ZERO);

    // Verify that a third party can also follow the chain.
    client3.follow_chain(chain2, true).await?;
    assert!(client3.local_balance(account2).await? > Amount::ZERO);

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_publish_data_blob_in_cli(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;
    client1.open_and_assign(&client2, Amount::ONE).await?;

    let tmp_dir = tempfile::tempdir()?;
    let path = tmp_dir.path().join("hello.txt");
    let mut f = std::fs::File::create_new(&path)?;
    std::io::Write::write_all(&mut f, b"Hello, world!")?;

    let blob_hash = client1.publish_data_blob(&path, None).await?;
    client2.read_data_blob(blob_hash, None).await?;
    client1.read_data_blob(blob_hash, None).await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_faucet(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let balance1 = client1.local_balance(Account::chain(chain1)).await?;

    // Generate keys for client 2.
    let owner2 = client2.keygen().await?;

    let mut faucet_service = client1
        .run_faucet(None, chain1, Amount::from_tokens(2))
        .await?;
    let faucet = faucet_service.instance();
    let chain2 = faucet.claim(&owner2).await?.id();

    // Test version info.
    let info = faucet.version_info().await?;
    assert_eq!(linera_version::VERSION_INFO, info);

    // Use the faucet directly to initialize client 3.
    let client3 = net.make_client().await;
    client3.wallet_init(None).await?;
    let chain_id = client3.request_chain(&faucet, false).await?.0;
    assert_eq!(chain_id, client3.load_wallet()?.default_chain().unwrap());

    let chain_id = client3.request_chain(&faucet, false).await?.0;
    assert!(chain_id != client3.load_wallet()?.default_chain().unwrap());
    client3.forget_chain(chain_id).await?;
    client3.follow_chain(chain_id, false).await?;

    let chain3 = client3.request_chain(&faucet, true).await?.0;
    assert_eq!(chain3, client3.load_wallet()?.default_chain().unwrap());

    faucet_service.ensure_is_running()?;
    faucet_service.terminate().await?;

    // Chain 1 should have transferred four tokens, two to each child.
    client1.sync(chain1).await?;
    let faucet_balance = client1.query_balance(Account::chain(chain1)).await?;
    assert!(faucet_balance <= balance1 - Amount::from_tokens(8));
    assert!(faucet_balance > balance1 - Amount::from_tokens(9));

    // Assign chain2 to client2_key.
    client2.assign(owner2, chain2).await?;

    // Clients 2 and 3 should have the tokens, and own the chain.
    client2.sync(chain2).await?;
    assert_eq!(
        client2.local_balance(Account::chain(chain2)).await?,
        Amount::from_tokens(2),
    );
    client2.transfer(Amount::ONE, chain2, chain1).await?;
    assert!(client2.local_balance(Account::chain(chain2)).await? <= Amount::ONE);

    client3.sync(chain3).await?;
    assert_eq!(
        client3.local_balance(Account::chain(chain3)).await?,
        Amount::from_tokens(2),
    );
    client3.transfer(Amount::ONE, chain3, chain1).await?;
    assert!(client3.query_balance(Account::chain(chain3)).await? <= Amount::ONE);
    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Tests creating a new wallet using a faucet that has already created a lot of microchains.
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
#[ignore = "This test takes a long time to run"]
async fn test_end_to_end_faucet_with_long_chains(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let chain_count = test_iterations().unwrap_or(3_000);

    let (mut net, faucet_client) = config.instantiate().await?;

    let faucet_chain = faucet_client.load_wallet()?.default_chain().unwrap();

    // Use the faucet directly to initialize many chains
    for _ in 0..chain_count {
        let (new_chain_id, _) = faucet_client
            .open_chain(faucet_chain, None, Amount::ONE)
            .await?;
        faucet_client.forget_chain(new_chain_id).await?;
    }

    let new_chain_init_balance = Amount::ONE;
    let mut faucet_service = faucet_client
        .run_faucet(None, faucet_chain, new_chain_init_balance)
        .await?;
    let faucet = faucet_service.instance();

    // Create a new wallet using the faucet
    let client = net.make_client().await;
    client.wallet_init(Some(&faucet)).await?;
    let chain = client.request_chain(&faucet, true).await?.0;
    assert_eq!(chain, client.load_wallet()?.default_chain().unwrap());

    let init_source_balance = client.query_balance(Account::chain(chain)).await?;
    assert_eq!(
        init_source_balance, new_chain_init_balance,
        "Chain balance should be equal to the faucet amount"
    );

    let transfer_amount = Amount::from_millis(1);
    client
        .transfer(transfer_amount, chain, faucet_chain)
        .await?;
    let final_balance = client.query_balance(Account::chain(chain)).await?;

    let transfer_fee = init_source_balance - final_balance - transfer_amount;
    assert!(
        transfer_fee > Amount::ZERO,
        "Transfer fee should be greater than zero"
    );

    assert!(
        final_balance < init_source_balance - transfer_amount,
        "Chain balance should decrease by transfer amount plus fees"
    );

    // This is how much we can transfer to zeroize the account and still pay for fees.
    let amount = final_balance - transfer_fee;
    client.transfer(amount, chain, faucet_chain).await?;

    let chain_balance = client.query_balance(Account::chain(chain)).await?;
    assert_eq!(
        chain_balance,
        Amount::ZERO,
        "Chain balance should be zero after transfer"
    );

    faucet_service.ensure_is_running()?;
    faucet_service.terminate().await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Tests faucet batch processing with multiple concurrent chain creation requests
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_faucet_batch_processing(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let balance1 = client1.local_balance(Account::chain(chain1)).await?;

    // Start faucet with small batch size for testing
    let mut faucet_service = client1
        .run_faucet(None, chain1, Amount::from_tokens(2))
        .await?;
    let faucet = faucet_service.instance();

    // Test batch processing by creating multiple concurrent requests
    const NUM_REQUESTS: usize = 20;
    let mut handles = Vec::new();

    for i in 0..NUM_REQUESTS {
        let faucet_clone = faucet.clone();
        handles.push(async move {
            let owner = AccountOwner::from(
                AccountSecretKey::Secp256k1(Secp256k1SecretKey::generate()).public(),
            );
            tracing::info!("Request {} claiming chain for owner: {}", i, owner);
            faucet_clone.claim(&owner).await
        });
    }

    // Wait for all requests to complete
    let chain_descriptions = future::try_join_all(handles).await?;

    // Verify all chains were created successfully
    assert_eq!(chain_descriptions.len(), NUM_REQUESTS);

    // Verify all chains are unique
    let mut chain_ids = std::collections::HashSet::new();
    for desc in &chain_descriptions {
        assert!(
            chain_ids.insert(desc.id()),
            "Duplicate chain ID: {}",
            desc.id()
        );
    }

    // Test duplicate request handling - should return existing chain
    let owner =
        AccountOwner::from(AccountSecretKey::Secp256k1(Secp256k1SecretKey::generate()).public());
    let first_claim = faucet.claim(&owner).await?;
    let second_claim = faucet.claim(&owner).await?;
    assert_eq!(
        first_claim.id(),
        second_claim.id(),
        "Duplicate request should return same chain"
    );

    faucet_service.ensure_is_running()?;
    faucet_service.terminate().await?;

    // Verify balance was decremented appropriately (NUM_REQUESTS * 2 tokens + fees)
    let final_balance = client1.query_balance(Account::chain(chain1)).await?;
    let expected_transfer = Amount::from_tokens((NUM_REQUESTS * 2) as u128);
    assert!(final_balance <= balance1 - expected_transfer);

    // Verify that fewer than NUM_REQUESTS blocks were created, i.e. some of them were batched.
    let port = get_node_port().await;
    let service = client1.run_node_service(port, ProcessInbox::Skip).await?;
    let query =
        format!("query {{ chain(chainId:\"{chain1}\") {{ tipState {{ nextBlockHeight }} }} }}");
    let response = service.query_node(query).await?;
    let height = response["chain"]["tipState"]["nextBlockHeight"]
        .as_u64()
        .unwrap();
    assert!(height < NUM_REQUESTS as u64);

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_fungible_client_benchmark(config: impl LineraNetConfig) -> Result<()> {
    use linera_base::command::CommandExt;
    use tokio::process::Command;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();

    let mut faucet_service = client1.run_faucet(None, chain1, Amount::ONE).await?;
    let faucet = faucet_service.instance();

    let path =
        linera_base::command::resolve_binary("linera-benchmark", env!("CARGO_PKG_NAME")).await?;
    // The benchmark looks for examples/fungible, so it needs to run in the project root.
    let current_dir = std::env::current_exe()?;
    let dir = current_dir.ancestors().nth(4).unwrap();
    let mut command = Command::new(path);
    command
        .current_dir(dir)
        .arg("fungible")
        .args(["--wallets", "3"])
        .args(["--transactions", "1"])
        .arg("--uniform")
        .args(["--faucet", faucet.url()]);
    let stdout = command.spawn_and_wait_for_stdout().await?;
    let json = serde_json::from_str::<serde_json::Value>(&stdout)?;
    assert_eq!(json["successes"], 3);

    faucet_service.ensure_is_running()?;
    faucet_service.terminate().await?;
    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_listen_for_new_rounds(config: impl LineraNetConfig) -> Result<()> {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use tokio::task::JoinHandle;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;
    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;
    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Open a chain owned by both clients, with only single-leader rounds.
    let owner1 = client1.keygen().await?;
    let owner2 = client2.keygen().await?;
    let chain2 = client1
        .open_multi_owner_chain(
            chain1,
            vec![owner1, owner2],
            vec![100, 100],
            0,
            Amount::from_tokens(9),
            u64::MAX,
        )
        .await?;
    client1.assign(owner1, chain2).await?;
    client2.assign(owner2, chain2).await?;
    client2.sync(chain2).await?;

    let (tx, mut rx) = mpsc::channel(8);
    let drop_barrier = Arc::new(Barrier::new(3));
    let handle1 = tokio::spawn(run_client(
        drop_barrier.clone(),
        client1,
        tx.clone(),
        chain2,
        chain1,
    ));
    let handle2 = tokio::spawn(run_client(
        drop_barrier.clone(),
        client2,
        tx,
        chain2,
        chain1,
    ));

    /// Runs the `client` in a task, so that it can race to produce blocks transferring tokens.
    ///
    /// Stops when transferring fails or the `notifier` channel is closed. When exiting, it will
    /// drop the client in a separate thread so that the synchronous `Drop` implementation
    /// can close the chains without blocking the asynchronous worker thread, which might be
    /// shared with the other client's task. If the asynchronous thread is blocked, the
    /// other client might have the round but not be able to execute and propose a block,
    /// deadlocking the test.
    async fn run_client(
        drop_barrier: Arc<Barrier>,
        client: ClientWrapper,
        mut notifier: mpsc::Sender<()>,
        source: ChainId,
        target: ChainId,
    ) -> Result<JoinHandle<Result<()>>> {
        let result = async {
            loop {
                client.transfer(Amount::ONE, source, target).await?;
                notifier.send(()).await?;
            }
        }
        .await;
        thread::spawn(move || {
            drop(client);
            drop_barrier.wait();
        });
        result
    }

    for _ in 0..8 {
        let () = rx.next().await.unwrap();
    }
    drop(rx);

    let (result1, result2) = futures::join!(handle1, handle2);
    assert!(result1?.is_err());
    assert!(result2?.is_err());

    drop_barrier.wait();
    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Tests token transfers between two chains and measures the latency.
///
/// To use this as a benchmark, make sure to run it with `--nocapture`, e.g.:
///
/// ```bash
/// RUST_LOG=info cargo test -p linera-service \
///     --features storage-service
///     test_end_to_end_repeated_transfers::storage_test_service_grpc \
///     -- --nocapture
/// ```
///
/// It will print the average end-to-end transfer latency, including creating the sending block,
/// the receiving block, and the resulting `NewBlock` notification.
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote-net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_repeated_transfers(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let transfer_count = test_iterations().unwrap_or(100);
    const WARMUP_ITERATIONS: usize = 2;

    // Get a new chain, 1, from the faucet. Use it to open another chain: 2.
    let (mut net, client1) = config.instantiate().await?;
    let chain_id1 = client1.load_wallet()?.default_chain().unwrap();
    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;
    let chain_id2 = client1.open_and_assign(&client2, Amount::ONE).await?;
    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let node_service2 = client2
        .run_node_service(port2, ProcessInbox::Automatic)
        .await?;

    // Make sure all incoming messages are processed, and get both chains' heights.
    let mut next_height1 = {
        let node_service1 = client1.run_node_service(port1, ProcessInbox::Skip).await?;
        node_service1.process_inbox(&chain_id1).await?;
        let mut chain = node_service1
            .query_node(&format!(
                "query {{ chain(chainId: \"{chain_id1}\") {{ tipState {{ nextBlockHeight }} }} }}"
            ))
            .await?;
        serde_json::from_value::<BlockHeight>(chain["chain"]["tipState"]["nextBlockHeight"].take())?
    };
    let mut next_height2 = {
        node_service2.process_inbox(&chain_id2).await?;
        let mut chain = node_service2
            .query_node(&format!(
                "query {{ chain(chainId: \"{chain_id2}\") {{ tipState {{ nextBlockHeight }} }} }}"
            ))
            .await?;
        serde_json::from_value::<BlockHeight>(chain["chain"]["tipState"]["nextBlockHeight"].take())?
    };

    let mut notifications2 = Box::pin(node_service2.notifications(chain_id2).await?);
    let mut block_duration = Duration::ZERO;
    let mut message_duration = Duration::ZERO;

    for i in 0..(WARMUP_ITERATIONS + transfer_count) {
        // Transfer a small amount from chain 1 to chain 2.
        let start_time = Instant::now();
        client1
            .transfer(Amount::from_attos(1), chain_id1, chain_id2)
            .await?;
        let mut got_message = false;

        // Wait until chain 2 created a block receiving the tokens.
        let timeout = Instant::now() + Duration::from_secs(1);
        let hash2 = loop {
            let duration = timeout.duration_since(Instant::now());
            let sleep = Box::pin(linera_base::time::timer::sleep(duration));
            let reason = match future::select(notifications2.next(), sleep).await {
                Either::Left((None, _)) => {
                    panic!("Failed to receive notification about transfer #{i}.");
                }
                Either::Right(((), _)) => {
                    panic!("Timeout to receive notification about transfer #{i}.");
                }
                Either::Left((Some(Err(error)), _)) => {
                    panic!("Error waiting for notification about transfer #{i}: {error}");
                }
                Either::Left((Some(Ok(Notification { reason, chain_id })), _))
                    if chain_id == chain_id2 =>
                {
                    reason
                }
                Either::Left((Some(Ok(_)), _)) => continue,
            };
            match reason {
                Reason::NewIncomingBundle { height, origin } => {
                    assert_eq!(height, next_height1);
                    assert_eq!(origin, chain_id1);
                    assert!(
                        !got_message,
                        "Duplicate message notification about transfer #{i}"
                    );
                    got_message = true;
                    next_height1.0 += 1;
                    if i >= WARMUP_ITERATIONS {
                        message_duration += start_time.elapsed();
                    }
                }
                Reason::NewBlock {
                    height,
                    hash,
                    event_streams,
                } => {
                    assert_eq!(height, next_height2);
                    assert!(event_streams.is_empty());
                    assert!(
                        got_message,
                        "Missing message notification about transfer #{i}"
                    );
                    next_height2.0 += 1;
                    if i >= WARMUP_ITERATIONS {
                        block_duration += start_time.elapsed();
                    }
                    break hash;
                }
                reason @ Reason::NewRound { .. } => {
                    panic!("Unexpected notification about transfer #{i} {reason:?}")
                }
            }
        };

        // Verify that the created block received the transfer message from chain 1.
        let block2 = node_service2
            .query_node(&format!(
                "query {{ block(hash: \"{hash2}\", chainId: \"{chain_id2}\") {{ \
                    block {{ body {{ transactionMetadata {{ \
                        transactionType incomingBundle {{ origin bundle {{ height }} }} \
                    }} }} }} \
                }} }}"
            ))
            .await?;
        // Find the transaction metadata entry that contains an incoming bundle
        let transaction_metadata = &block2["block"]["block"]["body"]["transactionMetadata"];
        let mut bundle = None;
        for metadata in transaction_metadata.as_array().unwrap() {
            if metadata["transactionType"] == "ReceiveMessages" {
                bundle = Some(metadata["incomingBundle"].clone());
                break;
            }
        }
        let mut bundle = bundle.expect("No ReceiveMessages transaction found");
        let origin = serde_json::from_value::<ChainId>(bundle["origin"].take())?;
        assert_eq!(origin, chain_id1);
        let sender_height =
            serde_json::from_value::<BlockHeight>(bundle["bundle"]["height"].take())?;
        assert_eq!(sender_height + BlockHeight(1), next_height1);
    }

    tracing::info!(
        "{transfer_count} transfers completed. Average latency\n\
         * incoming message: {} ms\n\
         * receiving block: {} ms",
        (message_duration / (transfer_count as u32)).as_millis(),
        (block_duration / (transfer_count as u32)).as_millis(),
    );

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}
