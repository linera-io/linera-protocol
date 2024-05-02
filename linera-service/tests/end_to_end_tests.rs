// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod common;

use std::{collections::BTreeMap, env, mem, path::PathBuf, time::Duration};

use anyhow::Result;
use assert_matches::assert_matches;
use async_graphql::InputType;
use common::INTEGRATION_TEST_GUARD;
use futures::{channel::mpsc, SinkExt, StreamExt};
use linera_base::{
    command::resolve_binary,
    crypto::{KeyPair, PublicKey},
    data_types::{Amount, Timestamp},
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, Owner},
};
#[cfg(feature = "remote_net")]
use linera_service::cli_wrappers::remote_net::RemoteNetTestingConfig;
#[cfg(feature = "kubernetes")]
use linera_service::cli_wrappers::{
    docker::BuildArg, local_kubernetes_net::SharedLocalKubernetesNetTestingConfig,
};
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNet, LocalNetConfig, PathProvider},
        ApplicationWrapper, ClientWrapper, FaucetOption, LineraNet, LineraNetConfig, Network,
    },
    config::WalletState,
};
use serde_json::{json, Value};
use test_case::test_case;
use tokio::task::JoinHandle;
use tracing::{info, warn};

fn get_fungible_account_owner(client: &ClientWrapper) -> AccountOwner {
    let owner = client.get_owner().unwrap();
    AccountOwner::User(owner)
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

    async fn entries(&self) -> Vec<native_fungible::AccountEntry> {
        let query = "accounts { entries { key, value } }";
        let response_body = self.0.query(&query).await.unwrap();
        serde_json::from_value(response_body["accounts"]["entries"].clone()).unwrap()
    }

    async fn assert_entries(&self, accounts: impl IntoIterator<Item = (AccountOwner, Amount)>) {
        let entries: BTreeMap<AccountOwner, Amount> = self
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
        destination: fungible::Account,
    ) -> Value {
        let mutation = format!(
            "transfer(owner: {}, amount: \"{}\", targetAccount: {})",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn claim(&self, source: fungible::Account, target: fungible::Account, amount: Amount) {
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

struct NonFungibleApp(ApplicationWrapper<non_fungible::NonFungibleTokenAbi>);

impl NonFungibleApp {
    pub fn create_token_id(
        chain_id: &ChainId,
        application_id: &ApplicationId,
        name: &String,
        minter: &AccountOwner,
        payload: &Vec<u8>,
        num_minted_nfts: u64,
    ) -> String {
        use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
        let token_id_vec = non_fungible::Nft::create_token_id(
            chain_id,
            application_id,
            name,
            minter,
            payload,
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

    async fn mint(&self, minter: &AccountOwner, name: &String, payload: &Vec<u8>) -> Value {
        let mutation = format!(
            "mint(minter: {}, name: {}, payload: {})",
            minter.to_value(),
            name.to_value(),
            payload.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn transfer(
        &self,
        source_owner: &AccountOwner,
        token_id: &String,
        target_account: &fungible::Account,
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
        source_account: &fungible::Account,
        token_id: &String,
        target_account: &fungible::Account,
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

/// Test if the wallet file is correctly locked when used.
#[test_log::test(tokio::test)]
async fn test_wallet_lock() -> Result<()> {
    let config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (_net, client) = config.instantiate().await?;

    let wallet_state = WalletState::from_file(client.wallet_path().as_path())?;
    let chain_id = wallet_state.inner().default_chain().unwrap();

    let lock = wallet_state;
    assert!(client.process_inbox(chain_id).await.is_err());

    mem::drop(lock);
    assert!(client.process_inbox(chain_id).await.is_ok());

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc); "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter(config: impl LineraNetConfig) -> Result<()> {
    use counter::CounterAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await?;

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let (contract, service) = client.build_example("counter").await?;

    let application_id = client
        .publish_and_create::<CounterAbi, (), u64>(
            contract,
            service,
            &(),
            &original_counter_value,
            &[],
            None,
        )
        .await?;
    let mut node_service = client.run_node_service(None).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value);

    let mutation = format!("increment(value: {increment})");
    application.mutate(mutation).await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter_publish_create(config: impl LineraNetConfig) -> Result<()> {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await?;

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let (contract, service) = client.build_example("counter").await?;

    let bytecode_id = client
        .publish_bytecode::<CounterAbi, (), u64>(contract, service, None)
        .await?;
    let application_id = client
        .create_application(&bytecode_id, &(), &original_counter_value, &[], None)
        .await?;
    let mut node_service = client.run_node_service(None).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value);

    let mutation = format!("increment(value: {increment})");
    application.mutate(mutation).await?;

    let counter_value: u64 = application.query_json("value").await?;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_social_user_pub_sub(config: impl LineraNetConfig) -> Result<()> {
    use social::SocialAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;
    let (contract, service) = client1.build_example("social").await?;
    let bytecode_id = client1
        .publish_bytecode::<SocialAbi, (), ()>(contract, service, None)
        .await?;
    let application_id = client1
        .create_application(&bytecode_id, &(), &(), &[], None)
        .await?;

    let mut node_service1 = client1.run_node_service(8080).await?;
    let mut node_service2 = client2.run_node_service(8081).await?;

    node_service1.process_inbox(&chain1).await?;

    // Request the application so chain 2 has it, too.
    node_service2
        .request_application(&chain2, &application_id)
        .await?;

    let app2 = node_service2
        .make_application(&chain2, &application_id)
        .await?;
    let hash = app2
        .mutate(format!("subscribe(chainId: \"{chain1}\")"))
        .await?;

    // The returned hash should now be the latest one.
    let query = format!("query {{ chain(chainId: \"{chain2}\") {{ tipState {{ blockHash }} }} }}");
    let response = node_service2.query_node(&query).await?;
    assert_eq!(hash, response["chain"]["tipState"]["blockHash"]);

    let app1 = node_service1
        .make_application(&chain1, &application_id)
        .await?;
    app1.mutate("post(text: \"Linera Social is the new Mastodon!\")")
        .await?;

    // Instead of retrying, we could call `node_service1.process_inbox(chain1).await` here.
    // However, we prefer to test the notification system for a change.
    let expected_response = json!({
        "receivedPosts": {
            "keys": [
                { "author": chain1, "index": 0 }
            ]
        }
    });
    'success: {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = app2
                .query("receivedPosts { keys { author, index } }")
                .await?;
            if response == expected_response {
                info!("Confirmed post");
                break 'success;
            }
            warn!("Waiting to confirm post: {}", response);
        }
        panic!("Failed to confirm post");
    }

    node_service1.ensure_is_running()?;
    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "fungible" ; "service_grpc")]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "native-fungible" ; "native_service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "fungible" ; "scylladb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "native-fungible" ; "native_scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "fungible" ; "aws_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "native-fungible" ; "native_aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "fungible" ; "kubernetes_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "native-fungible" ; "native_kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None), "fungible" ; "remote_net_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None), "native-fungible" ; "native_remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_fungible(
    config: impl LineraNetConfig,
    example_name: &str,
) -> Result<()> {
    use fungible::{FungibleTokenAbi, InitialState, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    // The players
    let account_owner1 = get_fungible_account_owner(&client1);
    let account_owner2 = get_fungible_account_owner(&client2);
    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let (contract, service) = client1.build_example(example_name).await?;
    let params = if example_name == "native-fungible" {
        // Native Fungible has a fixed NAT ticker symbol, anything else will be rejected
        Parameters::new("NAT")
    } else {
        Parameters::new("FUN")
    };
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
            contract,
            service,
            &params,
            &state,
            &[],
            None,
        )
        .await?;

    let mut node_service1 = client1.run_node_service(8080).await?;
    let mut node_service2 = client2.run_node_service(8081).await?;

    let app1 = FungibleApp(
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
        fungible::Account {
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

    // Fungible didn't exist on chain2 initially but now it does and we can talk to it.
    let app2 = FungibleApp(
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
        fungible::Account {
            chain_id: chain1,
            owner: account_owner2,
        },
        fungible::Account {
            chain_id: chain2,
            owner: account_owner2,
        },
        Amount::from_tokens(2),
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox(&chain1).await?;
    node_service2.process_inbox(&chain2).await?;

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

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "fungible" ; "service_grpc")]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc), "native-fungible" ; "native_service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "fungible" ; "scylladb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc), "native-fungible" ; "native_scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "fungible" ; "aws_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc), "native-fungible" ; "native_aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "fungible" ; "kubernetes_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build), "native-fungible" ; "native_kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None), "fungible" ; "remote_net_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None), "native-fungible" ; "native_remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_same_wallet_fungible(
    config: impl LineraNetConfig,
    example_name: &str,
) -> Result<()> {
    use fungible::{Account, FungibleTokenAbi, InitialState, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
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
    let account_owner1 = get_fungible_account_owner(&client1);
    let account_owner2 = {
        let wallet = client1.load_wallet()?;
        let user_chain = wallet.get(chain2).unwrap();
        let public_key = user_chain.key_pair.as_ref().unwrap().public();
        AccountOwner::User(public_key.into())
    };
    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let (contract, service) = client1.build_example(example_name).await?;
    let params = if example_name == "native-fungible" {
        // Native Fungible has a fixed NAT ticker symbol, anything else will be rejected
        Parameters::new("NAT")
    } else {
        Parameters::new("FUN")
    };
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
            contract,
            service,
            &params,
            &state,
            &[],
            None,
        )
        .await?;

    let mut node_service = client1.run_node_service(8080).await?;

    let app1 = FungibleApp(
        node_service
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

    let app2 = FungibleApp(
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

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_non_fungible(config: impl LineraNetConfig) -> Result<()> {
    use non_fungible::{NftOutput, NonFungibleTokenAbi};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    // The players
    let account_owner1 = get_fungible_account_owner(&client1);
    let account_owner2 = get_fungible_account_owner(&client2);

    // Setting up the application and verifying
    let (contract, service) = client1.build_example("non-fungible").await?;
    let application_id = client1
        .publish_and_create::<NonFungibleTokenAbi, (), ()>(contract, service, &(), &(), &[], None)
        .await?;

    let mut node_service1 = client1.run_node_service(8080).await?;
    let mut node_service2 = client2.run_node_service(8081).await?;

    let app1 = NonFungibleApp(
        node_service1
            .make_application(&chain1, &application_id)
            .await?,
    );

    let nft1_name = "nft1".to_string();
    let nft1_minter = account_owner1;
    let nft1_payload = "nft1_data".as_bytes().to_vec();

    let nft1_id = NonFungibleApp::create_token_id(
        &chain1,
        &application_id.forget_abi(),
        &nft1_name,
        &nft1_minter,
        &nft1_payload,
        0, // No NFTs are supposed to have been minted yet in this chain
    );

    app1.mint(&account_owner1, &nft1_name, &nft1_payload).await;

    let mut expected_nft1 = NftOutput {
        token_id: nft1_id.clone(),
        owner: account_owner1,
        name: nft1_name,
        minter: nft1_minter,
        payload: nft1_payload,
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
        &fungible::Account {
            chain_id: chain2,
            owner: account_owner1,
        },
    )
    .await;

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
        &fungible::Account {
            chain_id: chain2,
            owner: account_owner1,
        },
        &nft1_id,
        &fungible::Account {
            chain_id: chain1,
            owner: account_owner1,
        },
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox(&chain1).await?;
    node_service2.process_inbox(&chain2).await?;

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
        &fungible::Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox(&chain1).await?;
    node_service2.process_inbox(&chain2).await?;

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
    let nft2_payload = "nft2_data".as_bytes().to_vec();

    let nft2_id = NonFungibleApp::create_token_id(
        &chain2,
        &application_id.forget_abi(),
        &nft2_name,
        &nft2_minter,
        &nft2_payload,
        0, // No NFTs are supposed to have been minted yet in this chain
    );

    // Minting NFT from chain2
    app2.mint(&account_owner2, &nft2_name, &nft2_payload).await;

    let expected_nft2 = NftOutput {
        token_id: nft2_id.clone(),
        owner: account_owner2,
        name: nft2_name,
        minter: nft2_minter,
        payload: nft2_payload,
    };

    // Confirm it's there
    assert_eq!(app2.get_nft(&nft2_id).await?, expected_nft2);
    assert!(app2
        .get_owned_nfts(&account_owner2)
        .await?
        .contains(&nft2_id));

    // Transferring to another chain, maitaining the owner
    app2.transfer(
        &account_owner2,
        &nft2_id,
        &fungible::Account {
            chain_id: chain1,
            owner: account_owner2,
        },
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox(&chain1).await?;
    node_service2.process_inbox(&chain2).await?;

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
        &fungible::Account {
            chain_id: chain1,
            owner: account_owner2,
        },
        &nft2_id,
        &fungible::Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox(&chain1).await?;
    node_service2.process_inbox(&chain2).await?;

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

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_crowd_funding(config: impl LineraNetConfig) -> Result<()> {
    use crowd_funding::{CrowdFundingAbi, InstantiationArgument};
    use fungible::{FungibleTokenAbi, InitialState, Parameters};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2, Amount::ONE).await?;

    // The players
    let account_owner1 = get_fungible_account_owner(&client1); // operator
    let account_owner2 = get_fungible_account_owner(&client2); // contributor

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
            &params,
            &state_fungible,
            &[],
            None,
        )
        .await?;

    // Setting up the application crowd funding
    let deadline = Timestamp::from(std::u64::MAX);
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
            // TODO(#723): This hack will disappear soon.
            &application_id_fungible,
            &state_crowd,
            &[application_id_fungible.forget_abi()],
            None,
        )
        .await?;

    let mut node_service1 = client1.run_node_service(8080).await?;
    let mut node_service2 = client2.run_node_service(8081).await?;

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
            fungible::Account {
                chain_id: chain2,
                owner: account_owner2,
            },
        )
        .await;

    // Register the campaign on chain2.
    node_service2
        .request_application(&chain2, &application_id_crowd)
        .await?;

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
    node_service1.process_inbox(&chain1).await?;

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

// TODO(#1159): We should enable the matching engine on other storages.
// #[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
// #[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
// #[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
// #[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
// #[cfg_attr(feature = "rocksdb", test_case(LocalNetConfig::new_test(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_matching_engine(config: impl LineraNetConfig) -> Result<()> {
    use matching_engine::{MatchingEngineAbi, OrderNature, Parameters, Price};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client_admin) = config.instantiate().await?;

    let client_a = net.make_client().await;
    let client_b = net.make_client().await;

    client_a.wallet_init(&[], FaucetOption::None).await?;
    client_b.wallet_init(&[], FaucetOption::None).await?;

    // Create initial server and client config.
    let (contract_fungible_a, service_fungible_a) = client_a.build_example("fungible").await?;
    let (contract_fungible_b, service_fungible_b) = client_b.build_example("fungible").await?;
    let (contract_matching, service_matching) =
        client_admin.build_example("matching-engine").await?;

    let chain_admin = client_admin.load_wallet()?.default_chain().unwrap();
    let chain_a = client_admin.open_and_assign(&client_a, Amount::ONE).await?;
    let chain_b = client_admin.open_and_assign(&client_b, Amount::ONE).await?;

    // The players
    let owner_admin = get_fungible_account_owner(&client_admin);
    let owner_a = get_fungible_account_owner(&client_a);
    let owner_b = get_fungible_account_owner(&client_b);
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
            &params1,
            &state_fungible1,
            &[],
            None,
        )
        .await?;

    // Now creating the service and exporting the applications
    let mut node_service_admin = client_admin.run_node_service(8080).await?;
    let mut node_service_a = client_a.run_node_service(8081).await?;
    let mut node_service_b = client_b.run_node_service(8082).await?;

    node_service_a
        .request_application(&chain_a, &token1)
        .await?;
    node_service_b
        .request_application(&chain_b, &token0)
        .await?;
    node_service_admin
        .request_application(&chain_admin, &token0)
        .await?;
    node_service_admin
        .request_application(&chain_admin, &token1)
        .await?;

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
    let bytecode_id = node_service_admin
        .publish_bytecode::<MatchingEngineAbi, Parameters, ()>(
            &chain_admin,
            contract_matching,
            service_matching,
        )
        .await?;
    let application_id_matching = node_service_admin
        .create_application(
            &chain_admin,
            &bytecode_id,
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
    node_service_a
        .request_application(&chain_a, &application_id_matching)
        .await?;
    let app_matching_a = MatchingEngineApp(
        node_service_a
            .make_application(&chain_a, &application_id_matching)
            .await?,
    );
    node_service_b
        .request_application(&chain_b, &application_id_matching)
        .await?;
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
    node_service_admin.process_inbox(&chain_admin).await?;
    node_service_a.process_inbox(&chain_a).await?;
    node_service_b.process_inbox(&chain_b).await?;

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

    let chain_a_tip_after_cancel = node_service_a
        .chain_tip_hash(chain_a)
        .await?
        .expect("Missing blocks from chain A");
    node_service_admin
        .wait_for_messages(chain_admin, chain_a_tip_after_cancel)
        .await?;

    let chain_a_refund_admin_tip = node_service_admin
        .chain_tip_hash(chain_admin)
        .await?
        .expect("Missing blocks from admin chain");

    for order_id in order_ids_b {
        app_matching_b
            .order(matching_engine::Order::Cancel {
                owner: owner_b,
                order_id,
            })
            .await;
    }

    let chain_b_tip_after_cancel = node_service_b
        .chain_tip_hash(chain_b)
        .await?
        .expect("Missing blocks from chain B");

    node_service_admin
        .wait_for_messages(chain_admin, chain_b_tip_after_cancel)
        .await?;

    let chain_b_refund_admin_tip = node_service_admin
        .chain_tip_hash(chain_admin)
        .await?
        .expect("Missing blocks from admin chain");

    node_service_a
        .wait_for_messages(chain_a, chain_a_refund_admin_tip)
        .await?;

    node_service_b
        .wait_for_messages(chain_b, chain_b_refund_admin_tip)
        .await?;

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

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_amm(config: impl LineraNetConfig) -> Result<()> {
    use amm::{AmmAbi, Parameters};
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client_amm) = config.instantiate().await?;

    let client0 = net.make_client().await;
    let client1 = net.make_client().await;
    client0.wallet_init(&[], FaucetOption::None).await?;
    client1.wallet_init(&[], FaucetOption::None).await?;

    let (contract_fungible, service_fungible) = client_amm.build_example("fungible").await?;
    let (contract_amm, service_amm) = client_amm.build_example("amm").await?;

    // AMM chain
    let chain_amm = client_amm.load_wallet()?.default_chain().unwrap();

    // User chains
    let chain0 = client_amm.open_and_assign(&client0, Amount::ONE).await?;
    let chain1 = client_amm.open_and_assign(&client1, Amount::ONE).await?;

    // AMM user
    let owner_amm_chain = get_fungible_account_owner(&client_amm);

    // Users
    let owner0 = get_fungible_account_owner(&client0);
    let owner1 = get_fungible_account_owner(&client1);

    let mut node_service_amm = client_amm.run_node_service(8080).await?;
    let mut node_service0 = client0.run_node_service(8081).await?;
    let mut node_service1 = client1.run_node_service(8082).await?;

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
    let fungible_bytecode_id = node_service_amm
        .publish_bytecode::<
            fungible::FungibleTokenAbi,
            fungible::Parameters,
            fungible::InitialState
        >(&chain_amm, contract_fungible, service_fungible).await?;

    let params0 = fungible::Parameters::new("ZERO");
    let token0 = node_service_amm
        .create_application(
            &chain_amm,
            &fungible_bytecode_id,
            &params0,
            &state_fungible0,
            &[],
        )
        .await?;
    let params1 = fungible::Parameters::new("ONE");
    let token1 = node_service_amm
        .create_application(
            &chain_amm,
            &fungible_bytecode_id,
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
            fungible::Account {
                chain_id: chain0,
                owner: owner0,
            },
        )
        .await;
    app_fungible0_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(170),
            fungible::Account {
                chain_id: chain1,
                owner: owner1,
            },
        )
        .await;

    app_fungible1_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(150),
            fungible::Account {
                chain_id: chain0,
                owner: owner0,
            },
        )
        .await;
    app_fungible1_amm
        .transfer(
            &owner_amm_chain,
            Amount::from_tokens(100),
            fungible::Account {
                chain_id: chain1,
                owner: owner1,
            },
        )
        .await;

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
    let bytecode_id = node_service_amm
        .publish_bytecode::<AmmAbi, Parameters, ()>(&chain_amm, contract_amm, service_amm)
        .await?;
    let application_id_amm = node_service_amm
        .create_application(
            &chain_amm,
            &bytecode_id,
            &parameters,
            &(),
            &[token0.forget_abi(), token1.forget_abi()],
        )
        .await?;

    let owner_amm_app = AccountOwner::Application(application_id_amm.forget_abi());

    // Create AMM wrappers
    let app_amm = AmmApp(
        node_service_amm
            .make_application(&chain_amm, &application_id_amm)
            .await?,
    );
    node_service0
        .request_application(&chain0, &application_id_amm)
        .await?;
    let app_amm0 = AmmApp(
        node_service0
            .make_application(&chain0, &application_id_amm)
            .await?,
    );
    node_service1
        .request_application(&chain1, &application_id_amm)
        .await?;
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

    node_service_amm.process_inbox(&chain_amm).await?;
    node_service0.process_inbox(&chain0).await?;

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

    node_service_amm.process_inbox(&chain_amm).await?;
    node_service1.process_inbox(&chain1).await?;

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
    node_service_amm.process_inbox(&chain_amm).await?;
    node_service1.process_inbox(&chain1).await?;

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
    node_service_amm.process_inbox(&chain_amm).await?;
    node_service1.process_inbox(&chain1).await?;

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
    node_service_amm.process_inbox(&chain_amm).await?;
    node_service1.process_inbox(&chain1).await?;

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
    node_service_amm.process_inbox(&chain_amm).await?;
    node_service1.process_inbox(&chain1).await?;

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
    node_service_amm.process_inbox(&chain_amm).await?;
    node_service0.process_inbox(&chain0).await?;

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
    node_service_amm.process_inbox(&chain_amm).await?;
    node_service0.process_inbox(&chain0).await?;

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

#[test_log::test(tokio::test)]
async fn test_resolve_binary() -> Result<()> {
    resolve_binary("linera", env!("CARGO_PKG_NAME")).await?;
    resolve_binary("linera-proxy", env!("CARGO_PKG_NAME")).await?;
    assert!(resolve_binary("linera-spaceship", env!("CARGO_PKG_NAME"))
        .await
        .is_err());

    Ok(())
}

// TODO(#1655): Make the scylladb_udp / rocksdb_udp test work.
//#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Udp) ; "scylladb_udp"))]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "service_tcp")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_reconfiguration(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let network = config.network;
    let (mut net, client) = config.instantiate().await?;

    let client_2 = net.make_client().await;
    client_2.wallet_init(&[], FaucetOption::None).await?;
    let chain_1 = ChainId::root(0);

    let chain_2 = client
        .open_and_assign(&client_2, Amount::from_tokens(3))
        .await?;
    let node_service_2 = match network {
        Network::Grpc => Some(client_2.run_node_service(8081).await?),
        Network::Tcp | Network::Udp => None,
    };

    client.query_validators(None).await?;

    // Restart the first shard for the 4th validator.
    net.terminate_server(3, 0).await?;
    net.start_server(3, 0).await?;

    // Create configurations for two more validators
    net.generate_validator_config(4).await?;
    net.generate_validator_config(5).await?;

    // Start the validators
    net.start_validator(4).await?;
    net.start_validator(5).await?;

    // Add 5th validator
    client
        .set_validator(net.validator_name(4).unwrap(), LocalNet::proxy_port(4), 100)
        .await?;

    client.query_validators(None).await?;
    client.query_validators(Some(chain_1)).await?;

    // Add 6th validator
    client
        .set_validator(net.validator_name(5).unwrap(), LocalNet::proxy_port(5), 100)
        .await?;

    // Remove 5th validator
    client
        .remove_validator(net.validator_name(4).unwrap())
        .await?;
    net.remove_validator(4)?;

    client.query_validators(None).await?;
    client.query_validators(Some(chain_1)).await?;

    // Remove the first 4 validators, so only the last one remains.
    for i in 0..4 {
        let name = net.validator_name(i).unwrap();
        client.remove_validator(name).await?;
        net.remove_validator(i)?;
        if node_service_2.is_none() {
            client_2.process_inbox(chain_2).await?;
        }
    }

    let recipient = Owner::from(KeyPair::generate().public());
    client
        .transfer_with_accounts(
            Amount::from_tokens(5),
            Account::chain(chain_1),
            Account::owner(chain_2, recipient),
        )
        .await?;

    if let Some(node_service_2) = node_service_2 {
        let query = format!(
            "query {{ chain(chainId:\"{chain_2}\") {{
                executionState {{ system {{ balances {{
                    entry(key:\"{recipient}\") {{ value }}
                }} }} }}
            }} }}"
        );
        for i in 0.. {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = node_service_2.query_node(query.clone()).await?;
            let balances = &response["chain"]["executionState"]["system"]["balances"];
            if balances["entry"]["value"].as_str() == Some("5.") {
                break;
            }
            assert!(i < 3, "Failed to receive new block");
        }
    } else {
        client_2.sync(chain_2).await?;
        client_2.process_inbox(chain_2).await?;
        assert_eq!(
            client_2
                .local_balance(Account::owner(chain_2, recipient))
                .await?,
            Amount::from_tokens(5),
        );
    }

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_node_service(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let (mut net, client) = config.instantiate().await?;

    let chain1 = client.load_wallet()?.default_chain().unwrap();
    let public_key = client
        .load_wallet()?
        .get(chain1)
        .unwrap()
        .key_pair
        .as_ref()
        .unwrap()
        .public();

    // Create a fungible token application with 10 tokens for owner 1.
    let owner = get_fungible_account_owner(&client);
    let accounts = BTreeMap::from([(owner, Amount::from_tokens(10))]);
    let state = fungible::InitialState { accounts };
    let (contract, service) = client.build_example("fungible").await?;
    let params = fungible::Parameters::new("FUN");
    let application_id = client
        .publish_and_create::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            contract,
            service,
            &params,
            &state,
            &[],
            None,
        )
        .await
        ?;

    let node_service = client.run_node_service(8080).await?;

    // Open a new chain with the same public key.
    // The node service should automatically create a client for it internally.
    let query = format!(
        "mutation {{ openChain(\
            chainId:\"{chain1}\", \
            publicKey:\"{public_key}\"\
        ) }}"
    );
    node_service.query_node(query).await?;

    // Open another new chain.
    // This is a regression test; a PR had to be reverted because this was hanging:
    // https://github.com/linera-io/linera-protocol/pull/899
    let query = format!(
        "mutation {{ openChain(\
            chainId:\"{chain1}\", \
            publicKey:\"{public_key}\", \
            balance:\"1\"
        ) }}"
    );
    let data = node_service.query_node(query).await?;
    let chain2: ChainId = serde_json::from_value(data["openChain"].clone())?;

    // Send 8 tokens to the new chain.
    let app1 = FungibleApp(
        node_service
            .make_application(&chain1, &application_id)
            .await?,
    );
    app1.transfer(
        &owner,
        Amount::from_tokens(8),
        fungible::Account {
            chain_id: chain2,
            owner,
        },
    )
    .await;

    // Send 4 tokens back.
    let app2 = FungibleApp(
        node_service
            .make_application(&chain2, &application_id)
            .await?,
    );
    app2.transfer(
        &owner,
        Amount::from_tokens(4),
        fungible::Account {
            chain_id: chain1,
            owner,
        },
    )
    .await;

    // Verify that the default chain now has 6 and the new one has 4 tokens.
    for i in 0..10 {
        tokio::time::sleep(Duration::from_secs(i)).await;
        let balance1 = app1.get_amount(&owner).await;
        let balance2 = app2.get_amount(&owner).await;
        if balance1 == Amount::from_tokens(6) && balance2 == Amount::from_tokens(4) {
            net.ensure_is_running().await?;
            net.terminate().await?;
            return Ok(());
        }
    }
    panic!("Failed to receive new block");
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_notification_stream(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    let chain = ChainId::root(0);
    let mut height = 0;
    client2.wallet_init(&[chain], FaucetOption::None).await?;

    // Listen for updates on root chain 0. There are no blocks on that chain yet.
    let mut node_service2 = client2.run_node_service(8081).await?;
    let response = node_service2
        .query_node(format!(
            "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
        ))
        .await?;
    assert_eq!(
        response["chain"]["tipState"]["nextBlockHeight"].as_u64(),
        Some(height)
    );

    // Oh no! The first validator has an outage and gets restarted!
    net.remove_validator(0)?;
    net.start_validator(0).await?;

    // The node service should try to reconnect.
    'success: {
        for i in 0..10 {
            // Add a new block on the chain, triggering a notification.
            client1
                .transfer(Amount::from_tokens(1), chain, ChainId::root(9))
                .await?;
            tokio::time::sleep(Duration::from_secs(i)).await;
            height += 1;
            let response = node_service2
                .query_node(format!(
                    "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
                ))
                .await?;
            if response["chain"]["tipState"]["nextBlockHeight"].as_u64() == Some(height) {
                break 'success;
            }
        }
        panic!("Failed to re-establish notification stream");
    }

    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_multiple_wallets(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create net and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    // Get some chain owned by Client 1.
    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate a key for Client 2.
    let client2_key = client2.keygen().await?;

    // Open chain on behalf of Client 2.
    let (message_id, chain2) = client1
        .open_chain(chain1, Some(client2_key), Amount::ZERO)
        .await?;

    // Assign chain2 to client2_key.
    assert_eq!(chain2, client2.assign(client2_key, message_id).await?);

    // Transfer a token to chain 2. Check that this increases the local balance, proving
    // that client 2 can create blocks on that chain.
    let account2 = Account::chain(chain2);
    assert_eq!(client2.local_balance(account2).await?, Amount::ZERO);
    client1.transfer(Amount::ONE, chain1, chain2).await?;
    client2.sync(chain2).await?;
    client2.process_inbox(chain2).await?;
    assert!(client2.local_balance(account2).await? > Amount::ZERO);

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_project_new() -> Result<()> {
    let _rustflags_override = override_disable_warnings_as_errors();
    let path_provider = PathProvider::create_temporary_directory()?;
    let client = ClientWrapper::new(path_provider, Network::Grpc, None, 0);
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await?;
    let project_dir = tmp_dir.path().join("init-test");
    client
        .build_application(project_dir.as_path(), "init-test", false)
        .await?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_project_test() -> Result<()> {
    let path_provider = PathProvider::create_temporary_directory()?;
    let client = ClientWrapper::new(path_provider, Network::Grpc, None, 0);
    client
        .project_test(&ClientWrapper::example_path("counter")?)
        .await?;

    Ok(())
}

#[test_case(Database::Service, Network::Grpc ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_project_publish(database: Database, network: Network) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let _rustflags_override = override_disable_warnings_as_errors();
    let config = LocalNetConfig {
        num_initial_validators: 1,
        num_shards: 1,
        ..LocalNetConfig::new_test(database, network)
    };

    let (mut net, client) = config.instantiate().await?;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await?;
    let project_dir = tmp_dir.path().join("init-test");

    client
        .project_publish(project_dir, vec![], None, &())
        .await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    let node_service = client.run_node_service(None).await?;

    assert_eq!(
        node_service.try_get_applications_uri(&chain).await?.len(),
        1
    );

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_linera_net_up_simple() -> Result<()> {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let mut command = Command::new(env!("CARGO_BIN_EXE_linera"));
    command.args(["net", "up"]);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let stderr = BufReader::new(child.stderr.take().unwrap());

    for line in stderr.lines() {
        let line = line?;
        if line.starts_with("READY!") {
            let mut exports = stdout.lines();
            assert!(exports
                .next()
                .unwrap()?
                .starts_with("export LINERA_WALLET="));
            assert!(exports
                .next()
                .unwrap()?
                .starts_with("export LINERA_STORAGE="));
            assert_eq!(exports.next().unwrap()?, "");

            // Send SIGINT to the child process.
            Command::new("kill")
                .args(["-s", "INT", &child.id().to_string()])
                .output()?;

            assert!(exports.next().is_none());
            assert!(child.wait()?.success());
            return Ok(());
        }
    }
    panic!("Unexpected EOF for stderr");
}

#[test_case(Database::Service, Network::Grpc ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_example_publish(database: Database, network: Network) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let config = LocalNetConfig {
        num_initial_validators: 1,
        num_shards: 1,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;

    let example_dir = ClientWrapper::example_path("counter")?;
    client
        .project_publish(example_dir, vec![], None, &0)
        .await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    let node_service = client.run_node_service(None).await?;

    assert_eq!(
        node_service.try_get_applications_uri(&chain).await?.len(),
        1
    );

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_open_multi_owner_chain(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate keys for both clients.
    let client1_key = client1.keygen().await?;
    let client2_key = client2.keygen().await?;

    // Open a chain owned by both clients.
    let (message_id, chain2) = client1
        .open_multi_owner_chain(
            chain1,
            vec![client1_key, client2_key],
            vec![100, 100],
            u32::MAX,
            Amount::from_tokens(6),
            10_000,
        )
        .await?;

    // Assign chain2 to client1_key.
    assert_eq!(chain2, client1.assign(client1_key, message_id).await?);

    // Assign chain2 to client2_key.
    assert_eq!(chain2, client2.assign(client2_key, message_id).await?);

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

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_change_ownership(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and client.
    let (mut net, client) = config.instantiate().await?;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let pub_key1 = {
        let wallet = client.load_wallet()?;
        let user_chain = wallet.get(chain).unwrap();
        user_chain.key_pair.as_ref().unwrap().public()
    };
    let pub_key2 = PublicKey::test_key(2);

    // Make both keys owners.
    client
        .change_ownership(chain, vec![], vec![pub_key1, pub_key2])
        .await?;

    // Make pub_key2 the only (super) owner.
    client
        .change_ownership(chain, vec![pub_key2], vec![])
        .await?;

    // Now we're not the owner anymore.
    let result = client.change_ownership(chain, vec![], vec![pub_key1]).await;
    assert_matches!(result, Err(_));

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_assign_greatgrandchild_chain(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Generate keys for client 2.
    let client2_key = client2.keygen().await?;

    // Open a great-grandchild chain on behalf of client 2.
    let (_, grandparent) = client1
        .open_chain(chain1, None, Amount::from_tokens(2))
        .await?;
    let (_, parent) = client1.open_chain(grandparent, None, Amount::ONE).await?;
    let (message_id, chain2) = client1
        .open_chain(parent, Some(client2_key), Amount::ZERO)
        .await?;
    client2.assign(client2_key, message_id).await?;

    // Transfer a token to chain 2. Check that this increases the local balance, proving
    // that client 2 can create blocks on that chain.
    let account2 = Account::chain(chain2);
    assert_eq!(client2.local_balance(account2).await?, Amount::ZERO);
    client1.transfer(Amount::ONE, chain1, chain2).await?;
    client2.sync(chain2).await?;
    client2.process_inbox(chain2).await?;
    assert!(client2.local_balance(account2).await? > Amount::ZERO);

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_faucet(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();
    let balance1 = client1.local_balance(Account::chain(chain1)).await?;

    // Generate keys for client 2.
    let client2_key = client2.keygen().await?;

    let mut faucet_service = client1
        .run_faucet(None, chain1, Amount::from_tokens(2))
        .await?;
    let faucet = faucet_service.instance();
    let outcome = faucet.claim(&client2_key).await?;
    let chain2 = outcome.chain_id;
    let message_id = outcome.message_id;

    // Test version info.
    let info = faucet.version_info().await?;
    assert_eq!(linera_version::VERSION_INFO, info);

    // Use the faucet directly to initialize client 3.
    let client3 = net.make_client().await;
    let outcome = client3
        .wallet_init(&[], FaucetOption::NewChain(&faucet))
        .await?;
    let chain3 = outcome.unwrap().chain_id;
    assert_eq!(chain3, client3.load_wallet()?.default_chain().unwrap());

    faucet_service.ensure_is_running()?;
    faucet_service.terminate().await?;

    // Chain 1 should have transferred four tokens, two to each child.
    client1.sync(chain1).await?;
    let faucet_balance = client1.query_balance(Account::chain(chain1)).await?;
    assert!(faucet_balance <= balance1 - Amount::from_tokens(4));
    assert!(faucet_balance > balance1 - Amount::from_tokens(5));

    // Assign chain2 to client2_key.
    assert_eq!(chain2, client2.assign(client2_key, message_id).await?);

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

#[cfg(feature = "benchmark")]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_fungible_benchmark(config: impl LineraNetConfig) -> Result<()> {
    use linera_base::command::CommandExt;
    use tokio::process::Command;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;

    let chain1 = client1.load_wallet()?.default_chain().unwrap();

    let mut faucet_service = client1.run_faucet(None, chain1, Amount::ONE).await?;
    let faucet = faucet_service.instance();

    let path = resolve_binary("linera-benchmark", env!("CARGO_PKG_NAME")).await?;
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

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_pending_block(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    // Create runner and client.
    let (mut net, client) = config.instantiate().await?;
    let chain_id = client.load_wallet()?.default_chain().unwrap();
    let account = Account::chain(chain_id);
    let balance = client.local_balance(account).await?;
    // Stop validators.
    for i in 0..4 {
        net.remove_validator(i)?;
    }
    let result = client
        .transfer_with_silent_logs(Amount::from_tokens(2), chain_id, ChainId::root(5))
        .await;
    assert!(result.is_err());
    // The transfer didn't get confirmed.
    assert_eq!(client.local_balance(account).await?, balance);
    // Restart validators.
    for i in 0..4 {
        net.start_validator(i).await?;
    }
    let result = client.retry_pending_block(Some(chain_id)).await;
    assert!(result?.is_some());
    client.sync(chain_id).await?;
    // After retrying, the transfer got confirmed.
    assert!(client.local_balance(account).await? <= balance - Amount::from_tokens(2));
    let result = client.retry_pending_block(Some(chain_id)).await;
    assert!(result?.is_none());

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(feature = "benchmark")]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "service_tcp")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_benchmark(mut config: LocalNetConfig) -> Result<()> {
    use fungible::{FungibleTokenAbi, InitialState, Parameters};

    config.num_other_initial_chains = 2;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let (mut net, client) = config.instantiate().await?;

    assert_eq!(client.load_wallet()?.num_chains(), 2);
    // Launch local benchmark using all user chains and creating additional ones.
    client.benchmark(2, 4, 10, None).await?;
    assert_eq!(client.load_wallet()?.num_chains(), 4);

    // Now we run the benchmark again, with the fungible token application instead of the
    // native token.
    let account_owner = get_fungible_account_owner(&client);
    let accounts = BTreeMap::from([(account_owner, Amount::from_tokens(1_000_000))]);
    let state = InitialState { accounts };
    let (contract, service) = client.build_example("fungible").await?;
    let params = Parameters::new("FUN");
    let application_id = client
        .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
            contract,
            service,
            &params,
            &state,
            &[],
            None,
        )
        .await?;
    client.benchmark(2, 5, 10, Some(application_id)).await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Clears the `RUSTFLAGS` environment variable, if it was configured to make warnings fail as
/// errors.
///
/// The returned [`RestoreVarOnDrop`] restores the environment variable to its original value when
/// it is dropped.
fn override_disable_warnings_as_errors() -> Option<RestoreVarOnDrop> {
    if matches!(env::var("RUSTFLAGS"), Ok(value) if value == "-D warnings") {
        env::set_var("RUSTFLAGS", "");
        Some(RestoreVarOnDrop)
    } else {
        None
    }
}

/// Restores the `RUSTFLAGS` environment variable to make warnings fail as errors.
struct RestoreVarOnDrop;

impl Drop for RestoreVarOnDrop {
    fn drop(&mut self) {
        env::set_var("RUSTFLAGS", "-D warnings");
    }
}

#[test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "service_grpc")]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, BuildArg::Build) ; "kubernetes_grpc"))]
#[cfg_attr(feature = "remote_net", test_case(RemoteNetTestingConfig::new(None) ; "remote_net_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_listen_for_new_rounds(config: impl LineraNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await?;
    let client2 = net.make_client().await;
    client2.wallet_init(&[], FaucetOption::None).await?;
    let chain1 = *client1.load_wallet()?.chain_ids().first().unwrap();

    // Open a chain owned by both clients, with only single-leader rounds.
    let client1_key = client1.keygen().await?;
    let client2_key = client2.keygen().await?;
    let (message_id, chain2) = client1
        .open_multi_owner_chain(
            chain1,
            vec![client1_key, client2_key],
            vec![100, 100],
            0,
            Amount::from_tokens(9),
            u64::MAX,
        )
        .await?;
    client1.assign(client1_key, message_id).await?;
    client2.assign(client2_key, message_id).await?;
    client2.sync(chain2).await?;

    let (mut tx1, mut rx) = mpsc::channel(8);
    let mut tx2 = tx1.clone();
    let handle1: JoinHandle<Result<()>> = tokio::spawn(async move {
        loop {
            client1.transfer(Amount::ONE, chain2, chain1).await?;
            tx1.send(()).await?;
        }
    });
    let handle2: JoinHandle<Result<()>> = tokio::spawn(async move {
        loop {
            client2.transfer(Amount::ONE, chain2, chain1).await?;
            tx2.send(()).await?;
        }
    });

    for _ in 0..8 {
        let () = rx.next().await.unwrap();
    }
    drop(rx);

    let (result1, result2) = futures::join!(handle1, handle2);
    assert!(result1?.is_err());
    assert!(result2?.is_err());

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}
