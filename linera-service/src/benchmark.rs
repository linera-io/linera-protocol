// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser as _;
use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState, Parameters};
use futures::future::join_all;
use linera_base::{
    async_graphql::InputType,
    data_types::Amount,
    identifiers::{ApplicationId, ChainId, Owner},
};
use linera_execution::system::SystemChannel;
use linera_service::cli_wrappers::{ApplicationWrapper, ClientWrapper, Network};
use port_selector::random_free_tcp_port;
use rand::{seq::IteratorRandom as _, SeedableRng};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tempfile::tempdir;
use tokio::time::Instant;

#[derive(clap::Parser)]
#[command(
    name = "linera-benchmark",
    version = clap::crate_version!(),
    about = "Run benchmarks against a Linera network",
)]
enum Args {
    Fungible {
        /// The number of users in the test.
        #[arg(long = "users", default_value = "4")]
        users: usize,

        /// The number of transactions being made per user.
        #[arg(long = "transactions", default_value = "4")]
        transactions: usize,

        /// The faucet (which implicitly defines the network)
        #[arg(long = "faucet", default_value = "http://faucet.devnet.linera.net")]
        faucet: String,

        /// The seed for the PRNG determining the pattern of transactions.
        #[arg(long = "seed", default_value = "0")]
        seed: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args {
        Args::Fungible {
            users,
            transactions,
            faucet,
            seed,
        } => benchmark_with_fungible(users, transactions, faucet, seed).await,
    }
}

async fn benchmark_with_fungible(
    n_users: usize,
    n_transactions: usize,
    faucet: String,
    seed: u64,
) -> Result<()> {
    // Create the clients and initialize the wallets
    let publisher = ClientWrapper::new(Arc::new(tempdir().unwrap()), Network::Grpc, None, n_users);
    publisher.wallet_init(&[], Some(&faucet)).await.unwrap();
    let clients = (0..n_users)
        .map(|n| ClientWrapper::new(Arc::new(tempdir().unwrap()), Network::Grpc, None, n))
        .collect::<Vec<_>>();
    join_all(clients.iter().map(|client| async {
        client.wallet_init(&[], Some(&faucet)).await.unwrap();
    }))
    .await;

    // Sync their balances (sanity check)
    join_all(clients.iter().map(|user| async move {
        let chain = user.default_chain().unwrap();
        let balance = user.synchronize_balance(chain).await.unwrap();
        println!("User {:?} has {}", user.get_owner(), balance)
    }))
    .await;

    // Start the node services and subscribe to the publisher chain.
    let publisher_chain_id = publisher.default_chain().unwrap();
    let services = join_all(clients.iter().map(|client| async move {
        let free_port = random_free_tcp_port().unwrap();
        let node_service = client.run_node_service(free_port).await.unwrap();
        let chain_id = client.default_chain().unwrap();
        let channel = SystemChannel::PublishedBytecodes;
        node_service
            .subscribe(chain_id, publisher_chain_id, channel)
            .await
            .unwrap();
        node_service
    }))
    .await;

    // Publish the fungible application bytecode.
    let path = Path::new("../examples/fungible").canonicalize().unwrap();
    let (contract, service) = publisher.build_application(&path, "fungible", true).await?;
    let bytecode_id = publisher.publish_bytecode(contract, service, None).await?;

    struct BenchmarkContext {
        application_id: ApplicationId<FungibleTokenAbi>,
        owner: Owner,
        default_chain: ChainId,
    }

    // Create the fungible applications
    let apps = join_all(clients.iter().zip(services).enumerate().map(
        |(i, (client, node_service))| async move {
            let initial_state = InitialState {
                accounts: BTreeMap::from([(
                    AccountOwner::User(client.get_owner().unwrap()),
                    Amount::from_tokens(n_transactions as u128),
                )]),
            };
            let parameters = Parameters::new(format!("FUN{}", i).leak());
            let application_id = client
                .create_application::<FungibleTokenAbi>(
                    &bytecode_id,
                    &parameters,
                    &initial_state,
                    &[],
                    None,
                )
                .await
                .unwrap();
            let owner = client.get_owner().unwrap();
            let default_chain = client.default_chain().unwrap();
            let context = BenchmarkContext {
                application_id,
                owner,
                default_chain,
            };
            let app = FungibleApp(
                node_service
                    .make_application(&context.default_chain, &context.application_id)
                    .await
                    .unwrap(),
            );
            (app, context, node_service)
        },
    ))
    .await;

    // create transaction futures
    let total_transactions = n_users * n_transactions;
    let mut expected_balances = vec![vec![Amount::ZERO; apps.len()]; apps.len()];
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

    let transaction_futures = (0..n_transactions).flat_map(|_| {
        apps.iter()
            .enumerate()
            .map(|(sender_i, (sender_app, sender_context, _))| {
                let (receiver_i, (_, receiver_context, _)) =
                    apps.iter().enumerate().choose(&mut rng).unwrap();
                expected_balances[receiver_i][sender_i]
                    .try_add_assign(Amount::ONE)
                    .unwrap();
                sender_app.transfer(
                    AccountOwner::User(sender_context.owner),
                    Amount::ONE,
                    Account {
                        chain_id: receiver_context.default_chain,
                        owner: AccountOwner::User(receiver_context.owner),
                    },
                )
            })
            .collect::<Vec<_>>()
    });

    println!("ROUND 1");

    let timer = Instant::now();

    let results = join_all(transaction_futures).await;
    let successes = results.into_iter().filter(Result::is_ok).count();

    let tps: f64 = successes as f64 / timer.elapsed().as_secs_f64();

    println!("Successes: {:?}", successes);
    println!("Failures:  {:?}", total_transactions - successes);

    println!("TPS:       {}", tps);

    join_all(apps.iter().zip(expected_balances).map(
        |((_, context, node_service), expected_balances)| {
            join_all(apps.iter().zip(expected_balances).map(
                |((_, sender_context, _), expected_balance)| async move {
                    let app = FungibleApp(
                        node_service
                            .make_application(
                                &context.default_chain,
                                &sender_context.application_id,
                            )
                            .await
                            .unwrap(),
                    );
                    assert_eq!(
                        app.get_amount(&AccountOwner::User(context.owner)).await,
                        expected_balance
                    );
                },
            ))
        },
    ))
    .await;

    Ok(())
}

struct FungibleApp(ApplicationWrapper<fungible::FungibleTokenAbi>);

impl FungibleApp {
    async fn get_amount(&self, account_owner: &fungible::AccountOwner) -> Amount {
        let query = format!(
            "accounts {{ entry(key: {}) {{ value }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await.unwrap();
        serde_json::from_value(response_body["accounts"]["entry"]["value"].clone())
            .unwrap_or_default()
    }

    async fn transfer(
        &self,
        account_owner: fungible::AccountOwner,
        amount_transfer: Amount,
        destination: fungible::Account,
    ) -> Result<Value> {
        let mutation = format!(
            "transfer(owner: {}, amount: \"{}\", targetAccount: {})",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await
    }
}
