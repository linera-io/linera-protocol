// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, path::Path, time::Duration};

use anyhow::{bail, Context as _, Result};
use clap::Parser as _;
use futures::future::{join_all, try_join_all};
use linera_base::{
    async_graphql::InputType,
    data_types::Amount,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, Owner},
    time::timer::Instant,
};
use linera_sdk::abis::fungible::{self, FungibleTokenAbi, InitialState, Parameters};
use linera_service::cli_wrappers::{
    local_net::{PathProvider, ProcessInbox},
    ApplicationWrapper, ClientWrapper, Faucet, FaucetOption, Network,
};
use port_selector::random_free_tcp_port;
use rand::{Rng as _, SeedableRng};
use serde_json::Value;
use tracing::info;

#[derive(clap::Parser)]
#[command(
    name = "linera-benchmark",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "Run benchmarks against a Linera network",
)]
enum Args {
    Fungible {
        /// The number of wallets in the test.
        #[arg(long = "wallets", default_value = "4")]
        wallets: usize,

        /// The number of transactions being made per wallet.
        #[arg(long = "transactions", default_value = "4")]
        transactions: usize,

        /// The faucet (which implicitly defines the network)
        #[arg(long = "faucet", default_value = "http://faucet.devnet.linera.net")]
        faucet: String,

        /// The seed for the PRNG determining the pattern of transactions.
        #[arg(long = "seed", default_value = "0")]
        seed: u64,

        #[arg(long = "uniform")]
        /// If set, each chain receives the exact same amount of transfers.
        uniform: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    linera_base::tracing::init("benchmark");

    let args = Args::parse();
    match args {
        Args::Fungible {
            wallets,
            transactions,
            faucet,
            seed,
            uniform,
        } => {
            benchmark_with_fungible(wallets, transactions, Faucet::new(faucet), seed, uniform).await
        }
    }
}

async fn benchmark_with_fungible(
    num_wallets: usize,
    num_transactions: usize,
    faucet: Faucet,
    seed: u64,
    uniform: bool,
) -> Result<()> {
    info!("Creating the clients and initializing the wallets");
    let path_provider = PathProvider::create_temporary_directory().unwrap();
    let publisher = ClientWrapper::new(path_provider, Network::Grpc, None, num_wallets);
    publisher
        .wallet_init(&[], FaucetOption::NewChain(&faucet))
        .await?;
    let clients = (0..num_wallets)
        .map(|n| {
            let path_provider = PathProvider::create_temporary_directory().unwrap();
            Ok(ClientWrapper::new(path_provider, Network::Grpc, None, n))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    try_join_all(
        clients
            .iter()
            .map(|client| client.wallet_init(&[], FaucetOption::NewChain(&faucet))),
    )
    .await?;

    info!("Synchronizing balances (sanity check)");
    try_join_all(clients.iter().map(|user| async move {
        let chain = user.default_chain().context("missing default chain")?;
        user.sync(chain).await?;
        let balance = user.query_balance(Account::chain(chain)).await?;
        info!("User {:?} has {}", user.get_owner(), balance);
        Ok::<_, anyhow::Error>(())
    }))
    .await?;

    info!("Starting the node services and subscribing to the publisher chain.");
    let mut services = Vec::new();
    for client in &clients {
        let free_port = random_free_tcp_port().context("no free TCP port")?;
        let node_service = client
            .run_node_service(free_port, ProcessInbox::Automatic)
            .await?;
        services.push(node_service);
    }

    info!("Building the fungible application bytecode.");
    let path = Path::new("examples/fungible").canonicalize().context(
        "`linera-benchmark` is meant to run from the root of the `linera-protocol` repository",
    )?;
    let (contract, service) = publisher.build_application(&path, "fungible", true).await?;

    info!("Publishing the fungible application bytecode.");
    let bytecode_id = publisher
        .publish_bytecode::<FungibleTokenAbi, Parameters, InitialState>(contract, service, None)
        .await?;

    struct BenchmarkContext {
        application_id: ApplicationId<FungibleTokenAbi>,
        owner: Owner,
        default_chain: ChainId,
    }

    info!("Creating the fungible applications");
    let apps = try_join_all(clients.iter().zip(services).enumerate().map(
        |(i, (client, node_service))| async move {
            let owner = client.get_owner().context("missing owner")?;
            let default_chain = client.default_chain().context("missing default chain")?;
            let initial_state = InitialState {
                accounts: BTreeMap::from([(
                    AccountOwner::User(owner),
                    Amount::from_tokens(num_transactions as u128),
                )]),
            };
            let parameters = Parameters::new(format!("FUN{}", i).leak());
            let application_id = node_service
                .create_application(
                    &default_chain,
                    &bytecode_id,
                    &parameters,
                    &initial_state,
                    &[],
                )
                .await?;
            let context = BenchmarkContext {
                application_id,
                owner,
                default_chain,
            };
            let app = FungibleApp(
                node_service
                    .make_application(&context.default_chain, &context.application_id)
                    .await?,
            );
            Ok::<_, anyhow::Error>((app, context, node_service))
        },
    ))
    .await?;

    info!("Creating the transaction futures");
    let mut expected_balances = vec![vec![Amount::ZERO; apps.len()]; apps.len()];
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

    let transaction_futures = (0..num_transactions).flat_map(|transaction_i| {
        apps.iter()
            .enumerate()
            .map(|(sender_i, (sender_app, sender_context, _))| {
                let receiver_i = if uniform {
                    (transaction_i + sender_i + 1) % apps.len()
                } else {
                    rng.gen_range(0..apps.len())
                };
                let (_, receiver_context, _) = &apps[receiver_i];
                expected_balances[receiver_i][sender_i]
                    .try_add_assign(Amount::ONE)
                    .unwrap();
                sender_app.transfer(
                    AccountOwner::User(sender_context.owner),
                    Amount::ONE,
                    fungible::Account {
                        chain_id: receiver_context.default_chain,
                        owner: AccountOwner::User(receiver_context.owner),
                    },
                )
            })
            .collect::<Vec<_>>()
    });

    info!("Making {} transactions", num_wallets * num_transactions);

    let timer = Instant::now();

    let results = join_all(transaction_futures).await;
    let successes = results.into_iter().filter(Result::is_ok).count();

    let tps: f64 = successes as f64 / timer.elapsed().as_secs_f64();

    let failures = num_wallets * num_transactions - successes;
    info!("Successes: {:?}", successes);
    info!("Failures:  {:?}", failures);
    info!("TPS:       {:.2}", tps);
    println!(
        "{{\
            \"successes\": {successes},
            \"failures\": {failures},
            \"tps\": {tps}
        }}"
    );

    try_join_all(apps.iter().zip(expected_balances).map(
        |((_, context, node_service), expected_balances)| {
            try_join_all(apps.iter().zip(expected_balances).map(
                |((_, sender_context, _), expected_balance)| async move {
                    if expected_balance == Amount::ZERO {
                        return Ok(()); // No transfers: The app won't be registered on this chain.
                    }
                    let app = FungibleApp(
                        node_service
                            .make_application(
                                &context.default_chain,
                                &sender_context.application_id,
                            )
                            .await?,
                    );
                    for i in 0.. {
                        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
                        let actual_balance =
                            app.get_amount(&AccountOwner::User(context.owner)).await;
                        if actual_balance == expected_balance {
                            break;
                        }
                        if i == 4 {
                            bail!(
                                "Expected balance: {}, actual balance: {}",
                                expected_balance,
                                actual_balance
                            );
                        }
                    }
                    assert_eq!(
                        app.get_amount(&AccountOwner::User(context.owner)).await,
                        expected_balance
                    );
                    Ok(())
                },
            ))
        },
    ))
    .await?;

    Ok(())
}

struct FungibleApp(ApplicationWrapper<FungibleTokenAbi>);

impl FungibleApp {
    async fn get_amount(&self, account_owner: &AccountOwner) -> Amount {
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
        account_owner: AccountOwner,
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
