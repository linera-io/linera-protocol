use anyhow::Result;
use clap::Parser as _;
use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState};
use futures::future::join_all;
use linera_base::{
    async_graphql::InputType,
    data_types::Amount,
    identifiers::{ApplicationId, ChainId, Owner},
};
use linera_service::cli_wrappers::{ApplicationWrapper, ClientWrapper, Network};
use port_selector::random_free_tcp_port;
use rand::{seq::IteratorRandom as _, SeedableRng};
use serde_json::Value;
use std::{collections::BTreeMap, iter, path::Path, sync::Arc};
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

        /// The number of app instances to be deployed per user.
        #[arg(long = "apps", default_value = "1")]
        apps: usize,

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
            apps,
            transactions,
            faucet,
            seed,
        } => benchmark_with_fungible(users, apps, transactions, faucet, seed).await,
    }
}

async fn benchmark_with_fungible(
    n_users: usize,
    n_apps: usize,
    n_transactions: usize,
    faucet: String,
    seed: u64,
) -> Result<()> {
    // Create the clients and initialize the wallets
    let clients = (0..n_users)
        .map(|n| ClientWrapper::new(Arc::new(tempdir().unwrap()), Network::Grpc, None, n))
        .collect::<Vec<_>>();
    join_all(clients.iter().map(|client| async {
        client.wallet_init(&[], Some(&faucet)).await.unwrap();
        println!("wallet created");
    }))
    .await;

    // Sync their balances (sanity check)
    join_all(clients.iter().map(|user| async move {
        println!(
            "User {:?} has {}",
            user.get_owner(),
            user.synchronize_balance(user.default_chain().unwrap())
                .await
                .unwrap()
        )
    }))
    .await;

    struct BenchmarkContext<'a> {
        application_id: ApplicationId<FungibleTokenAbi>,
        client: &'a ClientWrapper,
        owner: Owner,
        default_chain: ChainId,
    }

    // Publish and create the fungible applications
    let contexts = join_all(clients.iter().enumerate().map(|(i, client)| async move {
        let initial_state = InitialState {
            accounts: BTreeMap::from([(
                AccountOwner::User(client.get_owner().unwrap()),
                Amount::from_tokens(1_000_000),
            )]),
        };
        let path = Path::new("../examples/fungible").canonicalize().unwrap();
        let (contract, service) = client
            .build_application(&path, "fungible", true)
            .await
            .unwrap();
        let parameters = fungible::Parameters::new(format!("FUN{}", i).leak());
        let application_id = client
            .publish_and_create::<FungibleTokenAbi>(
                contract,
                service,
                &parameters,
                &initial_state,
                &[],
                None,
            )
            .await
            .unwrap();
        BenchmarkContext {
            application_id,
            client,
            owner: client.get_owner().unwrap(),
            default_chain: client.default_chain().unwrap(),
        }
    }))
    .await;
    for context in &contexts {
        println!(
            "User {:?} has published application {:?}",
            context.owner, context.application_id
        );
    }

    // create node services
    let apps = join_all(contexts.iter().map(|context| async move {
        let free_port = random_free_tcp_port().unwrap();
        let node_service = context.client.run_node_service(free_port).await.unwrap();
        let app = FungibleApp(
            node_service
                .make_application(&context.default_chain, &context.application_id)
                .await
                .unwrap(),
        );
        (app, context, node_service)
    }))
    .await;

    // create transaction futures
    let total_transactions = n_users * n_apps * n_transactions;
    let mut expected_balances = vec![vec![Amount::ZERO; contexts.len()]; clients.len()];
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

    let transaction_futures = (0..total_transactions).map(|_| {
        let (sender_i, (sender_app, sender_context, _)) =
            apps.iter().enumerate().choose(&mut rng).unwrap();
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
                    app.assert_balances(iter::once((
                        AccountOwner::User(context.owner),
                        expected_balance,
                    )))
                    .await;
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

    async fn assert_balances(
        &self,
        accounts: impl IntoIterator<Item = (fungible::AccountOwner, Amount)>,
    ) {
        for (account_owner, amount) in accounts {
            let value = self.get_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
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
