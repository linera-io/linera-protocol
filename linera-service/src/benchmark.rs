use anyhow::{Context, Result};
use concurrent_queue::ConcurrentQueue;
use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState, Operation};
use futures::{
    future::join_all,
    stream::{FuturesUnordered, StreamExt},
};
use linera_base::{async_graphql::InputType, data_types::Amount};
use linera_service::cli_wrappers::{ApplicationWrapper, ClientWrapper, Network};
use port_selector::random_free_tcp_port;
use rand::prelude::{IteratorRandom, RngCore};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use std::time::Duration;
use structopt::StructOpt;
use tempfile::tempdir;
use tokio::time::Instant;
use linera_base::identifiers::{ApplicationId, ChainId, Owner};

#[derive(StructOpt)]
enum Args {
    #[structopt(name = "fungible")]
    Fungible {
        /// The number of users in the test.
        #[structopt(long = "users", default_value = "4")]
        users: usize,

        /// The number of app instances to be deployed per user.
        #[structopt(long = "apps", default_value = "1")]
        apps: usize,

        /// The number of transactions being made per user.
        #[structopt(long = "transactions", default_value = "4")]
        transactions: usize,

        /// The faucet (which implicitly defines the network)
        #[structopt(long = "faucet", default_value = "http://faucet.devnet.linera.net")]
        faucet: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args();
    match args {
        Args::Fungible {
            users,
            apps,
            transactions,
            faucet,
        } => benchmark_with_fungible(users, apps, transactions, faucet).await,
    }
}

async fn benchmark_with_fungible(
    n_users: usize,
    n_apps: usize,
    n_transactions: usize,
    faucet: String,
) -> Result<()> {
    // Create the users
    let faucet = "http://faucet.devnet.linera.net";
    let clients = join_all(
        (0..n_users)
            .into_iter()
            .map(|n| ClientWrapper::new(Arc::new(tempdir().unwrap()), Network::Grpc, None, n))
            .map(|client| async move {
                client.wallet_init(&[], Some(faucet)).await.unwrap();
                println!("wallet created");
                client
            }),
    )
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
        default_chain: ChainId
    }

    let mut contexts = vec![];

    // Upload fungibles
    for client in &clients {
        let initial_state = InitialState {
            accounts: BTreeMap::from([(
                AccountOwner::User(client.get_owner().unwrap()),
                Amount::from_tokens(1_000_000),
            )]),
        };
        let (contract, service) = client
            .build_application(
                &Path::new("../examples/fungible").canonicalize().unwrap(),
                "fungible",
                true,
            )
            .await
            .unwrap();
        let application_id = client
            .publish_and_create::<FungibleTokenAbi>(
                contract,
                service,
                &(),
                &initial_state,
                &[],
                None,
            )
            .await
            .unwrap();
        println!(
            "User {:?} has published application {:?}",
            client.get_owner(),
            &application_id
        );
        contexts.push(
            BenchmarkContext {
                application_id,
                client,
                owner: client.get_owner().unwrap(),
                default_chain: client.default_chain().unwrap()
            }
        );
    }

    let mut total_transactions = n_users * n_apps * n_transactions;

    let mut apps = vec![];

    // create node-services
    for context in contexts {
        let free_port = random_free_tcp_port().unwrap();
        let node_service = context.client.run_node_service(free_port).await.unwrap();
        let app = FungibleApp(
            node_service
                .make_application(&context.default_chain, &context.application_id)
                .await
                .unwrap(),
        );
        // adding the node service so that it doesn't get dropped.
        apps.push((app, context, node_service));
    }

    // create transaction futures
    let mut transaction_futures = vec![];

    let successes_1 = Arc::new(AtomicUsize::new(0));
    let failures_1 = Arc::new(AtomicUsize::new(0));

    for _ in 0..total_transactions {
        let (sender_app, sender_context, _) = apps.iter().choose(&mut rand::rngs::OsRng).unwrap();
        let (_, receiver_context, _) = apps.iter().choose(&mut rand::rngs::OsRng).unwrap();
        let successes = successes_1.clone();
        let failures = failures_1.clone();
        transaction_futures.push(async move {
            let res = sender_app
                .transfer(
                    AccountOwner::User(sender_context.owner),
                    Amount::ONE,
                    Account {
                        chain_id: receiver_context.default_chain,
                        owner: AccountOwner::User(receiver_context.owner),
                    },
                )
                .await;

            match res {
                Ok(_) => successes.fetch_add(1, Ordering::Relaxed),
                Err(e) => {
                    eprintln!("{:?}", e);
                    failures.fetch_add(1, Ordering::Relaxed)
                },
            }
        });
    }

    println!("ROUND 1");

    let timer = Instant::now();

    join_all(transaction_futures).await;

    let tps: f64 = (successes_1.load(Ordering::Relaxed) + failures_1.load(Ordering::Relaxed)) as f64
        / timer.elapsed().as_secs_f64();

    println!("Successes: {:?}", successes_1);
    println!("Failures:  {:?}", failures_1);


    println!("TPS:       {}", tps);

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
