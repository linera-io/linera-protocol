use anyhow::Result;
use linera_service::cli_wrappers::{ClientWrapper, Network};
use std::sync::Arc;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use structopt::StructOpt;
use tempfile::tempdir;
use futures::stream::StreamExt;

#[derive(StructOpt)]
enum Args {
    #[structopt(name = "fungible")]
    Fungible {
        /// The number of users in the test.
        #[structopt(long = "users", default_value = "128")]
        users: usize,

        /// The number of app instances to be deployed.
        #[structopt(long = "apps", default_value = "128")]
        apps: usize,

        /// The number of transactions being made per user.
        #[structopt(long = "transactions", default_value = "128")]
        transactions: usize,
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
        } => benchmark_with_fungible(users, apps, transactions).await,
    }
}

async fn benchmark_with_fungible(
    n_users: usize,
    n_apps: usize,
    n_transactions: usize,
) -> Result<()> {
    let faucet = "http://faucet.devnet.linera.net";
    let users = join_all((0..n_users)
        .into_iter()
        .map(|n| ClientWrapper::new(Arc::new(tempdir().unwrap()), Network::Grpc, None, n))
        .map(|client| async move {
            client.wallet_init(&[], Some(faucet)).await.unwrap();
            client
        })).await;

    unimplemented!()
}
