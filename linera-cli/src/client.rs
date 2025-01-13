use std::fmt::Display;

// An option group corresponding to client behaviors.
#[derive(clap::Args, Debug)]
#[group(id = "client")]
pub struct Options {
    /// The name of the wallet to use for wallet operations.
    #[arg(long, short, default_value = "default")]
    wallet: crate::wallet::Name,
}
