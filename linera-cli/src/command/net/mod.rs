mod validator;

use crate::io::BoxOutput;

#[derive(clap::Parser, Debug)]
pub struct Options {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Start a new localnet.
    Start(start::Options),
    /// Tear down a localnet.
    Stop,
    /// Administer the set of validators for the current net.
    /// Modifying the set of validators requires being an owner of the admin chain.
    Validator(validator::Options),
}

impl Options {
    pub async fn run(&self, io: &crate::io::Options) -> eyre::Result<BoxOutput> {
        Ok(Box::new(match &self.command {
            Command::Start(start) => start.run().await?,
            Command::Stop => {
                assert!(io.confirm("are you sure?")?);
                todo!("net stop")
            }
            Command::Validator(_) => {
                todo!("net validator")
            }
        }))
    }
}

mod start {
    // We no longer create wallets as part of this command.
    // Instead, use `linera wallet create`.
    use std::num::NonZeroU8;

    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The number of root chains to create on the net.
        #[arg(long, default_value_t = nonzero_lit::u8!(1))]
        root_chains: NonZeroU8,
        /// The number of tokens each root chain starts with.
        #[arg(long, default_value_t = 1_000_000)]
        root_tokens: u128,
        /// The number of validator to run on the net.
        #[arg(long, default_value_t = nonzero_lit::u8!(1))]
        validator: NonZeroU8,
        /// The number of shards that comprise each validator.
        #[arg(long, default_value_t = nonzero_lit::u8!(1))]
        validator_shards: NonZeroU8,
    }

    #[derive(serde::Serialize, Debug)]
    pub struct Output;

    impl std::fmt::Display for Output {
        fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            todo!()
        }
    }

    impl Options {
        pub async fn run(&self) -> eyre::Result<Output> {
            todo!("net up")
        }
    }
}
