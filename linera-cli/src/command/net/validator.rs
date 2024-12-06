#[derive(clap::Parser, Debug)]
pub struct Options {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
pub enum Command {
    /// List the validators of the committee.
    List(list::Options),
    /// Add or update a validator in the committee.
    Set(set::Options),
    /// Remove a validator from the committee.
    Remove(remove::Options),
    /// Finalize the set of validators, invalidating all previous committees.
    Finalize,
}

pub type Output = Box<dyn crate::io::Output>;

impl Command {
    pub async fn run(&self) -> eyre::Result<Box<Output>> {
        todo!("net")
    }
}

mod list {
    #[derive(clap::Args, Debug)]
    pub struct Options;

    #[derive(serde::Serialize, Debug)]
    pub struct Output;

    impl Options {
        pub async fn run(&self) -> eyre::Result<Output> {
            todo!("net validator list")
        }
    }
}

mod set {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The public key of the validator.
        name: linera_execution::committee::ValidatorName,
        /// The network address of the validator.
        // TODO type
        #[arg(long)]
        address: String,
        /// The validator's voting power.
        #[arg(long)]
        votes: u64,
        /// Don't check the validator's version or genesis config.
        #[arg(long)]
        no_check: bool,
    }
}

mod remove {
    #[derive(clap::Args, Debug)]
    pub struct Options;
}
