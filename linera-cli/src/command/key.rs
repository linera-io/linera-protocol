#[derive(clap::Parser, Debug)]
pub struct Options {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Add a new unassigned keypair to the wallet.
    Create,
    /// *Permanently delete* a keypair from the wallet.
    ///
    /// *THIS ACTION CANNOT BE UNDONE.*
    Delete(delete::Options),
}

mod delete {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to initialize.
        key: linera_base::crypto::PublicKey,
    }
}

impl Options {
    pub async fn run(&self) -> crate::io::Result {
        todo!()
    }
}
