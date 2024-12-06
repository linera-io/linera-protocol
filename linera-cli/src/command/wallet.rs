#[derive(clap::Parser, Debug)]
pub struct Options {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Create a new wallet.
    ///
    /// # Storage backends
    /// TODO document
    Create(create::Options),
    /// Destroy a wallet.
    /// THIS OPERATION CANNOT BE UNDONE.
    Destroy(destroy::Options),
    /// Add a new keypair.
    Keygen,
}

pub type Output = Box<dyn crate::io::Output>;

impl Options {
    pub async fn run(&self) -> eyre::Result<Output> {
        todo!()
    }
}

mod create {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to create.
        name: String,
        /// The URL of a faucet to use to create the wallet.  If no faucet is provided,
        /// try to use the faucet of the current testnet.
        // TODO we can do better than this
        // TODO do we need to create a chain here? or can we postpone it to a `chain create`?
        #[arg(long, default_value = "https://faucet.testnet-archimedes.linera.net/")]
        faucet: url::Url,
        /// Whether to set this wallet as default.
        #[arg(long)]
        default: bool,
        /// The storage backend to use for this wallet.
        // TODO think harder about this
        /// If RocksDB was enabled, will default to creating a new
        /// RocksDB storage for the wallet.
        #[cfg(with_rocksdb)]
        #[arg(long)]
        storage: Option<linera_client::storage::StorageConfigNamespace>,
        #[cfg(not(with_rocksdb))]
        #[arg(long)]
        storage: linera_client::storage::StorageConfigNamespace,
    }
}

mod destroy {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to destroy.
        name: String,
    }
}
