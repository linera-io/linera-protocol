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
    /// *THIS OPERATION CANNOT BE UNDONE*.
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
    #[derive(clap::Parser, Clone, Debug)]
    #[group(required = true, multiple = false)]
    // The presence of this struct is a hack due to https://github.com/clap-rs/clap/issues/2621
    struct Upstream {
        /// The URL of a faucet to use to create the wallet.
        #[clap(long, value_hint = clap::ValueHint::Url, value_name = "URL")]
        faucet: Option<url::Url>,
        #[clap(long, value_hint = clap::ValueHint::FilePath, value_name = "PATH")]
        /// The path to a genesis configuration to use to create the wallet.
        genesis_config: Option<std::path::PathBuf>,
    }

    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to create.
        name: String,
        #[clap(flatten)]
        upstream: Upstream,
        /// Whether to set this wallet as default.
        // TODO support editing the wallet after creation
        #[arg(long)]
        default: bool,
        /// The storage backend to use for this wallet.
        // TODO think harder about this, particularly the format
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
