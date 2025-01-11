#[derive(clap::Parser, Debug)]
pub struct Options {
    #[command(subcommand)]
    command: Command,
}

pub type Name = String;

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Create a new wallet.
    Create(create::Options),
    /// Destroy a wallet.
    /// *THIS OPERATION CANNOT BE UNDONE*.
    Destroy(destroy::Options),
    /// Modify a wallet.
    Set(set::Options),
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
        /// The path to a genesis configuration file to use to create the wallet.
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
        #[cfg(with_rocksdb)]
        #[arg(long, value_hint = clap::ValueHint::FilePath, value_name = "PATH")]
        /// The storage configuration file for this wallet.  See `linera storage` for
        /// format and creation.
        ///
        /// Defaults to creating a new RocksDB storage for the wallet.
        storage: Option<std::path::PathBuf>,
        #[cfg(not(with_rocksdb))]
        #[arg(long, value_hint = clap::ValueHint::FilePath, value_name = "PATH")]
        /// The storage configuration file for this wallet.  See `linera storage` for
        /// format and creation.
        storage: std::path::PathBuf,
    }
}

mod destroy {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to destroy.
        name: String,
    }
}
