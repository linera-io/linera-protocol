pub type Name = String;

#[derive(clap::Parser, Debug)]
pub struct Options {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Initialize the wallet.
    Init(init::Options),
    /// Display the contents of the wallet.
    Show,
    /// Add a new keypair.
    Keygen,
}

pub type Output = Box<dyn crate::io::Output>;

impl Options {
    pub async fn run(&self) -> eyre::Result<Output> {
        todo!()
    }
}

mod init {
    #[derive(clap::Parser, Clone, Debug)]
    #[group(required = true, multiple = false)]
    // The presence of this struct is a hack due to https://github.com/clap-rs/clap/issues/2621
    struct Upstream {
        /// The URL of a faucet to use to init the wallet.
        #[arg(long, value_hint = clap::ValueHint::Url, value_name = "URL")]
        faucet: Option<url::Url>,
        #[arg(long, value_hint = clap::ValueHint::FilePath, value_name = "PATH")]
        /// The path to a genesis configuration file to use to initialize the wallet.
        genesis_config: Option<std::path::PathBuf>,
    }

    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to initialize.
        name: String,
        #[clap(flatten)]
        upstream: Upstream,
        #[arg(long, value_hint = clap::ValueHint::FilePath, value_name = "PATH")]
        /// The storage configuration file for this wallet.  See `linera storage` for
        /// format and creation.
        ///
        #[cfg_attr(feature = "rocksdb", doc = "Defaults to creating a new RocksDB storage for the wallet.")]
        #[cfg_attr(not(feature = "rocksdb"), doc = "Defaults to creating a new in-memory storage for the wallet.")]
        storage: Option<std::path::PathBuf>,
    }
}

mod destroy {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to destroy.
        name: String,
    }
}
