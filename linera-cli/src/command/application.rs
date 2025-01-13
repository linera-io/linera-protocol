use linera_base::identifiers::{BytecodeId, ChainId};

#[derive(clap::Parser, Debug)]
pub struct Options {
    /// The chain to use.
    ///
    /// Defaults to the default chain of the wallet.
    #[arg(long, short, global = true)]
    chain: Option<ChainId>,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run an application.
    Run(run::Options),
    /// Create an application by instantiating the application code with its static
    /// parameters.
    Create,
    /// Publish compiled application code to the network.
    Publish(publish::Options),
}

pub type Output = Box<dyn crate::io::Output>;

impl Options {
    pub async fn run(&self) -> eyre::Result<Output> {
        todo!()
    }
}

#[derive(clap::Parser, Clone, Debug)]
// Options required to convert bytecode into application.
struct PublishOptions {
    /// The static application parameters, as a JSON string.
    #[arg(long, default_value = "null")]
    json_params: serde_json::Value,
}

#[derive(clap::Parser, Clone, Debug)]
#[group(required = true, multiple = false)]
// The presence of this struct is a hack due to https://github.com/clap-rs/clap/issues/2621
struct Code {
    /// The code ID of the application code, as produced by `linera application publish`.
    #[arg(long, value_name = "CODE_ID")]
    id: Option<linera_base::identifiers::BytecodeId>,
    /// The path to a Linera application project, as produced by `linera application new`.
    #[arg(long, value_hint = clap::ValueHint::FilePath, value_name = "PATH")]
    source: Option<std::path::PathBuf>,
    /// The path to the compiled bytecode of a Linera application contract and service.
    #[arg(long, value_hint = clap::ValueHint::FilePath, num_args = 2, value_names = ["CONTRACT_PATH", "VALUE_PATH"])]
    compiled: Option<Vec<std::path::PathBuf>>,
}

mod publish {
    use super::*;

    #[derive(clap::Parser, Clone, Debug)]
    pub struct Options {
        #[command(flatten)]
        code: Code,
    }
}

mod run {
    use super::*;

    #[derive(clap::Args, Debug)]
    pub struct Options {
        #[command(flatten)]
        code: Code,
        #[arg(default_value = "null")]
        json_args: serde_json::Value,
    }
}

mod destroy {
    #[derive(clap::Args, Debug)]
    pub struct Options {
        /// The name of the wallet to destroy.
        name: String,
    }
}
