mod io;
mod command;
mod runtime;
// mod client;

use clap::{CommandFactory as _, Parser as _};
use futures::FutureExt as _;

type WalletName = String;

#[derive(clap::Parser, Debug)]
#[command(version)]
struct Options {
    /// Print CLI help in Markdown format, and exit.
    #[arg(long, hide = true, global = true)]
    help_markdown: bool,

    /// The name of the wallet to use for wallet operations.
    #[arg(long, short, default_value = "default")]
    wallet: WalletName,

    #[command(flatten)]
    io: io::Options,
    // #[command(flatten)]
    // client: client::Options,
    #[command(flatten)]
    runtime: runtime::Options,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Commands for administering running nets.
    Net(command::net::Options),
    /// Commands for managing wallets.
    Wallet(command::wallet::Options),
    /// Commands for deploying and managing applications.
    Application(command::application::Options),
    // Storage
    // Chain
    // Blob
    // Benchmark
    // Service (Graphql | Faucet)
    // CreateGenesisConfig?!
    // FinalizeCommittee?!
}

impl Options {
    fn run(&self) -> eyre::Result<()> {
        dbg!(self);

        if self.help_markdown {
            clap_markdown::print_help_markdown::<Options>();
            return Ok(())
        }

        let Some(command) = &self.command else {
            Self::command().print_help()?;
            return Ok(())
        };

        let output = self.runtime.build()?.block_on(match command {
            Command::Net(options) => options.run(&self.io).boxed(),
            Command::Wallet(options) => options.run().boxed(),
            Command::Application(options) => options.run().boxed(),
        })?;

        self.io.output(&output)
    }
}

fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    Options::parse().run()
}
