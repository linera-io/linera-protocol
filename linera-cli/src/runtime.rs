#[derive(clap::Args, Debug)]
#[group(id = "runtime")]
pub struct Options {
    /// The number of additional worker threads to use for I/O.
    ///
    /// Defaults to the number of cores on the system.
    #[arg(long, global = true)]
    threads: Option<u16>,
}

impl Options {
    pub fn build(&self) -> std::io::Result<tokio::runtime::Runtime> {
        use tokio::runtime::Builder;

        let mut builder = if let Some(0) = self.threads {
            Builder::new_current_thread()
        } else {
            Builder::new_multi_thread()
        };

        if let Some(threads) = self.threads {
            builder.worker_threads(threads.into());
        }

        builder.enable_all().build()
    }
}
