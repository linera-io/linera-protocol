use std::fmt::Display;

#[derive(clap::Args, Debug)]
#[group(id = "io")]
pub struct Options {
    /// The format to use for output.
    #[arg(name = "output", short, long, value_enum, default_value_t)]
    output_format: OutputFormat,
    /// Don't ask for confirmation.
    #[arg(name = "yes", short, long, global = true)]
    no_confirm: bool,
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

#[derive(clap::ValueEnum, Clone, Debug, Default)]
pub enum OutputFormat {
    #[default]
    /// A human-readable output format.
    Plain,
    /// JSON.
    Json,
}

pub trait Output: erased_serde::Serialize + Display {
    fn boxed(self) -> BoxOutput where Self: Sized + 'static {
        Box::new(self)
    }
}

erased_serde::serialize_trait_object!(Output);
impl<T: serde::Serialize + Display> Output for T { }

impl OutputFormat {
    pub fn write(&self, value: &impl Output, writer: &mut impl std::io::Write) -> eyre::Result<()> {
        use OutputFormat::*;

        match self {
            Plain => write!(writer, "{value}")?,
            Json => value.erased_serialize(&mut <dyn erased_serde::Serializer>::erase(&mut serde_json::Serializer::pretty(writer)))?,
        }

        Ok(())
    }
}

impl Options {
    pub fn output(&self, output: &impl Output) -> eyre::Result<()> {
        self.output_format.write(output, &mut std::io::stdout())
    }

    pub fn confirm(&self, prompt: &str) -> eyre::Result<bool> {
        Ok(self.no_confirm || inquire::Confirm::new(prompt).with_default(false).prompt()?)
    }
}

pub type BoxOutput = Box<dyn Output>;
