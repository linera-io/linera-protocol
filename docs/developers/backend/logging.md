# Printing Logs from an Application

Applications can use the [`log` crate](https://crates.io/crates/log) to print
log messages with different levels of importance. Log messages are useful during
development, but they may also be useful for end users. By default the
`linera service` command will log the messages from an application if they are
of the "info" importance level or higher (briefly, `log::info!`, `log::warn!`
and `log::error!`).

During development it is often useful to log messages of lower importance (such
as `log::debug!` and `log::trace!`). To enable them, the `RUST_LOG` environment
variable must be set before running `linera service`. The example below enables
trace level messages from applications and enables warning level messages from
other parts of the `linera` binary:

```ignore
export RUST_LOG="warn,linera_execution::wasm=trace"
```
