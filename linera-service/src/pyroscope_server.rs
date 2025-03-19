// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use pyroscope::{PyroscopeAgent, PyroscopeError};
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn start_pyroscope(
    address: String,
    application_name: String,
    shutdown_signal: CancellationToken,
    sample_rate: u32,
) -> Result<(), PyroscopeError> {
    info!("Starting to push pyroscope data to {:?}", address);
    let pprof_config = PprofConfig::new().sample_rate(sample_rate);
    let backend_impl = pprof_backend(pprof_config);

    let agent = PyroscopeAgent::builder(address, application_name)
        .backend(backend_impl)
        .build()?;
    let agent = agent.start()?;

    tokio::spawn(async move {
        shutdown_signal.cancelled_owned().await;

        // Stop the agent when the server shuts down
        let result = agent.stop();

        if let Err(e) = result {
            panic!("Error stopping pyroscope: {}", e);
        }

        result.unwrap().shutdown();
    });

    Ok(())
}
