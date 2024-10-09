// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functionality for obtaining some free port.

use anyhow::{bail, Result};
use port_selector::random_free_tcp_port;

use crate::time::Duration;

/// Provides a port that is currently not used
pub async fn get_free_port() -> Result<u16> {
    for i in 1..10 {
        let port = random_free_tcp_port();
        if let Some(port) = port {
            return Ok(port);
        }
        crate::time::timer::sleep(Duration::from_secs(i)).await;
    }
    bail!("Failed to obtain a port");
}

/// Provides a local endpoint that is currently available
pub async fn get_free_endpoint() -> Result<String> {
    let port = get_free_port().await?;
    Ok(format!("127.0.0.1:{}", port))
}
