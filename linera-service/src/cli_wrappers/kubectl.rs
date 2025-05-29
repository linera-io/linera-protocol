// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

pub struct KubectlInstance {
    pub port_forward_children: Vec<Child>,
}

impl KubectlInstance {
    pub fn new(port_forward_children: Vec<Child>) -> Self {
        Self {
            port_forward_children,
        }
    }

    pub fn port_forward(&mut self, resource: &str, ports: &str, cluster_id: u32) -> Result<()> {
        let port_forward_child = Command::new("kubectl")
            .arg("port-forward")
            .arg(resource)
            .arg(ports)
            .args(["--context", &format!("kind-{}", cluster_id)])
            .spawn()
            .context("Port forwarding failed")?;

        self.port_forward_children.push(port_forward_child);
        Ok(())
    }
}
