// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use linera_base::command::CommandExt;
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

    pub fn port_forward(&mut self, pod_name: &str, ports: &str, cluster_id: u32) -> Result<()> {
        let port_forward_child = Command::new("kubectl")
            .arg("port-forward")
            .arg(pod_name)
            .arg(ports)
            .args(["--context", &format!("kind-{}", cluster_id)])
            .spawn()
            .context("Port forwarding failed")?;

        self.port_forward_children.push(port_forward_child);
        Ok(())
    }

    pub async fn get_pods(&mut self, cluster_id: u32) -> Result<String> {
        let output = Command::new("kubectl")
            .arg("get")
            .arg("pods")
            .args(["--context", &format!("kind-{}", cluster_id)])
            .spawn_and_wait_for_stdout()
            .await?;

        Ok(String::from_utf8_lossy(output.as_bytes()).to_string())
    }
}
