// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util::CommandExt;
use anyhow::{Context, Result};
use tokio::process::{Child, Command};

pub struct KubectlInstance {
    pub port_forward_child: Option<Child>,
}

impl KubectlInstance {
    pub fn new(port_forward_child: Option<Child>) -> Self {
        Self { port_forward_child }
    }

    pub async fn port_forward(&mut self, pod_name: &str, ports: &str) -> Result<()> {
        self.port_forward_child = Some(
            Command::new("kubectl")
                .arg("port-forward")
                .arg(pod_name)
                .arg(ports)
                .spawn()
                .context("Port forwarding failed")?,
        );
        Ok(())
    }

    pub async fn get_pods(&mut self) -> Result<String> {
        Command::new("kubectl")
            .arg("get")
            .arg("pods")
            .spawn_and_wait_for_stdout()
            .await
    }
}
