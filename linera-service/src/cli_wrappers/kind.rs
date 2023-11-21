// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util::CommandExt;
use anyhow::Result;
use rand::Rng;
use tokio::process::Command;

pub struct KindCluster {
    id: u32,
}

impl KindCluster {
    fn get_random_cluster_id() -> u32 {
        rand::thread_rng().gen_range(0..99999)
    }

    pub async fn create() -> Result<Self> {
        let cluster = Self {
            id: Self::get_random_cluster_id(),
        };

        Command::new("kind")
            .args(["create", "cluster"])
            .args(["--name", cluster.id().to_string().as_str()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(cluster)
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub async fn delete(&self) -> Result<()> {
        Command::new("kind")
            .args(["delete", "cluster"])
            .args(["--name", self.id.to_string().as_str()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    pub async fn load_docker_image(&self, docker_image: &String) -> Result<()> {
        Command::new("kind")
            .args(["load", "docker-image", docker_image])
            .args(["--name", self.id.to_string().as_str()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }
}
