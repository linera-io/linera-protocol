// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use rand::Rng;
use std::process::Command;

#[derive(Clone)]
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

        let status = Command::new("kind")
            .args(["create", "cluster"])
            .args(["--name", cluster.id().to_string().as_str()])
            .status()
            .expect("Creating cluster should not fail");

        if !status.success() {
            return Err(anyhow::anyhow!("Error creating cluster: {}", cluster.id));
        }

        Ok(cluster)
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub async fn delete(&self) -> Result<()> {
        let status = Command::new("kind")
            .args(["delete", "cluster"])
            .args(["--name", &self.id.to_string()])
            .status()
            .expect("Deleting cluster should not fail");

        if !status.success() {
            println!("Error in deleting cluster {}", self.id);
            return Err(anyhow::anyhow!("Error deleting cluster: {}", self.id));
        }

        println!("Deleted cluster successfully {}", self.id);
        Ok(())
    }

    pub async fn load_docker_image(&self, docker_image: &String) -> Result<()> {
        let status = Command::new("kind")
            .args(["load", "docker-image", docker_image])
            .args(["--name", self.id.to_string().as_str()])
            .status()
            .expect("Loading docker image should not fail");

        if !status.success() {
            return Err(anyhow::anyhow!(
                "Error loading docker image {} into cluster {}",
                docker_image,
                self.id
            ));
        }

        Ok(())
    }
}
