// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tokio::process::Command;

use crate::util::CommandExt;

pub struct KindWrapper;

impl KindWrapper {
    pub async fn delete() -> Result<String, anyhow::Error> {
        Ok(Command::new("kind")
            .args(["delete", "cluster"])
            .spawn_and_wait_for_stdout()
            .await?)
    }

    pub async fn create() -> Result<String, anyhow::Error> {
        Ok(Command::new("kind")
            .args(["create", "cluster"])
            .spawn_and_wait_for_stdout()
            .await?)
    }

    pub async fn load_docker_image(docker_image: &str) -> Result<String, anyhow::Error> {
        Ok(Command::new("kind")
            .args(["load", "docker-image", docker_image])
            .spawn_and_wait_for_stdout()
            .await?)
    }
}
