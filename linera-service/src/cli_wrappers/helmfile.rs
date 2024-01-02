// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util::CommandExt;
use anyhow::{Context, Result};
use pathdiff::diff_paths;
use std::path::Path;
use tokio::process::Command;

pub struct HelmFile;

impl HelmFile {
    pub async fn sync(
        config_dir: &Path,
        server_config_id: usize,
        github_root: &Path,
        num_shards: usize,
        cluster_id: u32,
    ) -> Result<()> {
        let execution_dir = format!("{}/kubernetes/linera-validator", github_root.display());
        let config_dir = diff_paths(config_dir, execution_dir.clone())
            .context("failed to get relative path")?
            .to_str()
            .context("failed to convert relative path to string")?
            .to_string();

        Command::new("helmfile")
            .current_dir(&execution_dir)
            .env(
                "LINERA_HELMFILE_SET_SERVER_CONFIG",
                &format!("{config_dir}/server_{server_config_id}.json"),
            )
            .env(
                "LINERA_HELMFILE_SET_GENESIS_CONFIG",
                &format!("{config_dir}/genesis.json"),
            )
            .env("LINERA_HELMFILE_SET_NUM_SHARDS", num_shards.to_string())
            .arg("sync")
            .arg("--wait")
            .args(["--kube-context", &format!("kind-{}", cluster_id)])
            .spawn_and_wait()
            .await
    }
}
