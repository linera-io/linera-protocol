// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use fs_extra::dir::CopyOptions;
use linera_base::command::CommandExt;
use tokio::process::Command;

pub struct HelmFile;

impl HelmFile {
    pub async fn sync(
        server_config_id: usize,
        github_root: &Path,
        num_shards: usize,
        cluster_id: u32,
    ) -> Result<()> {
        let chart_dir = format!("{}/kubernetes/linera-validator", github_root.display());

        let temp_dir = tempfile::tempdir()?;
        fs_extra::copy_items(&[&chart_dir], temp_dir.path(), &CopyOptions::new())?;

        Command::new("helmfile")
            .current_dir(temp_dir.path().join("linera-validator"))
            .env(
                "LINERA_HELMFILE_SET_SERVER_CONFIG",
                format!("working/server_{server_config_id}.json"),
            )
            .env("LINERA_HELMFILE_SET_NUM_SHARDS", num_shards.to_string())
            .arg("sync")
            .arg("--wait")
            .args(["--kube-context", &format!("kind-{}", cluster_id)])
            .spawn_and_wait()
            .await
    }
}
