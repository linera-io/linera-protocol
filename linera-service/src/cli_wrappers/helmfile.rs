// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use fs_extra::dir::CopyOptions;
use linera_base::command::CommandExt;
use tokio::process::Command;

pub const DEFAULT_BLOCK_EXPORTER_PORT: u16 = 8882;

pub struct HelmFile;

impl HelmFile {
    #[expect(clippy::too_many_arguments)]
    pub async fn sync(
        server_config_id: usize,
        github_root: &Path,
        num_proxies: usize,
        num_shards: usize,
        cluster_id: u32,
        docker_image_name: String,
        with_block_exporter: bool,
        num_block_exporters: usize,
        indexer_image_name: String,
        explorer_image_name: String,
        dual_store: bool,
    ) -> Result<()> {
        let chart_dir = format!("{}/kubernetes/linera-validator", github_root.display());

        let temp_dir = tempfile::tempdir()?;
        fs_extra::copy_items(&[&chart_dir], temp_dir.path(), &CopyOptions::new())?;

        let mut command = Command::new("helmfile");
        command.current_dir(temp_dir.path().join("linera-validator"));

        if dual_store {
            command.env(
                "LINERA_HELMFILE_SET_STORAGE",
                "dualrocksdbscylladb:/linera.db:spawn_blocking:tcp:scylla-client.scylla.svc.cluster.local:9042",
            );
            command.env("LINERA_HELMFILE_SET_DUAL_STORE", "true");
        }

        if with_block_exporter {
            command.env("LINERA_HELMFILE_SET_EXPLORER_ENABLED", "true");
            command.env(
                "LINERA_HELMFILE_NUM_BLOCK_EXPORTERS",
                num_block_exporters.to_string(),
            );
            command.env(
                "LINERA_HELMFILE_BLOCK_EXPORTER_PORT",
                DEFAULT_BLOCK_EXPORTER_PORT.to_string(),
            );
            command.env("LINERA_HELMFILE_INDEXER_IMAGE", indexer_image_name);
            command.env("LINERA_HELMFILE_EXPLORER_IMAGE", explorer_image_name);
        }

        command
            .env(
                "LINERA_HELMFILE_SET_SERVER_CONFIG",
                format!("working/server_{server_config_id}.json"),
            )
            .env("LINERA_HELMFILE_SET_NUM_PROXIES", num_proxies.to_string())
            .env("LINERA_HELMFILE_SET_NUM_SHARDS", num_shards.to_string())
            .env("LINERA_HELMFILE_LINERA_IMAGE", docker_image_name)
            .env(
                "LINERA_HELMFILE_SET_KUBE_CONTEXT",
                format!("kind-{}", cluster_id),
            )
            .arg("sync")
            .arg("--wait")
            .spawn_and_wait()
            .await
    }
}
