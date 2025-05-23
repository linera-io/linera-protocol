// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};

/// Counts before saving
const COUNT: u16 = 32;

pub(super) struct RollingFileLogger {
    size: u64,
    count: u16,
    metadata: Metadata,
    base_path: PathBuf,
    buffered_file: BufWriter<File>,
}

#[derive(Serialize, Deserialize, Default)]
struct Metadata {
    written: u64,
    current_index: u64,
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(self) {
            Ok(json) => write!(f, "{}", json),
            Err(_) => write!(
                f,
                "Metadata(written: {}, current_index: {})",
                self.written, self.current_index
            ),
        }
    }
}

impl RollingFileLogger {
    pub(super) async fn new(base_path: &Path, size: u64) -> anyhow::Result<Self> {
        let metadata_path = Self::get_metadata_path(base_path);
        let metadata = if metadata_path
            .try_exists()
            .with_context(|| "unable to verify the metadata path")?
        {
            let metadata_string = tokio::fs::read_to_string(metadata_path.as_path())
                .await
                .with_context(|| "unable to read the metadata")?;
            serde_json::from_str(&metadata_string).with_context(|| "corrupted metadata")?
        } else {
            let metadata = Metadata::default();
            let metadata_string = serde_json::to_string_pretty(&metadata)?;
            tokio::fs::write(metadata_path, metadata_string)
                .await
                .with_context(|| "unable to create a new metadata")?;
            metadata
        };

        let buffered_file = Self::get_current_log_file(base_path).await?;

        Ok(Self {
            size,
            metadata,
            count: 0,
            base_path: base_path.to_path_buf(),
            buffered_file,
        })
    }

    pub(super) async fn log(&mut self, content: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let bytes = content.as_ref();
        self.buffered_file.write_all(bytes).await?;
        self.buffered_file.write_all(b"\n").await?;
        self.update_metadata(Some(content.as_ref().len() as u64), false)
            .await?;

        if self.size < self.metadata.written {
            self.rotate().await?;
        }

        Ok(())
    }

    async fn rotate(&mut self) -> anyhow::Result<()> {
        self.buffered_file.flush().await?;
        let consolidated_log_file_name = self.get_next_log_path();
        let volatile_log_file_name = Self::get_current_log_path(self.base_path.as_path());
        tokio::fs::rename(volatile_log_file_name, consolidated_log_file_name).await?;
        self.update_metadata(None, true).await?;

        let fresh_file = Self::get_current_log_file(self.base_path.as_path()).await?;
        self.buffered_file = fresh_file;

        Ok(())
    }

    async fn update_metadata(
        &mut self,
        maybe_written: Option<u64>,
        maybe_current_index: bool,
    ) -> anyhow::Result<()> {
        let mut should_log = false;

        if let Some(some_written) = maybe_written {
            self.metadata.written += some_written;
            should_log = true;
            self.count += 1;
        }

        if maybe_current_index {
            self.metadata.current_index += 1;
            self.metadata.written = 0;
            should_log = true;
            self.count += 1;
        }

        if should_log {
            tracing::info!("updated metadata: {}", self.metadata);
        }

        if COUNT < self.count {
            self.persist_metadata().await?;
        }

        Ok(())
    }

    async fn persist_metadata(&self) -> anyhow::Result<()> {
        let metadata_path = Self::get_metadata_path(self.base_path.as_path());
        let jsoned = serde_json::to_string_pretty(&self.metadata)?;

        if let Err(err) = tokio::fs::write(metadata_path, &jsoned).await {
            tracing::error!("unable to persist the metadata: {jsoned}");
            Err(err)?
        };

        Ok(())
    }

    fn get_current_log_path(base_path: &Path) -> PathBuf {
        base_path.join("current.walrus_log")
    }

    fn get_metadata_path(base_path: &Path) -> PathBuf {
        base_path.join("metadata.json")
    }

    fn get_next_log_path(&self) -> PathBuf {
        self.base_path.join(format!(
            "{}_materialized.walrus_log",
            self.metadata.current_index
        ))
    }

    async fn get_current_log_file(base_path: &Path) -> anyhow::Result<BufWriter<File>> {
        let path = Self::get_current_log_path(base_path);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(BufWriter::new(file))
    }

    pub(super) async fn flush_logs(&mut self) -> anyhow::Result<()> {
        // in memory logs snapshot before flushing
        let buffer = self.buffered_file.buffer();
        let logs_snapshot =
            String::from_utf8(buffer.to_vec()).expect("there should not be invalid characters");

        if let Err(err) = self.buffered_file.flush().await {
            tracing::error!(
                "unable to flush walrus logs due to {}. \n {}",
                err,
                logs_snapshot
            );
            Err(err)?
        }

        self.persist_metadata().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::RollingFileLogger;
    use crate::runloops::walrus::logger::Metadata;

    #[test_log::test(tokio::test)]
    async fn test() -> anyhow::Result<()> {
        let base_path = tempdir()?;
        let size = 100;
        let mut logger = RollingFileLogger::new(base_path.path(), size).await?;
        for i in 0..=36 {
            logger.log(format!("test: {i}")).await?;
        }

        logger.flush_logs().await?;

        let bytes = tokio::fs::read(base_path.path().join("0_materialized.walrus_log")).await?;
        let string = String::from_utf8(bytes)?;

        for i in 0..=13 {
            assert!(string.contains(format!("test: {i}").as_str()))
        }

        let bytes = tokio::fs::read(base_path.path().join("1_materialized.walrus_log")).await?;
        let string = String::from_utf8(bytes)?;

        for i in 14..=26 {
            assert!(string.contains(format!("test: {i}").as_str()))
        }

        let bytes = tokio::fs::read(base_path.path().join("current.walrus_log")).await?;
        let string = String::from_utf8(bytes)?;

        for i in 27..=36 {
            assert!(string.contains(format!("test: {i}").as_str()))
        }

        let bytes = tokio::fs::read(base_path.path().join("metadata.json")).await?;
        let metadata = serde_json::from_slice::<Metadata>(&bytes)?;

        assert_eq!(metadata.current_index, 2);

        Ok(())
    }
}
