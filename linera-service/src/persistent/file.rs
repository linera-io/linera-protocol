// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{self, BufRead as _, Write as _},
    path::Path,
};

use anyhow::Context as _;
use fs4::FileExt as _;

use super::Persist;

/// A guard that keeps an exclusive lock on a file.
pub struct Lock(fs_err::File);

impl Lock {
    /// Acquires an exclusive lock on a provided `file`, returning a [`Lock`] which will
    /// release the lock when dropped.
    pub fn new(file: fs_err::File, path: &Path) -> anyhow::Result<Self> {
        file.file().try_lock_exclusive().with_context(|| {
            format!(
                "Error getting write lock \"{}\". Please make sure the file exists \
                 and that it is not in use by another process already.",
                path.display()
            )
        })?;

        Ok(Lock(file))
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        if let Err(error) = self.0.file().unlock() {
            tracing::warn!("Failed to unlock wallet file: {error}");
        }
    }
}

/// An implementation of [`Persist`] based on an atomically-updated file at a given path.
/// An exclusive lock is taken using `flock(2)` to ensure that concurrent updates cannot
/// happen, and writes are saved to a staging file before being moved over the old file,
/// an operation that is atomic on all UNIXes.
pub struct File<T> {
    _lock: Lock,
    path: std::path::PathBuf,
    value: T,
}

impl<T> std::ops::Deref for File<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

/// Returns options for opening and writing to the file, creating it if it doesn't
/// exist. On Unix, this restricts read and write permissions to the current user.
// TODO(#1924): Implement better key management.
fn open_options() -> fs_err::OpenOptions {
    let mut options = fs_err::OpenOptions::new();
    #[cfg(target_family = "unix")]
    fs_err::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.create(true).read(true).write(true);
    options
}

impl<T: serde::de::DeserializeOwned> File<T> {
    /// Create a new persistent file at `path` containing `value`.
    pub fn new(path: &Path, value: T) -> anyhow::Result<Self> {
        Ok(Self {
            _lock: Lock::new(
                fs_err::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?,
                path,
            )?,
            path: path.into(),
            value,
        })
    }

    /// Read the value from a file at `path`, returning an error if it does not exist.
    pub fn read(path: &Path) -> anyhow::Result<Self> {
        Self::read_or_create(path, || {
            Err(anyhow::anyhow!("Path does not exist: {}", path.display()))
        })
    }

    /// Read the value from a file at `path`, calling the `value` function to create it if
    /// it does not exist.  If it does exist, `value` will not be called.
    pub fn read_or_create(
        path: &Path,
        value: impl FnOnce() -> anyhow::Result<T>,
    ) -> anyhow::Result<Self> {
        let lock = Lock::new(open_options().read(true).open(path)?, path)?;
        let mut reader = io::BufReader::new(&lock.0);

        Ok(Self {
            value: if reader.fill_buf()?.is_empty() {
                value()?
            } else {
                serde_json::from_reader(reader)?
            },
            path: path.into(),
            _lock: lock,
        })
    }

    /// Take the value out, releasing the lock on the persistent file.
    pub fn into_value(self) -> T {
        self.value
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> Persist for File<T> {
    type Error = anyhow::Error;

    fn as_mut(this: &mut Self) -> &mut T {
        &mut this.value
    }

    /// Writes the value to disk.
    ///
    /// The contents of the file need to be over-written completely, so
    /// a temporary file is created as a backup in case a crash occurs while
    /// writing to disk.
    ///
    /// The temporary file is then renamed to the original filename. If
    /// serialization or writing to disk fails, the temporary file is
    /// deleted.
    fn persist(this: &mut Self) -> anyhow::Result<()> {
        let mut temp_file_path = this.path.clone();
        temp_file_path.set_extension("json.new");
        let temp_file = open_options().open(&temp_file_path)?;
        let mut temp_file_writer = std::io::BufWriter::new(temp_file);

        if let Err(e) = serde_json::to_writer_pretty(&mut temp_file_writer, &this.value) {
            fs_err::remove_file(&temp_file_path).context("handling writing error {e}")?;
            anyhow::bail!("failed to serialize the wallet state: {e}")
        }
        if let Err(e) = temp_file_writer.flush() {
            fs_err::remove_file(&temp_file_path).context("handling flushing error {e}")?;
            anyhow::bail!("failed to write the wallet state: {e}");
        }
        fs_err::rename(&temp_file_path, &this.path)?;
        Ok(())
    }
}
