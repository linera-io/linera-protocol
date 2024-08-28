// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{self, BufRead as _, Write as _},
    path::Path,
};

use fs4::FileExt as _;
use thiserror_context::Context;

use super::{Dirty, Persist};

/// A guard that keeps an exclusive lock on a file.
struct Lock(fs_err::File);

#[derive(Debug, thiserror::Error)]
enum ErrorInner {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Serde(#[from] serde_json::Error),
}

thiserror_context::impl_context!(Error(ErrorInner));

/// Utility: run a fallible cleanup function if an operation failed, attaching the
/// original operation as context to its error.
trait CleanupExt {
    type Ok;
    type Error;

    fn or_cleanup<E>(self, f: impl FnOnce() -> Result<(), E>) -> Result<Self::Ok, Self::Error>
    where
        E: Into<Self::Error>,
        Result<(), E>: Context<Self::Error, Self::Ok, E>;
}

impl<T, W> CleanupExt for Result<T, W>
where
    W: std::fmt::Display + Send + Sync + 'static,
{
    type Ok = T;
    type Error = W;

    fn or_cleanup<E>(self, cleanup: impl FnOnce() -> Result<(), E>) -> Self
    where
        E: Into<W>,
        Result<(), E>: Context<W, T, E>,
    {
        self.or_else(|error| {
            if let Err(cleanup_error) = cleanup() {
                Err(cleanup_error).context(error)
            } else {
                Err(error)
            }
        })
    }
}

impl Lock {
    /// Acquires an exclusive lock on a provided `file`, returning a [`Lock`] which will
    /// release the lock when dropped.
    pub fn new(file: fs_err::File) -> std::io::Result<Self> {
        file.file().try_lock_exclusive()?;
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
    dirty: Dirty,
}

impl<T> std::ops::Deref for File<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for File<T> {
    fn deref_mut(&mut self) -> &mut T {
        *self.dirty = true;
        &mut self.value
    }
}

/// Returns options for opening and writing to the file, creating it if it doesn't
/// exist. On Unix, this restricts read and write permissions to the current user.
// TODO(#1924): Implement better key management.
// BUG(#2053): Use a separate lock file per staging file.
fn open_options() -> fs_err::OpenOptions {
    let mut options = fs_err::OpenOptions::new();
    #[cfg(target_family = "unix")]
    fs_err::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.create(true).read(true).write(true);
    options
}

impl<T: serde::de::DeserializeOwned> File<T> {
    /// Creates a new persistent file at `path` containing `value`.
    pub fn new(path: &Path, value: T) -> Result<Self, Error> {
        Ok(Self {
            _lock: Lock::new(
                fs_err::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?,
            )
            .with_context(|| format!("locking path {}", path.display()))?,
            path: path.into(),
            value,
            dirty: Dirty::new(true),
        })
    }

    /// Reads the value from a file at `path`, returning an error if it does not exist.
    pub fn read(path: &Path) -> Result<Self, Error> {
        Self::read_or_create(path, || {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("path does not exist: {}", path.display()),
            )
            .into())
        })
    }

    /// Reads the value from a file at `path`, calling the `value` function to create it
    /// if it does not exist. If it does exist, `value` will not be called.
    pub fn read_or_create(
        path: &Path,
        value: impl FnOnce() -> Result<T, Error>,
    ) -> Result<Self, Error> {
        let lock = Lock::new(open_options().read(true).open(path)?)?;
        let mut reader = io::BufReader::new(&lock.0);
        let dirty;

        let value = if reader.fill_buf()?.is_empty() {
            dirty = Dirty::new(true);
            value()?
        } else {
            dirty = Dirty::new(false);
            serde_json::from_reader(reader)?
        };

        Ok(Self {
            value,
            path: path.into(),
            dirty,
            _lock: lock,
        })
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned + Send> Persist for File<T> {
    type Error = Error;

    fn as_mut(&mut self) -> &mut T {
        &mut self.value
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
    async fn persist(&mut self) -> Result<(), Error> {
        let mut temp_file_path = self.path.clone();
        temp_file_path.set_extension("json.new");
        let temp_file = open_options().open(&temp_file_path)?;
        let mut temp_file_writer = std::io::BufWriter::new(temp_file);

        let remove_temp_file = || fs_err::remove_file(&temp_file_path);

        serde_json::to_writer_pretty(&mut temp_file_writer, &self.value)
            .map_err(Error::from)
            .or_cleanup(remove_temp_file)?;
        temp_file_writer
            .flush()
            .map_err(Error::from)
            .or_cleanup(remove_temp_file)?;
        fs_err::rename(&temp_file_path, &self.path)?;
        *self.dirty = false;
        Ok(())
    }

    /// Takes the value out, releasing the lock on the persistent file.
    fn into_value(self) -> T {
        self.value
    }
}
