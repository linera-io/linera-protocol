// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A [`Persist`] backend that atomically saves the value to a locked file on disk.

use std::{
    io::{self, BufRead as _, Write as _},
    path::Path,
};

use fs4::FileExt;

use super::Persist;

/// A guard that keeps an exclusive lock on a file.
struct Lock(fs_err::File);

/// The kinds of error that persisting a value to a file can produce.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An I/O operation on the file failed.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    /// The value could not be serialized to, or deserialized from, JSON.
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    /// The file could not be locked for exclusive access.
    #[error("failed to lock {}: {source}", path.display())]
    Lock {
        /// The path that could not be locked.
        path: std::path::PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// An operation failed, and so did the cleanup that followed it.
    #[error("failed to clean up after an error: {cleanup}; the original error was: {original}")]
    Cleanup {
        /// The failure of the cleanup step itself.
        #[source]
        cleanup: Box<Error>,
        /// The failure that prompted the cleanup.
        original: Box<Error>,
    },
}

/// Utility: run a fallible cleanup function if an operation failed, reporting both
/// failures if the cleanup fails too.
trait CleanupExt {
    /// The success type of the operation.
    type Ok;

    /// Runs `cleanup` if the operation failed.
    fn or_cleanup<E: Into<Error>>(
        self,
        cleanup: impl FnOnce() -> Result<(), E>,
    ) -> Result<Self::Ok, Error>;
}

impl<T> CleanupExt for Result<T, Error> {
    type Ok = T;

    fn or_cleanup<E: Into<Error>>(
        self,
        cleanup: impl FnOnce() -> Result<(), E>,
    ) -> Result<T, Error> {
        self.map_err(|original| match cleanup() {
            Ok(()) => original,
            Err(cleanup) => Error::Cleanup {
                cleanup: Box::new(cleanup.into()),
                original: Box::new(original),
            },
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
        if let Err(error) = FileExt::unlock(self.0.file()) {
            tracing::warn!("Failed to unlock wallet file: {error}");
        }
    }
}

/// An implementation of [`Persist`] based on an atomically-updated file at a given path.
/// An exclusive lock is taken using `flock(2)` to ensure that concurrent updates cannot
/// happen, and writes are saved to a staging file before being moved over the old file,
/// an operation that is atomic on all Unixes.
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

impl<T> std::ops::DerefMut for File<T> {
    fn deref_mut(&mut self) -> &mut T {
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

impl<T: serde::Serialize + serde::de::DeserializeOwned> File<T> {
    /// Creates a new persistent file at `path` containing `value`.
    pub fn new(path: &Path, value: T) -> Result<Self, Error> {
        let this = Self {
            _lock: Lock::new(
                fs_err::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?,
            )
            .map_err(|source| Error::Lock {
                path: path.into(),
                source,
            })?,
            path: path.into(),
            value,
        };
        this.save()?;
        Ok(this)
    }

    /// Reads the value from a file at `path`, returning an error if it does not exist.
    pub fn read(path: &Path) -> Result<Self, Error> {
        Self::read_or_create(path, || {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file is empty or does not exist: {}", path.display()),
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
        let file_is_empty = reader.fill_buf()?.is_empty();

        let me = Self {
            value: if file_is_empty {
                value()?
            } else {
                serde_json::from_reader(reader)?
            },
            path: path.into(),
            _lock: lock,
        };

        me.save()?;

        Ok(me)
    }

    /// Atomically writes the current value to the file, via a temporary staging file.
    pub fn save(&self) -> Result<(), Error> {
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
        drop(temp_file_writer);
        fs_err::rename(&temp_file_path, &self.path)?;
        Ok(())
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
    fn persist(&mut self) -> impl std::future::Future<Output = Result<(), Error>> {
        let result = self.save();
        async { result }
    }

    /// Takes the value out, releasing the lock on the persistent file.
    fn into_value(self) -> T {
        self.value
    }
}
