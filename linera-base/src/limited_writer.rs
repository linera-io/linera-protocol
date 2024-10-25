// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::{self, Write};

use thiserror::Error;

use crate::ensure;

#[derive(Error, Debug)]
#[error("Writer limit exceeded")]
pub struct LimitedWriterError;

/// Custom writer that enforces a byte limit.
pub struct LimitedWriter<W: Write> {
    inner: W,
    limit: usize,
    written: usize,
}

impl<W: Write> LimitedWriter<W> {
    pub fn new(inner: W, limit: usize) -> Self {
        Self {
            inner,
            limit,
            written: 0,
        }
    }
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Calculate the number of bytes we can write without exceeding the limit.
        // Fail if the buffer doesn't fit.
        ensure!(
            self.limit
                .checked_sub(self.written)
                .is_some_and(|remaining| buf.len() <= remaining),
            io::Error::other(LimitedWriterError)
        );
        // Forward to the inner writer.
        let n = self.inner.write(buf)?;
        self.written += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limited_writer() {
        let mut out_buffer = Vec::new();
        let mut writer = LimitedWriter::new(&mut out_buffer, 5);
        assert_eq!(writer.write(b"foo").unwrap(), 3);
        assert_eq!(writer.write(b"ba").unwrap(), 2);
        assert!(writer
            .write(b"r")
            .unwrap_err()
            .downcast::<LimitedWriterError>()
            .is_ok());
    }
}
