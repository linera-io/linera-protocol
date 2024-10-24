// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::{self, Write};

use crate::ensure;

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
        let remaining = self.limit.checked_sub(self.written).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Data exceeds the allowed limit")
        })?;
        // Ensure the new data still fits.
        ensure!(
            buf.len() <= remaining,
            io::Error::new(io::ErrorKind::Other, "Data exceeds the allowed limit",)
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
