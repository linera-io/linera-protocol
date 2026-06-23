// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Extracts the formats of an application from a SNAP file and writes them as a
//! BCS-encoded file ready to be published as a data blob with
//! `linera publish-data-blob`.
//!
//! The SNAP file is the YAML snapshot produced by the `format` test of the example
//! applications (e.g. `examples/<app>/tests/snapshots/format__format.snap`). The
//! output is the exact byte payload that `linera publish-module-with-formats` binds
//! in the formats registry, so the blob published from it has the hash the registry
//! expects.
//!
//! Usage:
//!
//! ```bash
//! extract-formats <SNAP_FILE> <OUTPUT_BLOB_FILE>
//! ```

#![cfg_attr(target_arch = "wasm32", no_main)]

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

#[cfg(not(target_arch = "wasm32"))]
use anyhow::{bail, Context as _};
#[cfg(not(target_arch = "wasm32"))]
use linera_client::client_context::read_formats_from_snap;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let (Some(input), Some(output), None) = (args.next(), args.next(), args.next()) else {
        bail!("usage: extract-formats <SNAP_FILE> <OUTPUT_BLOB_FILE>");
    };
    let input = PathBuf::from(input);
    let output = PathBuf::from(output);

    let formats = read_formats_from_snap(&input)
        .with_context(|| format!("failed to read formats from {input:?}"))?;
    // BCS-serialize the formats the same way the client does when publishing, so the
    // data blob produced from this file matches the hash bound in the registry write.
    let blob = bcs::to_bytes(&formats).context("failed to serialize formats as BCS")?;
    std::fs::write(&output, &blob)
        .with_context(|| format!("failed to write blob file {output:?}"))?;

    println!(
        "Wrote {} bytes of formats from {input:?} to {output:?}; \
         publish it with `linera publish-data-blob {}`",
        blob.len(),
        output.display(),
    );
    Ok(())
}
