// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Decode a `RootKey` partition key blob (as stored by any backend) into its variant.
//!
//! Reads hex-encoded keys from positional arguments or from stdin (one per line).
//! Tolerates a leading `0x` prefix and skips lines that do not look like hex (so
//! the raw output of `cqlsh` table dumps can be piped in directly).

use std::io::{self, BufRead, Write};

use anyhow::{anyhow, Result};
use clap::Parser;
use linera_storage::RootKey;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Strip this many leading bytes from each blob before decoding. Use this when the stored
    /// partition key is wrapped (e.g. backends may prepend a single tag byte).
    #[arg(long, default_value_t = 0, conflicts_with = "scylla")]
    strip_bytes: usize,

    /// Shorthand for `--strip-bytes 1`: the ScyllaDB backend prepends a single 0x00 byte
    /// to every root key (see `get_big_root_key` in `linera-views::backends::scylla_db`).
    #[arg(long)]
    scylla: bool,

    /// Hex-encoded root key blobs. If omitted, read one per line from stdin.
    keys: Vec<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let strip = if args.scylla { 1 } else { args.strip_bytes };
    let stdout = io::stdout();
    let mut out = stdout.lock();
    if args.keys.is_empty() {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            process_line(&mut out, &line?, strip)?;
        }
    } else {
        for key in &args.keys {
            process_line(&mut out, key, strip)?;
        }
    }
    Ok(())
}

fn process_line(out: &mut impl Write, line: &str, strip: usize) -> io::Result<()> {
    for token in line.split(|c: char| c.is_whitespace() || c == '|' || c == ',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = trimmed.strip_prefix("0x").unwrap_or(trimmed);
        if candidate.len() % 2 != 0 || !candidate.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        let bytes = match hex::decode(candidate) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let payload = if strip <= bytes.len() {
            &bytes[strip..]
        } else {
            writeln!(
                out,
                "{trimmed}\tINVALID: shorter than --strip-bytes={strip}"
            )?;
            continue;
        };
        match decode(payload) {
            Ok(rendered) => writeln!(out, "{trimmed}\t{rendered}")?,
            Err(error) => writeln!(out, "{trimmed}\tINVALID: {error}")?,
        }
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() {
        return Err(anyhow!("empty"));
    }
    let key: RootKey = bcs::from_bytes(bytes).map_err(|e| anyhow!("bcs: {e}"))?;
    Ok(format!("{key:?}"))
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::CryptoHash,
        identifiers::{BlobId, BlobType, ChainId},
    };
    use linera_storage::RootKey;

    use super::decode;

    fn roundtrip(key: RootKey) -> String {
        decode(&key.bytes()).expect("decode")
    }

    #[test]
    fn decodes_network_description() {
        assert_eq!(roundtrip(RootKey::NetworkDescription), "NetworkDescription");
    }

    #[test]
    fn decodes_block_exporter_state() {
        let rendered = roundtrip(RootKey::BlockExporterState(7));
        assert!(rendered.starts_with("BlockExporterState("), "{rendered}");
        assert!(rendered.contains('7'), "{rendered}");
    }

    #[test]
    fn decodes_chain_state() {
        let chain_id = ChainId(CryptoHash::test_hash("conway"));
        let rendered = roundtrip(RootKey::ChainState(chain_id));
        assert!(rendered.starts_with("ChainState("), "{rendered}");
    }

    #[test]
    fn decodes_blob() {
        let blob_id = BlobId::new(CryptoHash::test_hash("blob"), BlobType::Data);
        let rendered = roundtrip(RootKey::Blob(blob_id));
        assert!(rendered.starts_with("Blob("), "{rendered}");
    }

    #[test]
    fn rejects_empty() {
        assert!(decode(&[]).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode(&[0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn scylla_prefix_is_one_zero_byte() {
        let key = RootKey::NetworkDescription;
        let mut wrapped = vec![0u8];
        wrapped.extend(key.bytes());
        let stripped = &wrapped[1..];
        assert_eq!(decode(stripped).unwrap(), "NetworkDescription");
    }
}
