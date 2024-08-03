// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_version::VersionInfo;

fn main() -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout(), &VersionInfo::get()?.api_hashes())?;

    Ok(())
}
