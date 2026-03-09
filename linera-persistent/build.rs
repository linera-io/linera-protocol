// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { target_os = "web" },
        with_indexed_db: { all(web, feature = "indexed-db") },
    };
}
