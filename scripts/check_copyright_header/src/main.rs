use anyhow::{bail, Error, Result};
use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
};

fn check_file_header(
    lines: impl IntoIterator<Item = Result<String, std::io::Error>>,
) -> Result<(), Error> {
    let mut found_zefchain_labs_header = false;
    let mut is_end_of_header = false;

    for line in lines {
        let line = line.expect("Failed to read line");
        if !is_end_of_header {
            if line == "// Copyright (c) Zefchain Labs, Inc." {
                found_zefchain_labs_header = true;
                continue;
            }

            if line.starts_with("// Copyright (c)") {
                continue;
            }

            if line == "// SPDX-License-Identifier: Apache-2.0" {
                is_end_of_header = true;
                continue;
            }

            bail!("Unexpected line reached");
        } else {
            if !found_zefchain_labs_header {
                bail!("Incorrect copyright header, Zefchain Labs header not found");
            }

            if line.is_empty() {
                // Found separation line
                return Ok(());
            } else {
                bail!("Separation line not found");
            }
        }
    }

    bail!("Incorrect copyright header")
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let file_path = args.get(1).expect("Usage: FILE");

    let file = File::open(file_path).expect("Failed to open file");
    let reader = BufReader::new(file);

    check_file_header(reader.lines())
}
