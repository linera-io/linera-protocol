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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_file_with_header() {
        let lines = vec![
            "// Copyright (c) Zefchain Labs, Inc.",
            "// SPDX-License-Identifier: Apache-2.0",
            "",
            "// Rest of the file...",
        ]
        .into_iter()
        .map(String::from)
        .map(Result::Ok);

        assert!(check_file_header(lines).is_ok());
    }

    #[test]
    fn test_valid_file_with_multiple_headers() {
        let lines = vec![
            "// Copyright (c) Zefchain Labs, Inc.",
            "// Copyright (c) Some Other Company",
            "// SPDX-License-Identifier: Apache-2.0",
            "",
            "// Rest of the file...",
        ]
        .into_iter()
        .map(String::from)
        .map(Result::Ok);

        assert!(check_file_header(lines).is_ok());
    }

    #[test]
    fn test_invalid_file_missing_zefchain_header() {
        let lines = vec![
            "// SPDX-License-Identifier: Apache-2.0",
            "",
            "// Rest of the file...",
        ]
        .into_iter()
        .map(String::from)
        .map(Result::Ok);

        assert_eq!(
            check_file_header(lines)
                .map_err(|err| err.to_string())
                .unwrap_err(),
            "Incorrect copyright header, Zefchain Labs header not found"
        );
    }

    #[test]
    fn test_invalid_file_incorrect_zefchain_header() {
        let lines = vec![
            "// Copyright (c) Some Other Company",
            "// SPDX-License-Identifier: Apache-2.0",
            "",
            "// Rest of the file...",
        ]
        .into_iter()
        .map(String::from)
        .map(Result::Ok);

        assert_eq!(
            check_file_header(lines)
                .map_err(|err| err.to_string())
                .unwrap_err(),
            "Incorrect copyright header, Zefchain Labs header not found"
        );
    }

    #[test]
    fn test_invalid_file_unexpected_line() {
        let lines = vec![
            "// Copyright (c) Zefchain Labs, Inc.",
            "// SPDX-License-Identifier: Apache-2.0",
            "Unexpected line",
            "",
            "// Rest of the file...",
        ]
        .into_iter()
        .map(String::from)
        .map(Result::Ok);

        assert_eq!(
            check_file_header(lines)
                .map_err(|err| err.to_string())
                .unwrap_err(),
            "Separation line not found"
        );
    }

    #[test]
    fn test_invalid_file_empty_line_before_header() {
        let lines = vec![
            "",
            "// SPDX-License-Identifier: Apache-2.0",
            "",
            "// Rest of the file...",
        ]
        .into_iter()
        .map(String::from)
        .map(Result::Ok);

        assert_eq!(
            check_file_header(lines)
                .map_err(|err| err.to_string())
                .unwrap_err(),
            "Unexpected line reached"
        );
    }
}
