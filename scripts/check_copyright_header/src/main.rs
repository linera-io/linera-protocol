use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
};
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum CheckFileHeaderError {
    #[error("Unexpected line reached")]
    UnexpectedLineReachedError,

    #[error("Incorrect copyright header, Zefchain Labs header not found")]
    ZefchainLabsHeaderNotFoundError,

    #[error("Separation line not found")]
    SeparationLineNotFoundError,

    #[error("Incorrect copyright header")]
    IncorrectCopyrightHeaderError,
}

fn check_file_header(
    lines: impl IntoIterator<Item = Result<String, std::io::Error>>,
) -> Result<(), CheckFileHeaderError> {
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

            return Err(CheckFileHeaderError::UnexpectedLineReachedError);
        } else {
            if !found_zefchain_labs_header {
                return Err(CheckFileHeaderError::ZefchainLabsHeaderNotFoundError);
            }

            if line.is_empty() {
                // Found separation line
                return Ok(());
            } else {
                return Err(CheckFileHeaderError::SeparationLineNotFoundError);
            }
        }
    }

    Err(CheckFileHeaderError::IncorrectCopyrightHeaderError)
}

fn main() -> std::process::ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();

    let mut exit_code = std::process::ExitCode::SUCCESS;

    for file_path in args {
        let file = File::open(&file_path).expect("Failed to open file");
        let reader = BufReader::new(file);

        if let Err(e) = check_file_header(reader.lines()) {
            exit_code = std::process::ExitCode::FAILURE;
            eprintln!("{}: {}", file_path, e);
        }
    }

    exit_code
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
            check_file_header(lines).unwrap_err(),
            CheckFileHeaderError::ZefchainLabsHeaderNotFoundError,
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
            check_file_header(lines).unwrap_err(),
            CheckFileHeaderError::ZefchainLabsHeaderNotFoundError,
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
            check_file_header(lines).unwrap_err(),
            CheckFileHeaderError::SeparationLineNotFoundError,
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
            check_file_header(lines).unwrap_err(),
            CheckFileHeaderError::UnexpectedLineReachedError,
        );
    }
}
