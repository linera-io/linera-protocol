// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::ParseIntError, time::Duration};

pub fn parse_millis(s: &str) -> Result<Duration, ParseIntError> {
    let millis = s.parse::<u64>()?;
    Ok(Duration::from_millis(millis))
}
