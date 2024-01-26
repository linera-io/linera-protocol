// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::BufRead as _;

fn main() {
    let mut versions = std::process::Command::new("bash")
        .arg("versions.sh")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("failed to launch child");

    for line in
        std::io::BufReader::new(versions.stdout.take().expect("child has no stdout")).lines()
    {
        println!(
            "cargo:rustc-env=LINERA_VERSION_{}",
            line.expect("failed to read line")
        );
    }
}
