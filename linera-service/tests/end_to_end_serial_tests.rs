// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};
mod common;
use common::INTEGRATION_TEST_GUARD;

#[test_log::test(tokio::test)]
async fn test_linera_net_up_simple() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let mut command = Command::new(env!("CARGO_BIN_EXE_linera"));
    command.args(["net", "up"]);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let stderr = BufReader::new(child.stderr.take().unwrap());

    for line in stderr.lines() {
        let line = line.unwrap();
        if line.starts_with("READY!") {
            let mut exports = stdout.lines();
            assert!(exports
                .next()
                .unwrap()
                .unwrap()
                .starts_with("export LINERA_WALLET="));
            assert!(exports
                .next()
                .unwrap()
                .unwrap()
                .starts_with("export LINERA_STORAGE="));
            assert_eq!(exports.next().unwrap().unwrap(), "");

            // Send SIGINT to the child process.
            Command::new("kill")
                .args(["-s", "INT", &child.id().to_string()])
                .output()
                .unwrap();

            assert!(exports.next().is_none());
            assert!(child.wait().unwrap().success());
            return;
        }
    }
    panic!("Unexpected EOF for stderr");
}
