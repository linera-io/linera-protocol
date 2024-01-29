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

    // TODO(#1573): Rewrite this.
    println!("cargo:rerun-if-changed=../linera-sdk/contract.wit");
    println!("cargo:rerun-if-changed=../linera-sdk/contract_system_api.wit");
    println!("cargo:rerun-if-changed=../linera-sdk/service.wit");
    println!("cargo:rerun-if-changed=../linera-sdk/service_system_api.wit");
    println!("cargo:rerun-if-changed=../linera-sdk/view_system_api.wit");
    println!("cargo:rerun-if-changed=../linera-sdk/mock_system_api.wit");

    // println!("cargo:rerun-if-changed=../linera-rpc/proto/rpc.proto");
    println!("cargo:rerun-if-changed=../linera-rpc/tests/staged/formats.yaml");

    println!(
        "cargo:rerun-if-changed=../linera-service-graphql-client/gql/service_requests.graphql"
    );
    println!("cargo:rerun-if-changed=../linera-service-graphql-client/gql/service_schema.graphql");
}
