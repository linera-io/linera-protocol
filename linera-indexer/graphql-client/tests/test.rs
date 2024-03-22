// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::Read;

use linera_base::command::resolve_binary;
use tempfile::tempdir;
use tokio::process::Command;

#[test_log::test(tokio::test)]
async fn test_check_indexer_schema() {
    let tmp_dir = tempdir().unwrap();
    let path = resolve_binary("linera-indexer", "linera-indexer-example")
        .await
        .unwrap();
    let mut command = Command::new(path);
    let output = command
        .current_dir(tmp_dir.path())
        .arg("schema")
        .output()
        .await
        .unwrap();
    let indexer_schema = String::from_utf8(output.stdout).unwrap();
    let mut file_base = std::fs::File::open("gql/indexer_schema.graphql").unwrap();
    let mut graphql_schema = String::new();
    file_base.read_to_string(&mut graphql_schema).unwrap();
    assert_eq!(
        graphql_schema, indexer_schema,
        "\nGraphQL indexer schema has changed -> \
         regenerate schema following steps in linera-indexer-graphql-client/README.md\n"
    )
}

#[test_log::test(tokio::test)]
async fn test_check_indexer_operations_schema() {
    let tmp_dir = tempdir().unwrap();
    let path = resolve_binary("linera-indexer", "linera-indexer-example")
        .await
        .unwrap();
    let mut command = Command::new(path);
    let output = command
        .current_dir(tmp_dir.path())
        .args(["schema", "operations"])
        .output()
        .await
        .unwrap();
    let service_schema = String::from_utf8(output.stdout).unwrap();
    let mut file_base = std::fs::File::open("gql/operations_schema.graphql").unwrap();
    let mut graphql_schema = String::new();
    file_base.read_to_string(&mut graphql_schema).unwrap();
    assert_eq!(
        graphql_schema, service_schema,
        "\nGraphQL indexer operations schema has changed -> \
         regenerate schema following steps in linera-indexer-graphql-client/README.md\n"
    )
}
