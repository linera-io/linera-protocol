// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "dynamodb",
    feature = "scylladb",
    feature = "storage-service",
))]
use anyhow::Result;
use linera_base::time::Duration;
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use linera_rpc::config::ExporterServiceConfig;
use linera_service::{
    cli_wrappers::{
        local_net::{Database, ExportersSetup, LocalNet, LocalNetConfig},
        LineraNetConfig, Network,
    },
    config::{BlockExporterConfig, Destination, DestinationConfig, LimitsConfig},
    test_name,
};
use test_case::test_case;

#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_linera_exporter(database: Database, network: Network) -> Result<()> {
    tracing::info!("Starting test {}", test_name!());

    let num_shards = 1;
    let num_initial_validators = 1;
    // This is based on the formula for proxy_public_port in local_net.rs
    let port = LocalNet::first_public_port() + num_shards;
    let destination = Destination::Validator {
        endpoint: "127.0.0.1".to_owned(),
        port: port as u16,
    };

    let destination_config = DestinationConfig {
        committee_destination: false,
        destinations: vec![destination],
    };

    let block_exporter_config = BlockExporterConfig {
        destination_config,
        id: 0,
        service_config: ExporterServiceConfig {
            host: "".to_owned(),
            port: 0,
        },
        limits: LimitsConfig::default(),
        metrics_port: 1234,
    };

    let config = LocalNetConfig {
        num_initial_validators,
        num_shards,
        block_exporters: ExportersSetup::Local(vec![block_exporter_config]),
        ..LocalNetConfig::new_test(database, network)
    };

    let (mut net, client) = config.instantiate().await?;

    net.generate_validator_config(1).await?;
    // Start a new validator.
    net.start_validator(1).await?;

    let chain = client.default_chain().expect("Client has no default chain");
    // Trigger a block export and sync.
    client
        .transfer_with_silent_logs(1.into(), chain, chain)
        .await?;

    tokio::time::sleep(Duration::from_secs(4)).await;

    let validator_client = net.validator_client(1).await?;
    let chain_info = validator_client
        .handle_chain_info_query(ChainInfoQuery::new(chain))
        .await?;

    // Check that the block exporter has exported the block.
    assert!(chain_info.info.next_block_height == 1.into());

    Ok(())
}
