// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage of in-worker block export over a real network.
//!
//! The unit tests in `linera-core` drive the export task against in-process validators, so they
//! say nothing about the transport. This exercises the part they cannot: real `linera-server`
//! shards, real proxies, real gRPC, and — the point of the whole arrangement — shards that reach
//! the other validators only by going through their own proxy.
//!
//! Must be run with the `metrics` feature, because the assertion reads the proxies' metrics
//! endpoint, and because `cargo test` rebuilds the binaries the harness spawns using whatever
//! features the test command names:
//!
//! ```text
//! cargo test -p linera-service --test block_export_tests --features storage-service,metrics \
//!     -- --ignored
//! ```

#![cfg(any(feature = "scylladb", feature = "storage-service"))]

use anyhow::Result;
use linera_base::time::Duration;
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNet, LocalNetConfig},
        LineraNet, LineraNetConfig, Network,
    },
    test_name,
};
use test_case::test_case;

/// The number of requests the proxy of `validator` reports having carried to other validators on
/// behalf of its shards.
///
/// Zero unless block export actually went through the relay, which is what makes this worth
/// asserting on: every validator ends up holding every block either way, because the client talks
/// to all of them, so the blocks alone prove nothing about the path they took.
async fn relayed_requests(net: &LocalNet, validator: usize) -> Result<u64> {
    let port = net.proxy_metrics_port(validator, 0);
    let metrics = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await?
        .text()
        .await?;
    let mut total = 0;
    for line in metrics.lines() {
        // e.g. `linera_proxy_relayed_request_count{method_name="relay_confirmed_certificate"} 7`
        if line.starts_with('#') || !line.contains("proxy_relayed_request_count") {
            continue;
        }
        if let Some(value) = line.rsplit(' ').next() {
            total += value.parse::<u64>().unwrap_or(0);
        }
    }
    Ok(total)
}

#[ignore]
#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[test_log::test(tokio::test)]
async fn test_block_export_through_the_proxy(database: Database, network: Network) -> Result<()> {
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig {
        num_initial_validators: 4,
        num_shards: 1,
        export_blocks_to_committee: true,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;
    let chain = client.default_chain().expect("client has no default chain");

    // Every one of these is executed by each validator's chain worker, which hands it to that
    // chain's export task.
    for _ in 0..3 {
        client
            .transfer_with_silent_logs(1.into(), chain, chain)
            .await?;
    }

    // Export is asynchronous, and the heights a worker records trail the tip by design, so give
    // the tasks a moment to drain before looking.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Each validator pushed to the three others, so each proxy must have carried traffic. A shard
    // that had opened its own connection to a peer — the thing this design exists to prevent —
    // would leave these at zero while the blocks still propagated.
    for validator in 0..4 {
        let relayed = relayed_requests(&net, validator).await?;
        assert!(
            relayed > 0,
            "validator {validator} exported nothing through its proxy",
        );
    }

    // And the blocks really did land everywhere.
    for validator in 0..4 {
        let info = net
            .validator_client(validator)?
            .handle_chain_info_query(ChainInfoQuery::new(chain))
            .await?;
        assert_eq!(info.info.next_block_height, 3.into());
    }

    net.terminate().await?;
    Ok(())
}
