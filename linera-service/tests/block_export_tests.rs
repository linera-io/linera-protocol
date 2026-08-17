// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage of in-worker block export over a real network.
//!
//! The `linera-core` unit tests drive the export task against in-process validators, so they say
//! nothing about the transport. This covers what they cannot: real shards, proxies and gRPC.
//!
//! ```text
//! cargo test -p linera-service --test block_export_tests --features storage-service,metrics \
//!     -- --ignored
//! ```
//!
//! `metrics` is required because the assertions read the proxies' metrics endpoint, and `cargo
//! test` rebuilds the spawned binaries with whatever features the command names. Each test takes
//! `INTEGRATION_TEST_GUARD`: every network derives its ports from the same `test_offset_port()`.

#![cfg(any(feature = "scylladb", feature = "storage-service"))]

mod guard;

use anyhow::{Context as _, Result};
use guard::INTEGRATION_TEST_GUARD;
use linera_base::time::Duration;
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use linera_rpc::grpc::api::{self, validator_relay_client::ValidatorRelayClient};
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNet, LocalNetConfig},
        LineraNet, LineraNetConfig, Network,
    },
    config::BlockExportTransport,
    test_name,
};
use test_case::test_case;

/// Requests the proxy of `validator` reports carrying to other validators for its shards. Worth
/// asserting on because the client talks to every validator anyway, so the blocks arriving prove
/// nothing about the path they took.
async fn relayed_requests(net: &LocalNet, validator: usize) -> Result<u64> {
    let port = net.proxy_metrics_port(validator, 0);
    let metrics = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await?
        .text()
        .await?;
    // The scrape itself must have worked: an empty or non-Prometheus body would otherwise make
    // every count read as zero — exactly what the direct-transport test asserts, for the wrong
    // reason. A silent *rename* of the counter is indistinguishable from "never incremented"
    // here, which is why `test_block_export_through_the_proxy` asserts the same counter is
    // nonzero: the pair fails loudly if the name rots.
    anyhow::ensure!(
        metrics.lines().any(|line| line.starts_with("# TYPE")),
        "the metrics scrape of validator {validator} returned no Prometheus payload",
    );
    let mut total = 0;
    for line in metrics.lines() {
        // e.g. `linera_proxy_relayed_request_count{method_name="relay_confirmed_certificate"} 7`
        if line.starts_with('#') || !line.contains("proxy_relayed_request_count") {
            continue;
        }
        let value = line
            .rsplit(' ')
            .next()
            .expect("rsplit yields at least one piece");
        total += value
            .parse::<u64>()
            .with_context(|| format!("unparseable metric line: {line}"))?;
    }
    Ok(total)
}

/// Export sends each destination *acknowledged*, per destination address, from the first shard
/// of `validator`. Successes, not attempts: the latency histogram counts failed sends too, so a
/// transport that cannot deliver anything still moves it — this counter only moves when the
/// destination answered.
async fn export_successes(
    net: &LocalNet,
    validator: usize,
) -> Result<std::collections::BTreeMap<String, u64>> {
    let port = net.shard_metrics_port(validator, 0);
    let metrics = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await?
        .text()
        .await?;
    anyhow::ensure!(
        metrics.lines().any(|line| line.starts_with("# TYPE")),
        "the metrics scrape of validator {validator}'s shard returned no Prometheus payload",
    );
    let mut per_destination = std::collections::BTreeMap::new();
    for line in metrics.lines() {
        // e.g. `linera_block_export_sends_succeeded{validator="grpc:127.0.0.1:9001"} 4`
        if line.starts_with('#') || !line.contains("block_export_sends_succeeded") {
            continue;
        }
        let (labels, value) = line
            .rsplit_once(' ')
            .with_context(|| format!("unparseable metric line: {line}"))?;
        per_destination.insert(
            labels.to_owned(),
            value
                .parse::<u64>()
                .with_context(|| format!("unparseable metric line: {line}"))?,
        );
    }
    Ok(per_destination)
}

#[ignore]
#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[test_log::test(tokio::test)]
async fn test_block_export_through_the_proxy(database: Database, network: Network) -> Result<()> {
    let _guard: tokio::sync::MutexGuard<'_, ()> = INTEGRATION_TEST_GUARD.lock().await;
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

/// A validator admitted to the committee after a chain already has history is brought up to date
/// by export alone.
///
/// The newcomer has never heard of the chain, so it answers a height query with 0 and every block
/// is replayed. Catch-up is bounded per round, so this asserts convergence, not one-shot.
#[ignore]
#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[test_log::test(tokio::test)]
async fn test_export_catches_up_a_newly_added_validator(
    database: Database,
    network: Network,
) -> Result<()> {
    let _guard: tokio::sync::MutexGuard<'_, ()> = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig {
        num_initial_validators: 4,
        num_shards: 1,
        export_blocks_to_committee: true,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;
    let chain = client.default_chain().expect("client has no default chain");

    // History the newcomer will have to be told about in full — on the client's chain, and on a
    // second chain that stays completely idle from here on. The idle one is the harder case:
    // nothing after the admission ever touches it, so only the export queue's own bookkeeping
    // can tell the newcomer it exists.
    for _ in 0..3 {
        client
            .transfer_with_silent_logs(1.into(), chain, chain)
            .await?;
    }
    let (idle_chain, _) = client
        .open_chain(chain, None, linera_base::data_types::Amount::from_tokens(2))
        .await?;
    for _ in 0..2 {
        client
            .transfer_with_silent_logs(1.into(), idle_chain, chain)
            .await?;
    }

    // Bring up a fifth validator and admit it. It starts knowing nothing of this chain.
    net.generate_validator_config(4).await?;
    net.start_validator(4).await?;
    client
        .set_validator(
            net.validator_keys(4).unwrap(),
            net.proxy_public_port(4, 0),
            100,
        )
        .await?;

    // Nothing below drives the chain forward, so any progress the newcomer makes on it is export
    // replaying the history to it.
    let target = net
        .validator_client(0)?
        .handle_chain_info_query(ChainInfoQuery::new(chain))
        .await?
        .info
        .next_block_height;
    let idle_target = net
        .validator_client(0)?
        .handle_chain_info_query(ChainInfoQuery::new(idle_chain))
        .await?
        .info
        .next_block_height;
    assert!(
        idle_target >= 2.into(),
        "the idle chain needs history for its replay to be observable",
    );

    for _ in 0..60 {
        // A query the newcomer cannot answer yet — it may not even hold a chain's description —
        // is the transient this poll waits out, not a failure.
        let height = net
            .validator_client(4)?
            .handle_chain_info_query(ChainInfoQuery::new(chain))
            .await
            .map_or(linera_base::data_types::BlockHeight::ZERO, |response| {
                response.info.next_block_height
            });
        let idle_height = net
            .validator_client(4)?
            .handle_chain_info_query(ChainInfoQuery::new(idle_chain))
            .await
            .map_or(linera_base::data_types::BlockHeight::ZERO, |response| {
                response.info.next_block_height
            });
        if height >= target && idle_height >= idle_target {
            net.terminate().await?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let height = net
        .validator_client(4)?
        .handle_chain_info_query(ChainInfoQuery::new(chain))
        .await
        .map_or(linera_base::data_types::BlockHeight::ZERO, |response| {
            response.info.next_block_height
        });
    let idle_height = net
        .validator_client(4)?
        .handle_chain_info_query(ChainInfoQuery::new(idle_chain))
        .await
        .map_or(linera_base::data_types::BlockHeight::ZERO, |response| {
            response.info.next_block_height
        });
    panic!(
        "the newly added validator did not catch up: active chain {height}/{target}, \
         idle chain {idle_height}/{idle_target}",
    );
}

/// Export keeps flowing when one of this validator's proxies dies.
///
/// A destination keeps the proxy it was handed, so without the rebuild-on-failure path a dead
/// proxy strands every destination assigned to it — silently, since the chain still looks healthy.
#[ignore]
#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[test_log::test(tokio::test)]
async fn test_export_survives_a_dead_proxy(database: Database, network: Network) -> Result<()> {
    let _guard: tokio::sync::MutexGuard<'_, ()> = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig {
        num_initial_validators: 4,
        num_shards: 1,
        num_proxies: 2,
        export_blocks_to_committee: true,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;
    let chain = client.default_chain().expect("client has no default chain");

    client
        .transfer_with_silent_logs(1.into(), chain, chain)
        .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let before = relayed_requests(&net, 0).await?;
    assert!(
        before > 0,
        "export should be relaying before the proxy is killed"
    );
    // Every destination must have been reached at least once before the kill, so that "its
    // counter grew afterwards" is meaningful for all three.
    let mut reached = std::collections::BTreeMap::new();
    for _ in 0..30 {
        reached = export_successes(&net, 0).await?;
        if reached.len() >= 3 && reached.values().all(|count| *count > 0) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        reached.len() >= 3 && reached.values().all(|count| *count > 0),
        "not every destination was reached before the kill: {reached:?}",
    );

    // Kill one of validator 0's two proxies. Destinations pinned to it must move to the other.
    net.kill_proxy(0, 1).await?;

    // The baseline is taken *after* the kill: a send that completed between an earlier snapshot
    // and the proxy actually dying would count towards "grew afterwards" while proving nothing.
    // Growth beyond this baseline can only come through the surviving proxy.
    let successes_before = export_successes(&net, 0).await?;

    // The load-bearing assertion: *every* destination's acknowledged-send counter grows after
    // the kill. The chains converging proves nothing (the client broadcasts every block to every
    // validator whether export works or not), and total relay traffic on the surviving proxy
    // proves little more — the destinations round-robined onto proxy 0 keep it growing even
    // with the destination stranded on the dead proxy never re-pointed. A frozen per-destination
    // counter is that stranding, directly. Blocks are produced inside the loop so a lagging
    // recovery still has traffic to prove itself on.
    for attempt in 0..30 {
        client
            .transfer_with_silent_logs(1.into(), chain, chain)
            .await?;
        let successes = export_successes(&net, 0).await?;
        let all_grew = successes_before
            .iter()
            .all(|(address, before)| successes.get(address).is_some_and(|now| now > before));
        if all_grew {
            net.terminate().await?;
            return Ok(());
        }
        if attempt == 29 {
            panic!(
                "a destination stopped being reached after its proxy was killed: \
                 {successes_before:?} before, {successes:?} after",
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    unreachable!("the loop either returns or panics on its last attempt");
}

/// The proxy refuses to relay to a host that is not in any committee.
///
/// Relaying only buys anything if the proxy will not dial wherever it is told, so this asserts
/// the refusal directly rather than inferring it from export working.
#[ignore]
#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[test_log::test(tokio::test)]
async fn test_relay_refuses_a_non_committee_destination(
    database: Database,
    network: Network,
) -> Result<()> {
    let _guard: tokio::sync::MutexGuard<'_, ()> = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig {
        num_initial_validators: 4,
        num_shards: 1,
        export_blocks_to_committee: true,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;
    let chain = client.default_chain().expect("client has no default chain");

    // Produce a block so the proxy has certainly served some legitimate relay traffic; whatever
    // rejection follows is therefore about the destination, not about the relay being unavailable.
    client
        .transfer_with_silent_logs(1.into(), chain, chain)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Speak to validator 0's relay port directly, as one of its own shards would, but name a host
    // that is in no committee.
    let relay = format!("http://127.0.0.1:{}", net.proxy_internal_port(0, 0));
    let channel = tonic::transport::Channel::from_shared(relay)?
        .connect()
        .await?;
    let mut relay_client = ValidatorRelayClient::new(channel);
    let status = relay_client
        .relay_chain_info_query(api::RelayChainInfoQueryRequest {
            destination: "grpc:198.51.100.7:443".to_string(),
            inner: Some(ChainInfoQuery::new(chain).try_into()?),
        })
        .await
        .expect_err("the proxy should refuse a destination that is in no committee");
    assert_eq!(
        status.code(),
        tonic::Code::PermissionDenied,
        "expected a refusal, got: {status:?}",
    );

    net.terminate().await?;
    Ok(())
}

/// With `--block-export-transport direct`, shards reach the other validators themselves and the
/// proxy carries nothing.
///
/// The exact mirror of `test_block_export_through_the_proxy` — same blocks everywhere, but the
/// relay counters stay at zero — so the two together pin down which path each setting takes. The
/// trade-off: shards hold the validator secret key, so direct means giving them outbound access.
#[ignore]
#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[test_log::test(tokio::test)]
async fn test_block_export_direct_bypasses_the_proxy(
    database: Database,
    network: Network,
) -> Result<()> {
    let _guard: tokio::sync::MutexGuard<'_, ()> = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig {
        num_initial_validators: 4,
        num_shards: 1,
        export_blocks_to_committee: true,
        block_export_transport: BlockExportTransport::Direct,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;
    let chain = client.default_chain().expect("client has no default chain");

    for _ in 0..3 {
        client
            .transfer_with_silent_logs(1.into(), chain, chain)
            .await?;
    }

    // The load-bearing positive assertion: every destination *acknowledged* a direct send. The
    // blocks reaching every validator proves nothing (the client broadcasts to all of them), the
    // relay counters staying at zero is also satisfied by a transport that exports nothing, and
    // counting send *attempts* would also be satisfied by one that fails every time.
    let mut successes = std::collections::BTreeMap::new();
    for _ in 0..30 {
        successes = export_successes(&net, 0).await?;
        if successes.len() >= 3 && successes.values().all(|count| *count > 0) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        successes.len() >= 3 && successes.values().all(|count| *count > 0),
        "the direct transport was not acknowledged by every destination: {successes:?}",
    );

    // The blocks still reach every validator...
    for validator in 0..4 {
        let info = net
            .validator_client(validator)?
            .handle_chain_info_query(ChainInfoQuery::new(chain))
            .await?;
        assert_eq!(info.info.next_block_height, 3.into());
    }

    // ...without a single one of them going through a proxy's relay.
    for validator in 0..4 {
        let relayed = relayed_requests(&net, validator).await?;
        assert_eq!(
            relayed, 0,
            "validator {validator} relayed {relayed} requests through its proxy despite \
             --block-export-transport direct",
        );
    }

    net.terminate().await?;
    Ok(())
}
