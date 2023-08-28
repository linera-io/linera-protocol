// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::ValidatorServerConfig;
use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, ListParams},
    Client,
};
use std::{collections::BTreeMap, net::IpAddr, str::FromStr};

pub async fn get_ips_from_k8s(config: &mut ValidatorServerConfig) -> Result<()> {
    let pod_name_to_ip = fetch_pod_ips().await?;

    // Assumes that each service will have just one pod. Since we have one service per shard,
    // the first pod will always have -0 at the end of it for servers and some hash for the validators.
    // So we check the prefix and assume only one entry will have that prefix.
    config.validator.network.host =
        find_value_with_key_prefix(&pod_name_to_ip, &config.validator.network.host)
            .expect("All hosts should be a prefix of a pod name!");
    config.internal_network.host =
        find_value_with_key_prefix(&pod_name_to_ip, &config.internal_network.host)
            .expect("All hosts should be a prefix of a pod name!");
    for shard in &mut config.internal_network.shards {
        shard.host = find_value_with_key_prefix(&pod_name_to_ip, &shard.host)
            .expect("All hosts should be a prefix of a pod name!");
        shard.metrics_host = find_value_with_key_prefix(&pod_name_to_ip, &shard.metrics_host)
            .expect("All hosts should be a prefix of a pod name!");
    }

    Ok(())
}

pub async fn fetch_pod_ips() -> Result<BTreeMap<String, String>> {
    let mut pod_name_to_ip = BTreeMap::new();
    let client = Client::try_default()
        .await
        .expect("Kubernetes client to be created");

    let pods: Api<Pod> = Api::namespaced(client, "default");
    let lp = ListParams::default();
    let pod_list = pods.list(&lp).await.expect("Failed to fetch pod list");

    for pod in pod_list.items {
        let pod_ip = pod
            .status
            .expect("Failed to get pod status")
            .pod_ip
            .expect("Failed to get pod IP");
        pod_name_to_ip.insert(pod.metadata.name.expect("Failed to get pod name"), pod_ip);
    }

    Ok(pod_name_to_ip)
}

pub fn find_value_with_key_prefix(map: &BTreeMap<String, String>, prefix: &str) -> Option<String> {
    if IpAddr::from_str(prefix).is_ok() {
        // If prefix is already an actual set IP, just return it
        // This will be the case for validators
        return Some(prefix.to_string());
    }

    let mut range = map.range(prefix.to_string()..);
    if let Some((key, value)) = range.next() {
        if key.starts_with(prefix) {
            return Some(value.clone());
        }
    }

    None
}
