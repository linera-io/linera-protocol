// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, ListParams},
    Client,
};
use std::{collections::BTreeMap, net::IpAddr, str::FromStr};

pub async fn fetch_pod_ips() -> Result<BTreeMap<String, String>> {
    let mut pod_name_to_ip = BTreeMap::new();
    // Create a Kubernetes client
    let client = Client::try_default()
        .await
        .expect("Kubernetes client to be created");

    // Access the API for pods
    let pods: Api<Pod> = Api::namespaced(client, "default");

    let lp = ListParams::default();

    // List all pods in the "default" namespace
    let pod_list = pods.list(&lp).await.unwrap();

    // Loop through the pods and get their IP addresses
    for pod in pod_list.items {
        let pod_ip = pod.status.unwrap().pod_ip.unwrap();
        pod_name_to_ip.insert(pod.metadata.name.unwrap(), pod_ip);
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
