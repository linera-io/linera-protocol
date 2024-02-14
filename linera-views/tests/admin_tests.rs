// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "rocksdb", feature = "aws", feature = "scylladb"))]

// We exercise the functionality of the `AdminKeyValueStore`. We use a prefix
// to the list of tables created so that this test can be run in parallel to
// other tests.

use linera_views::{common::AdminKeyValueStore, test_utils::generate_test_namespace};
use rand::{Rng, SeedableRng};
use std::{collections::BTreeSet, fmt::Debug};

#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::{create_rocks_db_test_config, RocksDbStore};

#[cfg(feature = "aws")]
use linera_views::dynamo_db::{create_dynamo_db_test_config, DynamoDbStore};

#[cfg(feature = "scylladb")]
use linera_views::scylla_db::{create_scylla_db_test_config, ScyllaDbStore};

#[cfg(test)]
async fn namespaces_with_prefix<S: AdminKeyValueStore>(
    config: &S::Config,
    prefix: &str,
) -> BTreeSet<String>
where
    S::Error: Debug,
{
    let namespaces = S::list_all(config).await.expect("namespaces");
    namespaces
        .into_iter()
        .filter(|x| x.starts_with(prefix))
        .collect::<BTreeSet<_>>()
}

#[cfg(test)]
async fn admin_test<S: AdminKeyValueStore>(config: &S::Config)
where
    S::Error: Debug,
{
    let prefix = generate_test_namespace();
    let namespaces = namespaces_with_prefix::<S>(config, &prefix).await;
    assert_eq!(namespaces.len(), 0);
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let size = 9;
    // Creating the initial list of namespaces
    let mut working_namespaces = BTreeSet::new();
    for i in 0..size {
        let namespace = format!("{}_{}", prefix, i);
        assert!(!S::exists(config, &namespace).await.expect("test"));
        working_namespaces.insert(namespace);
    }
    // Creating the namespaces
    for namespace in &working_namespaces {
        S::create(config, namespace)
            .await
            .expect("creation of a namespace");
        assert!(S::exists(config, namespace).await.expect("test"));
    }
    // Listing all of them
    let namespaces = namespaces_with_prefix::<S>(config, &prefix).await;
    assert_eq!(namespaces, working_namespaces);
    // Selecting at random some for deletion
    let mut deleted_namespaces = BTreeSet::new();
    let mut kept_namespaces = BTreeSet::new();
    for namespace in working_namespaces {
        let delete = rng.gen::<bool>();
        if delete {
            S::delete(config, &namespace)
                .await
                .expect("A successful deletion");
            assert!(!S::exists(config, &namespace).await.expect("test"));
            deleted_namespaces.insert(namespace);
        } else {
            kept_namespaces.insert(namespace);
        }
    }
    for namespace in &kept_namespaces {
        assert!(S::exists(config, namespace).await.expect("test"));
    }
    let namespaces = namespaces_with_prefix::<S>(config, &prefix).await;
    assert_eq!(namespaces, kept_namespaces);
    for namespace in kept_namespaces {
        S::delete(config, &namespace)
            .await
            .expect("A successful deletion");
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn admin_test_rocks_db() {
    let (config, _dir) = create_rocks_db_test_config().await;
    admin_test::<RocksDbStore>(&config).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn admin_test_dynamo_db() {
    let config = create_dynamo_db_test_config().await;
    admin_test::<DynamoDbStore>(&config).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn admin_test_scylla_db() {
    let config = create_scylla_db_test_config().await;
    admin_test::<ScyllaDbStore>(&config).await;
}
