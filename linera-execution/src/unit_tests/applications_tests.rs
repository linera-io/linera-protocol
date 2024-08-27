// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainId, MessageId},
};

use super::{
    ApplicationRegistry, ApplicationRegistryView, UserApplicationDescription, UserApplicationId,
};

fn message_id(index: u32) -> MessageId {
    MessageId {
        chain_id: ChainId::root(0),
        height: BlockHeight::ZERO,
        index,
    }
}

fn bytecode_id() -> BytecodeId {
    BytecodeId::new(
        CryptoHash::test_hash("contract"),
        CryptoHash::test_hash("service"),
    )
}

fn app_id(index: u32) -> UserApplicationId {
    UserApplicationId {
        bytecode_id: bytecode_id(),
        creation: message_id(index),
    }
}

fn app_description(index: u32, deps: Vec<u32>) -> UserApplicationDescription {
    UserApplicationDescription {
        bytecode_id: bytecode_id(),
        creation: message_id(index),
        parameters: vec![],
        required_application_ids: deps.into_iter().map(app_id).collect(),
    }
}

fn registry(graph: impl IntoIterator<Item = (u32, Vec<u32>)>) -> ApplicationRegistry {
    let mut registry = ApplicationRegistry::default();
    for (index, deps) in graph {
        let description = app_description(index, deps);
        registry
            .known_applications
            .insert((&description).into(), description);
    }
    registry
}

#[tokio::test]
async fn test_topological_sort() {
    let mut view = ApplicationRegistryView::new().await;
    view.import(registry([(1, vec![2, 3])])).unwrap();
    assert!(view
        .find_dependencies(vec![app_id(1)], &Default::default())
        .await
        .is_err());
    view.import(registry([(3, vec![2]), (2, vec![]), (0, vec![1])]))
        .unwrap();
    let app_ids = view
        .find_dependencies(vec![app_id(1)], &Default::default())
        .await
        .unwrap();
    assert_eq!(app_ids, Vec::from_iter([2, 3, 1].into_iter().map(app_id)));
    let app_ids = view
        .find_dependencies(vec![app_id(0)], &Default::default())
        .await
        .unwrap();
    assert_eq!(
        app_ids,
        Vec::from_iter([2, 3, 1, 0].into_iter().map(app_id))
    );
    let app_ids = view
        .find_dependencies(
            vec![app_id(0), app_id(5)],
            &vec![
                (app_id(5), app_description(5, vec![4])),
                (app_id(4), app_description(5, vec![2])),
            ]
            .into_iter()
            .collect(),
        )
        .await
        .unwrap();
    assert_eq!(
        app_ids,
        Vec::from_iter([2, 4, 5, 3, 1, 0].into_iter().map(app_id))
    );
}

#[tokio::test]
async fn test_topological_sort_with_loop() {
    let mut view = ApplicationRegistryView::new().await;
    view.import(registry([
        (1, vec![2, 3]),
        (3, vec![1, 2]),
        (2, vec![]),
        (0, vec![1]),
    ]))
    .unwrap();
    let app_ids = view
        .find_dependencies(vec![app_id(1)], &Default::default())
        .await
        .unwrap();
    assert_eq!(app_ids, Vec::from_iter([2, 3, 1].into_iter().map(app_id)));
    let app_ids = view
        .find_dependencies(vec![app_id(0)], &Default::default())
        .await
        .unwrap();
    assert_eq!(
        app_ids,
        Vec::from_iter([2, 3, 1, 0].into_iter().map(app_id))
    );
}
