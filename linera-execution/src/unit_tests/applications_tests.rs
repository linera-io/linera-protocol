// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{
    ApplicationRegistry, ApplicationRegistryView, BytecodeLocation, UserApplicationDescription,
    UserApplicationId,
};
use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainId, EffectId},
};
use serde::{Deserialize, Serialize};

fn effect_id(index: u32) -> EffectId {
    EffectId {
        chain_id: ChainId::root(0),
        height: BlockHeight::from(0),
        index,
    }
}

fn bytecode_id(index: u32) -> BytecodeId {
    BytecodeId::new(effect_id(index))
}

fn app_id(index: u32) -> UserApplicationId {
    UserApplicationId {
        bytecode_id: bytecode_id(0),
        creation: effect_id(index),
    }
}

fn app_description(index: u32, deps: Vec<u32>) -> UserApplicationDescription {
    UserApplicationDescription {
        bytecode_id: bytecode_id(0),
        bytecode_location: location(0),
        creation: effect_id(index),
        parameters: vec![],
        required_application_ids: deps.into_iter().map(app_id).collect(),
    }
}

fn location(operation_index: u32) -> BytecodeLocation {
    #[derive(Serialize, Deserialize)]
    struct Dummy;

    impl BcsSignable for Dummy {}

    BytecodeLocation {
        certificate_hash: CryptoHash::new(&Dummy),
        operation_index,
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
    let results = view
        .find_dependencies(vec![app_id(1)], &Default::default())
        .await
        .unwrap();
    assert_eq!(results, Vec::from_iter([2, 3, 1].into_iter().map(app_id)));
    let results = view
        .find_dependencies(vec![app_id(0)], &Default::default())
        .await
        .unwrap();
    assert_eq!(
        results,
        Vec::from_iter([2, 3, 1, 0].into_iter().map(app_id))
    );
    let results = view
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
        results,
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
    let results = view
        .find_dependencies(vec![app_id(1)], &Default::default())
        .await
        .unwrap();
    assert_eq!(results, Vec::from_iter([2, 3, 1].into_iter().map(app_id)));
    let results = view
        .find_dependencies(vec![app_id(0)], &Default::default())
        .await
        .unwrap();
    assert_eq!(
        results,
        Vec::from_iter([2, 3, 1, 0].into_iter().map(app_id))
    );
}
