// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{ChainId, MessageId, ModuleId},
    vm::VmRuntime,
};

use super::{
    ApplicationRegistry, ApplicationRegistryView, ApplicationIdDescription, ApplicationId,
};

fn message_id(index: u32) -> MessageId {
    MessageId {
        chain_id: ChainId::root(0),
        height: BlockHeight::ZERO,
        index,
    }
}

fn module_id() -> ModuleId {
    ModuleId::new(
        CryptoHash::test_hash("contract"),
        CryptoHash::test_hash("service"),
        VmRuntime::Wasm,
    )
}

fn app_id(index: u32) -> ApplicationId {
    ApplicationId {
        module_id: module_id(),
        creation: message_id(index),
    }
}

fn app_description(index: u32, deps: Vec<u32>) -> ApplicationIdDescription {
    ApplicationIdDescription {
        module_id: module_id(),
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
    assert!(view.find_dependencies(vec![app_id(1)]).await.is_err());
    view.import(registry([(3, vec![2]), (2, vec![]), (0, vec![1])]))
        .unwrap();
    let app_ids = view.find_dependencies(vec![app_id(1)]).await.unwrap();
    assert_eq!(app_ids, Vec::from_iter([2, 3, 1].into_iter().map(app_id)));
    let app_ids = view.find_dependencies(vec![app_id(0)]).await.unwrap();
    assert_eq!(
        app_ids,
        Vec::from_iter([2, 3, 1, 0].into_iter().map(app_id))
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
    let app_ids = view.find_dependencies(vec![app_id(1)]).await.unwrap();
    assert_eq!(app_ids, Vec::from_iter([2, 3, 1].into_iter().map(app_id)));
    let app_ids = view.find_dependencies(vec![app_id(0)]).await.unwrap();
    assert_eq!(
        app_ids,
        Vec::from_iter([2, 3, 1, 0].into_iter().map(app_id))
    );
}
