// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    context::ViewContext,
    memory::SyncMemoryStore,
    sync_views::{map_view::SyncMapView, register_view::SyncRegisterView, SyncRootView, SyncView},
    ViewError,
};

type TestContext = ViewContext<(), SyncMemoryStore>;

#[derive(SyncRootView)]
#[view(context = TestContext)]
struct TestView {
    counter: SyncRegisterView<TestContext, u64>,
    map: SyncMapView<TestContext, String, u64>,
}

#[test]
fn sync_view_roundtrip() -> Result<(), ViewError> {
    let store = SyncMemoryStore::new_for_testing();
    let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());

    let mut view = TestView::load(context)?;
    assert_eq!(*view.counter.get(), 0);
    assert_eq!(view.map.get(&"a".to_string())?, None);

    view.counter.set(42);
    view.map.insert(&"a".to_string(), 1)?;
    view.map.insert(&"b".to_string(), 2)?;
    view.save()?;

    let context = TestContext::new_unchecked(store, Vec::new(), ());
    let view = TestView::load(context)?;
    assert_eq!(*view.counter.get(), 42);
    assert_eq!(view.map.get(&"a".to_string())?, Some(1));
    assert_eq!(view.map.get(&"b".to_string())?, Some(2));

    Ok(())
}
