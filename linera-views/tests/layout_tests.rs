// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The layout a composite view reports, and the agreement between that layout and the keys the
//! view actually writes.

use linera_views::{
    collection_view::CollectionView,
    context::{Context as _, MemoryContext},
    log_view::LogView,
    map_view::MapView,
    register_view::RegisterView,
    set_view::SetView,
    views::{
        layout::{DescribeLayout, ViewLayout},
        View,
    },
};

/// A view with one of every shape the description has to cover: a plain value, a tagged view, an
/// indexed view, and a collection that recurses into a subview.
#[derive(linera_views::views::View)]
struct SampleView<C> {
    counter: RegisterView<C, u64>,
    log: LogView<C, String>,
    names: MapView<C, u32, String>,
    tags: SetView<C, u32>,
    nested: CollectionView<C, u32, RegisterView<C, u64>>,
}

#[test]
fn test_layout() {
    insta::assert_yaml_snapshot!(
        "sample_view_layout.yaml",
        SampleView::<MemoryContext<()>>::layout()
    );
}

/// Walks the reported layout to the leaf a field occupies and returns the key prefix that path
/// implies, so it can be checked against a key the view really wrote.
fn prefix_of_field(layout: &ViewLayout, field: &str) -> Vec<u8> {
    let ViewLayout::Struct(fields) = layout else {
        panic!("expected a struct layout");
    };
    let field = fields
        .iter()
        .find(|candidate| candidate.name == field)
        .expect("no such field");
    // A derived struct puts each field under `[MIN_VIEW_TAG, field index]`, the index encoded
    // as the layout says.
    let mut prefix = vec![linera_views::views::MIN_VIEW_TAG];
    match field.index_type {
        "u8" => prefix.extend(bcs::to_bytes(&u8::try_from(field.index).unwrap()).unwrap()),
        "u16" => prefix.extend(bcs::to_bytes(&u16::try_from(field.index).unwrap()).unwrap()),
        other => panic!("unexpected index type {other}"),
    }
    prefix
}

/// The description has to agree with the keys the view writes, or it is decoration. This walks
/// one field of the layout and checks that the batch a real write produces lands under the
/// prefix the layout predicts.
#[tokio::test]
async fn layout_agrees_with_the_keys_written() -> anyhow::Result<()> {
    let context = MemoryContext::new_for_testing(());
    let mut view = SampleView::load(context.clone()).await?;
    view.counter.set(42);
    view.log.push("entry".to_owned());

    let mut batch = linera_views::batch::Batch::new();
    view.pre_save(&mut batch)?;

    let layout = SampleView::<MemoryContext<()>>::layout();
    let base = context.base_key().bytes.clone();

    for field in ["counter", "log"] {
        let mut expected = base.clone();
        expected.extend(prefix_of_field(&layout, field));
        let written = batch
            .operations
            .iter()
            .filter_map(|operation| match operation {
                linera_views::batch::WriteOperation::Put { key, .. } => Some(key),
                _ => None,
            })
            .filter(|key| key.starts_with(&expected))
            .count();
        assert!(
            written > 0,
            "the layout puts `{field}` under {expected:?}, but nothing was written there"
        );
    }

    // And nothing was written outside the prefixes the layout accounts for.
    let ViewLayout::Struct(fields) = &layout else {
        panic!("expected a struct layout");
    };
    let known: Vec<Vec<u8>> = fields
        .iter()
        .map(|field| {
            let mut prefix = base.clone();
            prefix.extend(prefix_of_field(&layout, field.name));
            prefix
        })
        .collect();
    for operation in &batch.operations {
        let linera_views::batch::WriteOperation::Put { key, .. } = operation else {
            continue;
        };
        assert!(
            known.iter().any(|prefix| key.starts_with(prefix)),
            "{key:?} was written outside every prefix the layout describes"
        );
    }
    Ok(())
}
