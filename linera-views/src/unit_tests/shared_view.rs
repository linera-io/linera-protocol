// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests the behavior of [`SharedView`].

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use linera_views::{
    memory::create_memory_context,
    register_view::RegisterView,
    shared_view::SharedView,
    views::{RootView, View, ViewError},
};
use std::{mem, time::Duration};
use tokio::time::sleep;

/// Test if a [`View`] can be shared among multiple readers.
#[tokio::test(start_paused = true)]
async fn test_multiple_readers() -> Result<(), ViewError> {
    let context = create_memory_context();

    let dummy_value = 82;
    let mut dummy_view = SimpleView::load(context).await?;
    dummy_view.byte.set(dummy_value);

    let mut shared_view = SharedView::new(dummy_view);

    let tasks = FuturesUnordered::new();

    for _ in 0..100 {
        let reference = shared_view
            .inner()
            .now_or_never()
            .expect("Read-only reference should be immediately available")?;

        let task = tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            *reference.byte.get()
        });

        tasks.push(task);
    }

    tasks
        .for_each_concurrent(100, |read_value| async {
            assert_eq!(read_value.unwrap(), dummy_value);
        })
        .await;

    Ok(())
}

/// Test if a [`View`] is shared with at most one writer.
#[tokio::test(start_paused = true)]
async fn test_if_second_writer_waits_for_first_writer() -> Result<(), ViewError> {
    let context = create_memory_context();
    let dummy_view = SimpleView::load(context).await?;
    let mut shared_view = SharedView::new(dummy_view);

    let writer_reference = shared_view
        .inner_mut()
        .now_or_never()
        .expect("First read-write reference should be immediately available");

    assert!(
        shared_view.inner_mut().now_or_never().is_none(),
        "Second read-write reference should wait for first writer to finish"
    );

    mem::drop(writer_reference);

    let _second_writer_reference = shared_view.inner_mut().now_or_never().expect(
        "Second read-write reference should be immediately available after the first writer \
        finishes",
    );

    Ok(())
}

/// Test if a [`View`] stops sharing with new readers when it is shared with one writer.
#[tokio::test(start_paused = true)]
async fn test_writer_blocks_new_readers() -> Result<(), ViewError> {
    let context = create_memory_context();
    let dummy_view = SimpleView::load(context).await?;
    let mut shared_view = SharedView::new(dummy_view);

    let _first_reader_reference = shared_view
        .inner()
        .now_or_never()
        .expect("Initial read-only references should be immediately available");
    let _second_reader_reference = shared_view
        .inner()
        .now_or_never()
        .expect("Initial read-only references should be immediately available");

    let writer_reference = shared_view
        .inner_mut()
        .now_or_never()
        .expect("First read-write reference should be immediately available");

    assert!(
        shared_view.inner().now_or_never().is_none(),
        "Read-only references should wait for writer to finish"
    );

    mem::drop(writer_reference);

    let _third_reader_reference = shared_view.inner().now_or_never().expect(
        "Third read-only reference should be immediately available after the writer finishes",
    );

    Ok(())
}

/// Test if writer waits for readers to finish before saving.
#[tokio::test(start_paused = true)]
async fn test_writer_waits_for_readers() -> Result<(), ViewError> {
    let context = create_memory_context();
    let dummy_view = SimpleView::load(context).await?;
    let mut shared_view = SharedView::new(dummy_view);

    let reader_delays = [100, 300, 250, 200, 150, 400, 200]
        .into_iter()
        .map(Duration::from_millis);

    let reader_tasks = FuturesUnordered::new();

    for delay in reader_delays {
        let reader_reference = shared_view.inner().await?;

        reader_tasks.push(tokio::spawn(async move {
            let _reader_reference = reader_reference;
            sleep(delay).await;
        }));
    }

    let mut writer_reference = shared_view.inner_mut().await?;
    writer_reference.save().await?;

    let readers_collector =
        reader_tasks.for_each(|task_result| async move { assert!(task_result.is_ok()) });

    assert_eq!(
        readers_collector.now_or_never(),
        Some(()),
        "Reader tasks should have finished before the writer saved, so collecting the task \
        results should finish immediately"
    );

    Ok(())
}

/// A simple view used to test sharing views.
#[derive(RootView)]
struct SimpleView<C> {
    byte: RegisterView<C, u8>,
}
