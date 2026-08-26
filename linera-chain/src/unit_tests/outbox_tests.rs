// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn test_outbox() {
    let mut view = OutboxStateView::new().await;
    assert!(view.schedule_message(BlockHeight::ZERO).unwrap());
    assert!(view.schedule_message(BlockHeight::from(2)).unwrap());
    assert!(view.schedule_message(BlockHeight::from(4)).unwrap());
    assert!(!view.schedule_message(BlockHeight::ZERO).unwrap());

    assert_eq!(view.queue.count(), 3);
    assert_eq!(
        view.mark_messages_as_received(BlockHeight::from(3))
            .await
            .unwrap(),
        vec![BlockHeight::ZERO, BlockHeight::from(2)]
    );
    assert_eq!(
        view.mark_messages_as_received(BlockHeight::from(3))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(view.queue.count(), 1);
    assert_eq!(
        view.mark_messages_as_received(BlockHeight::from(4))
            .await
            .unwrap(),
        vec![BlockHeight::from(4)]
    );
    assert_eq!(view.queue.count(), 0);
}

#[tokio::test]
async fn test_outbox_revert() {
    let mut view = OutboxStateView::new().await;
    // Schedule heights 0, 2, 4.
    assert!(view.schedule_message(BlockHeight::ZERO).unwrap());
    assert!(view.schedule_message(BlockHeight::from(2)).unwrap());
    assert!(view.schedule_message(BlockHeight::from(4)).unwrap());
    // Confirm up to height 2 (removes 0 and 2 from queue).
    view.mark_messages_as_received(BlockHeight::from(2))
        .await
        .unwrap();
    assert_eq!(view.queue.count(), 1); // only height 4 remains

    // Revert: re-add heights 0 and 2.
    let new = view
        .revert(&[BlockHeight::ZERO, BlockHeight::from(2)])
        .await
        .unwrap();
    assert_eq!(new, vec![BlockHeight::ZERO, BlockHeight::from(2)]);
    // Queue now has all three heights in order.
    let elements = view.queue.elements().await.unwrap();
    assert_eq!(
        elements,
        vec![
            BlockHeight::ZERO,
            BlockHeight::from(2),
            BlockHeight::from(4)
        ]
    );

    // Reverting with already-present heights is a no-op.
    let new = view
        .revert(&[BlockHeight::from(2), BlockHeight::from(4)])
        .await
        .unwrap();
    assert!(new.is_empty());
    assert_eq!(view.queue.count(), 3);
}
