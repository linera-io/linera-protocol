// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn test_outbox() {
    let mut view = OutboxStateView::new().await;
    assert!(view.schedule_message(BlockHeight::from(0)).unwrap());
    assert!(view.schedule_message(BlockHeight::from(2)).unwrap());
    assert!(view.schedule_message(BlockHeight::from(4)).unwrap());
    assert!(!view.schedule_message(BlockHeight::from(0)).unwrap());

    assert_eq!(view.queue.count(), 3);
    assert!(view
        .mark_messages_as_received(BlockHeight::from(3))
        .await
        .unwrap());
    assert!(!view
        .mark_messages_as_received(BlockHeight::from(3))
        .await
        .unwrap());
    assert_eq!(view.queue.count(), 1);
    assert!(view
        .mark_messages_as_received(BlockHeight::from(4))
        .await
        .unwrap());
    assert_eq!(view.queue.count(), 0);
}
