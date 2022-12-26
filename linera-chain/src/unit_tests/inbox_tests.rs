// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_execution::Effect;

fn make_event(height: u64, index: usize, effect: impl Into<Vec<u8>>) -> Event {
    Event {
        height: BlockHeight::from(height),
        index,
        effect: Effect::User(effect.into()),
    }
}

#[tokio::test]
async fn test_inbox_add_then_remove() {
    let mut view = InboxStateView::new().await;
    // Add one event.
    view.add_event(make_event(0, 0, [0])).await.unwrap();
    // Remove the same event
    view.remove_event(&make_event(0, 0, [0])).await.unwrap();
    // Fail to add an old event.
    assert!(matches!(
        view.add_event(make_event(0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Fail to remove an old event.
    assert!(matches!(
        view.remove_event(&make_event(0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Add two more events.
    view.add_event(make_event(0, 1, [1])).await.unwrap();
    view.add_event(make_event(1, 0, [2])).await.unwrap();
    // Fail to remove non-matching event.
    assert!(matches!(
        view.remove_event(&make_event(0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // OK to skip events.
    view.remove_event(&make_event(1, 0, [2])).await.unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add() {
    let mut view = InboxStateView::new().await;
    // Remove one event by anticipation.
    view.remove_event(&make_event(0, 0, [0])).await.unwrap();
    // Add the same event
    view.add_event(make_event(0, 0, [0])).await.unwrap();
    // Fail to remove an old event.
    assert!(matches!(
        view.remove_event(&make_event(0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Fail to add an old event.
    assert!(matches!(
        view.add_event(make_event(0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Remove two more events.
    view.remove_event(&make_event(0, 1, [1])).await.unwrap();
    view.remove_event(&make_event(1, 0, [2])).await.unwrap();
    // Fail to add non-matching event.
    assert!(matches!(
        view.add_event(make_event(0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // NOT OK to skip events while adding.
    assert!(matches!(
        view.add_event(make_event(1, 0, [2])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // OK to add the two events.
    view.add_event(make_event(0, 1, [1])).await.unwrap();
    view.add_event(make_event(1, 0, [2])).await.unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}
