// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_base::crypto::{BcsSignable, HashValue};
use linera_execution::Effect;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Dummy;
#[derive(Serialize, Deserialize)]
struct Dummy2;

impl BcsSignable for Dummy {}
impl BcsSignable for Dummy2 {}

fn make_event(
    certificate_hash: HashValue,
    height: u64,
    index: usize,
    effect: impl Into<Vec<u8>>,
) -> Event {
    Event {
        certificate_hash,
        height: BlockHeight::from(height),
        index,
        timestamp: Default::default(),
        effect: Effect::User(effect.into()),
    }
}

#[tokio::test]
async fn test_inbox_add_then_remove() {
    let hash = HashValue::new(&Dummy);
    let mut view = InboxStateView::new().await;
    // Add one event.
    view.add_event(make_event(hash, 0, 0, [0])).await.unwrap();
    // Remove the same event
    view.remove_event(&make_event(hash, 0, 0, [0]))
        .await
        .unwrap();
    // Fail to add an old event.
    assert!(matches!(
        view.add_event(make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Fail to remove an old event.
    assert!(matches!(
        view.remove_event(&make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Add two more events.
    view.add_event(make_event(hash, 0, 1, [1])).await.unwrap();
    view.add_event(make_event(hash, 1, 0, [2])).await.unwrap();
    // Fail to remove non-matching event.
    assert!(matches!(
        view.remove_event(&make_event(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to remove non-matching even (hash).
    assert!(matches!(
        view.remove_event(&make_event(HashValue::new(&Dummy2), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // OK to skip events.
    view.remove_event(&make_event(hash, 1, 0, [2]))
        .await
        .unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add() {
    let hash = HashValue::new(&Dummy);
    let mut view = InboxStateView::new().await;
    // Remove one event by anticipation.
    view.remove_event(&make_event(hash, 0, 0, [0]))
        .await
        .unwrap();
    // Add the same event
    view.add_event(make_event(hash, 0, 0, [0])).await.unwrap();
    // Fail to remove an old event.
    assert!(matches!(
        view.remove_event(&make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Fail to add an old event.
    assert!(matches!(
        view.add_event(make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Remove two more events.
    view.remove_event(&make_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    view.remove_event(&make_event(hash, 1, 0, [2]))
        .await
        .unwrap();
    // Fail to add non-matching event.
    assert!(matches!(
        view.add_event(make_event(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to add non-matching event (hash).
    assert!(matches!(
        view.add_event(make_event(HashValue::new(&Dummy2), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // NOT OK to skip events while adding.
    assert!(matches!(
        view.add_event(make_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // OK to add the two events.
    view.add_event(make_event(hash, 0, 1, [1])).await.unwrap();
    view.add_event(make_event(hash, 1, 0, [2])).await.unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}
