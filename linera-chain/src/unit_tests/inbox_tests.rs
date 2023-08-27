// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::Timestamp,
};
use linera_execution::{Message, UserApplicationId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Dummy;
#[derive(Serialize, Deserialize)]
struct Dummy2;

impl BcsSignable for Dummy {}
impl BcsSignable for Dummy2 {}

fn make_event(
    certificate_hash: CryptoHash,
    height: u64,
    index: u32,
    message: impl Into<Vec<u8>>,
) -> Event {
    Event {
        certificate_hash,
        height: BlockHeight::from(height),
        index,
        authenticated_signer: None,
        is_skippable: true,
        timestamp: Timestamp::default(),
        message: Message::User {
            application_id: UserApplicationId::default(),
            bytes: message.into(),
        },
    }
}

fn make_unskippable_event(
    certificate_hash: CryptoHash,
    height: u64,
    index: u32,
    message: impl Into<Vec<u8>>,
) -> Event {
    let mut event = make_event(certificate_hash, height, index, message);
    event.is_skippable = false;
    event
}

#[tokio::test]
async fn test_inbox_add_then_remove_skippable() {
    let hash = CryptoHash::new(&Dummy);
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
        view.remove_event(&make_event(CryptoHash::new(&Dummy2), 0, 1, [1]))
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
async fn test_inbox_remove_then_add_skippable() {
    let hash = CryptoHash::new(&Dummy);
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
    view.remove_event(&make_event(hash, 1, 1, [3]))
        .await
        .unwrap();
    // Fail to add non-matching event.
    assert!(matches!(
        view.add_event(make_event(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to add non-matching event (hash).
    assert!(matches!(
        view.add_event(make_event(CryptoHash::new(&Dummy2), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // NOT OK to forget about previous consumed events while backfilling.
    assert!(matches!(
        view.add_event(make_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // OK to backfill the two consumed events, with one skippable event in the middle.
    view.add_event(make_event(hash, 0, 1, [1])).await.unwrap();
    // Cannot add an unskippable event that was visibly skipped already.
    assert!(matches!(
        view.add_event(make_unskippable_event(hash, 1, 0, [2]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    view.add_event(make_event(hash, 1, 0, [2])).await.unwrap();
    view.add_event(make_event(hash, 1, 1, [3])).await.unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_add_then_remove_unskippable() {
    let hash = CryptoHash::new(&Dummy);
    let mut view = InboxStateView::new().await;
    // Add one event.
    view.add_event(make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap();
    // Remove the same event
    view.remove_event(&make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap();
    // Fail to add an old event.
    assert!(matches!(
        view.add_event(make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Fail to remove an old event.
    assert!(matches!(
        view.remove_event(&make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Add two more events.
    view.add_event(make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    view.add_event(make_unskippable_event(hash, 1, 0, [2]))
        .await
        .unwrap();
    // Fail to remove non-matching event.
    assert!(matches!(
        view.remove_event(&make_unskippable_event(hash, 0, 1, [0]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to remove non-matching even (hash).
    assert!(matches!(
        view.remove_event(&make_unskippable_event(CryptoHash::new(&Dummy2), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to skip unskippable event.
    assert!(matches!(
        view.remove_event(&make_unskippable_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnskippableEvent {event }) if event == make_unskippable_event(hash, 0, 1, [1])));
    view.remove_event(&make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    view.remove_event(&make_unskippable_event(hash, 1, 0, [2]))
        .await
        .unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add_unskippable() {
    let hash = CryptoHash::new(&Dummy);
    let mut view = InboxStateView::new().await;
    // Remove one event by anticipation.
    view.remove_event(&make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap();
    // Add the same event
    view.add_event(make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap();
    // Fail to remove an old event.
    assert!(matches!(
        view.remove_event(&make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Fail to add an old event.
    assert!(matches!(
        view.add_event(make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    ));
    // Remove two more events.
    view.remove_event(&make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    view.remove_event(&make_unskippable_event(hash, 1, 1, [3]))
        .await
        .unwrap();
    // Fail to add non-matching event.
    assert!(matches!(
        view.add_event(make_unskippable_event(hash, 0, 1, [0]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to add non-matching event (hash).
    assert!(matches!(
        view.add_event(make_unskippable_event(CryptoHash::new(&Dummy2), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // NOT OK to forget about previous consumed events while backfilling.
    assert!(matches!(
        view.add_event(make_unskippable_event(hash, 1, 1, [3]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // OK to add the two events.
    view.add_event(make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    // Cannot add an unskippable event that was visibly skipped already.
    assert!(matches!(
        view.add_event(make_unskippable_event(hash, 1, 0, [2]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    view.add_event(make_unskippable_event(hash, 1, 1, [3]))
        .await
        .unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_add_then_remove_mixed() {
    let hash = CryptoHash::new(&Dummy);
    let mut view = InboxStateView::new().await;
    // Add two events.
    view.add_event(make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    view.add_event(make_event(hash, 1, 0, [2])).await.unwrap();
    // Fail to remove non-matching event.
    assert!(matches!(
        view.remove_event(&make_event(hash, 0, 1, [1])).await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to remove non-matching even (hash).
    assert!(matches!(
        view.remove_event(&make_unskippable_event(CryptoHash::new(&Dummy2), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    ));
    // Fail to skip unskippable event.
    assert!(matches!(
        view.remove_event(&make_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnskippableEvent {event }) if event == make_unskippable_event(hash, 0, 1, [1])));
    view.remove_event(&make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap();
    view.remove_event(&make_event(hash, 1, 0, [2]))
        .await
        .unwrap();
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}
