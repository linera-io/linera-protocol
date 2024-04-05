// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use assert_matches::assert_matches;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Timestamp},
};
use linera_execution::{Message, MessageKind, UserApplicationId};

use super::*;

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
        grant: Amount::ZERO,
        refund_grant_to: None,
        kind: MessageKind::Simple,
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
    event.kind = MessageKind::Protected;
    event
}

#[tokio::test]
async fn test_inbox_add_then_remove_skippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Add one event.
    assert!(view.add_event(make_event(hash, 0, 0, [0])).await.unwrap());
    // Remove the same event
    assert!(view
        .remove_event(&make_event(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to add an old event.
    assert_matches!(
        view.add_event(make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to remove an old event.
    assert_matches!(
        view.remove_event(&make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Add two more events.
    assert!(view.add_event(make_event(hash, 0, 1, [1])).await.unwrap());
    assert!(view.add_event(make_event(hash, 1, 0, [2])).await.unwrap());
    // Fail to remove non-matching event.
    assert_matches!(
        view.remove_event(&make_event(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to remove non-matching event (hash).
    assert_matches!(
        view.remove_event(&make_event(CryptoHash::test_hash("2"), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // OK to skip events.
    assert!(view
        .remove_event(&make_event(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add_skippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Remove one event by anticipation.
    assert!(!view
        .remove_event(&make_event(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Add the same event
    assert!(!view.add_event(make_event(hash, 0, 0, [0])).await.unwrap());
    // Fail to remove an old event.
    assert_matches!(
        view.remove_event(&make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to add an old event.
    assert_matches!(
        view.add_event(make_event(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Remove two more events.
    assert!(!view
        .remove_event(&make_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(!view
        .remove_event(&make_event(hash, 1, 1, [3]))
        .await
        .unwrap());
    // Fail to add non-matching event.
    assert_matches!(
        view.add_event(make_event(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to add non-matching event (hash).
    assert_matches!(
        view.add_event(make_event(CryptoHash::test_hash("2"), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // NOT OK to forget about previous consumed events while backfilling.
    assert_matches!(
        view.add_event(make_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // OK to backfill the two consumed events, with one skippable event in the middle.
    assert!(!view.add_event(make_event(hash, 0, 1, [1])).await.unwrap());
    // Cannot add an unskippable event that was visibly skipped already.
    assert_matches!(
        view.add_event(make_unskippable_event(hash, 1, 0, [2]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    assert!(!view.add_event(make_event(hash, 1, 0, [2])).await.unwrap());
    assert!(!view.add_event(make_event(hash, 1, 1, [3])).await.unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_add_then_remove_unskippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Add one event.
    assert!(view
        .add_event(make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Remove the same event
    assert!(view
        .remove_event(&make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to add an old event.
    assert_matches!(
        view.add_event(make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to remove an old event.
    assert_matches!(
        view.remove_event(&make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Add two more events.
    assert!(view
        .add_event(make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view
        .add_event(make_unskippable_event(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Fail to remove non-matching event.
    assert_matches!(
        view.remove_event(&make_unskippable_event(hash, 0, 1, [0]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to remove non-matching event (hash).
    assert_matches!(
        view.remove_event(&make_unskippable_event(
            CryptoHash::test_hash("2"),
            0,
            1,
            [1]
        ))
        .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to skip unskippable event.
    assert_matches!(
        view.remove_event(&make_unskippable_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnskippableEvent {event })
        if event == make_unskippable_event(hash, 0, 1, [1])
    );
    assert!(view
        .remove_event(&make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view
        .remove_event(&make_unskippable_event(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add_unskippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Remove one event by anticipation.
    assert!(!view
        .remove_event(&make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Add the same event
    assert!(!view
        .add_event(make_unskippable_event(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to remove an old event.
    assert_matches!(
        view.remove_event(&make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to add an old event.
    assert_matches!(
        view.add_event(make_unskippable_event(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Remove two more events.
    assert!(!view
        .remove_event(&make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(!view
        .remove_event(&make_unskippable_event(hash, 1, 1, [3]))
        .await
        .unwrap());
    // Fail to add non-matching event.
    assert_matches!(
        view.add_event(make_unskippable_event(hash, 0, 1, [0]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to add non-matching event (hash).
    assert_matches!(
        view.add_event(make_unskippable_event(
            CryptoHash::test_hash("2"),
            0,
            1,
            [1]
        ))
        .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // NOT OK to forget about previous consumed events while backfilling.
    assert_matches!(
        view.add_event(make_unskippable_event(hash, 1, 1, [3]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // OK to add the two events.
    assert!(!view
        .add_event(make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    // Cannot add an unskippable event that was visibly skipped already.
    assert_matches!(
        view.add_event(make_unskippable_event(hash, 1, 0, [2]))
            .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    assert!(!view
        .add_event(make_unskippable_event(hash, 1, 1, [3]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}

#[tokio::test]
async fn test_inbox_add_then_remove_mixed() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Add two events.
    assert!(view
        .add_event(make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view.add_event(make_event(hash, 1, 0, [2])).await.unwrap());
    // Fail to remove non-matching event (skippability).
    assert_matches!(
        view.remove_event(&make_event(hash, 0, 1, [1])).await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to remove non-matching event (hash).
    assert_matches!(
        view.remove_event(&make_unskippable_event(
            CryptoHash::test_hash("2"),
            0,
            1,
            [1]
        ))
        .await,
        Err(InboxError::UnexpectedEvent { .. })
    );
    // Fail to skip unskippable event.
    assert_matches!(
        view.remove_event(&make_event(hash, 1, 0, [2])).await,
        Err(InboxError::UnskippableEvent { event })
        if event == make_unskippable_event(hash, 0, 1, [1])
    );
    assert!(view
        .remove_event(&make_unskippable_event(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view
        .remove_event(&make_event(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_events.count(), 0);
    assert_eq!(view.removed_events.count(), 0);
}
