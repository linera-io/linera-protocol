// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use assert_matches::assert_matches;
use linera_base::{crypto::CryptoHash, data_types::Timestamp, identifiers::ApplicationId};
use linera_execution::{Message, MessageKind};

use super::*;
use crate::test::MessageTestExt as _;

fn make_bundle(
    certificate_hash: CryptoHash,
    height: u64,
    index: u32,
    message: impl Into<Vec<u8>>,
) -> MessageBundle {
    let message = Message::User {
        application_id: ApplicationId::default(),
        bytes: message.into(),
    };
    MessageBundle {
        certificate_hash,
        height: BlockHeight::from(height),
        timestamp: Timestamp::default(),
        transaction_index: index,
        messages: vec![message.to_posted(index, MessageKind::Simple)],
    }
}

fn make_unskippable_bundle(
    certificate_hash: CryptoHash,
    height: u64,
    index: u32,
    message: impl Into<Vec<u8>>,
) -> MessageBundle {
    let mut bundle = make_bundle(certificate_hash, height, index, message);
    bundle.messages[0].kind = MessageKind::Protected;
    bundle
}

#[tokio::test]
async fn test_inbox_add_then_remove_skippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Add one bundle.
    assert!(view.add_bundle(make_bundle(hash, 0, 0, [0])).await.unwrap());
    // Remove the same bundle
    assert!(view
        .remove_bundle(&make_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to remove an old bundle.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Add two more bundles.
    assert!(view.add_bundle(make_bundle(hash, 0, 1, [1])).await.unwrap());
    assert!(view.add_bundle(make_bundle(hash, 1, 0, [2])).await.unwrap());
    // Fail to remove non-matching bundle.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to remove non-matching bundle (hash).
    assert_matches!(
        view.remove_bundle(&make_bundle(CryptoHash::test_hash("2"), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // OK to skip bundles.
    assert!(view
        .remove_bundle(&make_bundle(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_bundles.count(), 0);
    assert_eq!(view.removed_bundles.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add_skippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Remove one bundle by anticipation.
    assert!(!view
        .remove_bundle(&make_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Add the same bundle
    assert!(!view.add_bundle(make_bundle(hash, 0, 0, [0])).await.unwrap());
    // Fail to remove an old bundle.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Remove two more bundles.
    assert!(!view
        .remove_bundle(&make_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(!view
        .remove_bundle(&make_bundle(hash, 1, 1, [3]))
        .await
        .unwrap());
    // Fail to add non-matching bundle.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 0, 1, [0])).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to add non-matching bundle (hash).
    assert_matches!(
        view.add_bundle(make_bundle(CryptoHash::test_hash("2"), 0, 1, [1]))
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // NOT OK to forget about previous consumed bundles while backfilling.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 1, 0, [2])).await,
        Err(InboxError::GapDetected { .. })
    );
    // OK to backfill the two consumed bundles, with one skippable bundle in the middle.
    assert!(!view.add_bundle(make_bundle(hash, 0, 1, [1])).await.unwrap());
    // Cannot add an unskippable bundle that was visibly skipped already.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 1, 0, [2]))
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    assert!(!view.add_bundle(make_bundle(hash, 1, 0, [2])).await.unwrap());
    assert!(!view.add_bundle(make_bundle(hash, 1, 1, [3])).await.unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_bundles.count(), 0);
    assert_eq!(view.removed_bundles.count(), 0);
}

#[tokio::test]
async fn test_inbox_add_then_remove_unskippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Add one bundle.
    assert!(view
        .add_bundle(make_unskippable_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Remove the same bundle
    assert!(view
        .remove_bundle(&make_unskippable_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to remove an old bundle.
    assert_matches!(
        view.remove_bundle(&make_unskippable_bundle(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Add two more bundles.
    assert!(view
        .add_bundle(make_unskippable_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view
        .add_bundle(make_unskippable_bundle(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Fail to remove non-matching bundle.
    assert_matches!(
        view.remove_bundle(&make_unskippable_bundle(hash, 0, 1, [0]))
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to remove non-matching bundle (hash).
    assert_matches!(
        view.remove_bundle(&make_unskippable_bundle(
            CryptoHash::test_hash("2"),
            0,
            1,
            [1]
        ))
        .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to skip unskippable bundle.
    assert_matches!(
        view.remove_bundle(&make_unskippable_bundle(hash, 1, 0, [2])).await,
        Err(InboxError::UnskippableBundle { bundle })
        if bundle == make_unskippable_bundle(hash, 0, 1, [1])
    );
    assert!(view
        .remove_bundle(&make_unskippable_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view
        .remove_bundle(&make_unskippable_bundle(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_bundles.count(), 0);
    assert_eq!(view.removed_bundles.count(), 0);
}

#[tokio::test]
async fn test_inbox_remove_then_add_unskippable() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Remove one bundle by anticipation.
    assert!(!view
        .remove_bundle(&make_unskippable_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Add the same bundle
    assert!(!view
        .add_bundle(make_unskippable_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to remove an old bundle.
    assert_matches!(
        view.remove_bundle(&make_unskippable_bundle(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 0, 0, [0]))
            .await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Remove two more bundles.
    assert!(!view
        .remove_bundle(&make_unskippable_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(!view
        .remove_bundle(&make_unskippable_bundle(hash, 1, 1, [3]))
        .await
        .unwrap());
    // Fail to add non-matching bundle.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 0, 1, [0]))
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to add non-matching bundle (hash).
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(
            CryptoHash::test_hash("2"),
            0,
            1,
            [1]
        ))
        .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // NOT OK to forget about previous consumed bundles while backfilling.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 1, 1, [3]))
            .await,
        Err(InboxError::GapDetected { .. })
    );
    // OK to add the two bundles.
    assert!(!view
        .add_bundle(make_unskippable_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    // Cannot add an unskippable bundle that was visibly skipped already.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 1, 0, [2]))
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    assert!(!view
        .add_bundle(make_unskippable_bundle(hash, 1, 1, [3]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_bundles.count(), 0);
    assert_eq!(view.removed_bundles.count(), 0);
}

#[tokio::test]
async fn test_inbox_add_then_remove_mixed() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Add two bundles.
    assert!(view
        .add_bundle(make_unskippable_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view.add_bundle(make_bundle(hash, 1, 0, [2])).await.unwrap());
    // Fail to remove non-matching bundle (skippability).
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 0, 1, [1])).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to remove non-matching bundle (hash).
    assert_matches!(
        view.remove_bundle(&make_unskippable_bundle(
            CryptoHash::test_hash("2"),
            0,
            1,
            [1]
        ))
        .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to skip unskippable bundle.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 1, 0, [2])).await,
        Err(InboxError::UnskippableBundle { bundle })
        if bundle == make_unskippable_bundle(hash, 0, 1, [1])
    );
    assert!(view
        .remove_bundle(&make_unskippable_bundle(hash, 0, 1, [1]))
        .await
        .unwrap());
    assert!(view
        .remove_bundle(&make_bundle(hash, 1, 0, [2]))
        .await
        .unwrap());
    // Inbox is empty again.
    assert_eq!(view.added_bundles.count(), 0);
    assert_eq!(view.removed_bundles.count(), 0);
}

#[tokio::test]
async fn test_inbox_gap_detected() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Remove bundles at heights 0 and 2 by anticipation.
    assert!(!view
        .remove_bundle(&make_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    assert!(!view
        .remove_bundle(&make_bundle(hash, 2, 0, [2]))
        .await
        .unwrap());
    // Adding height 0 reconciles with removed_bundles front.
    assert!(!view.add_bundle(make_bundle(hash, 0, 0, [0])).await.unwrap());
    // Adding height 2 directly (skipping height 1, which was never removed) is fine.
    assert!(!view.add_bundle(make_bundle(hash, 2, 0, [2])).await.unwrap());
    assert_eq!(view.removed_bundles.count(), 0);

    // Now test the gap case: remove height 5, then try to add height 7 — gap detected.
    assert!(!view
        .remove_bundle(&make_bundle(hash, 5, 0, [5]))
        .await
        .unwrap());
    assert_matches!(
        view.add_bundle(make_bundle(hash, 7, 0, [7])).await,
        Err(InboxError::GapDetected {
            expected_height,
            actual_height,
        }) if expected_height == BlockHeight::from(5) && actual_height == BlockHeight::from(7)
    );
    // removed_bundles still has height 5 (inbox unchanged after gap error).
    assert_eq!(view.removed_bundles.count(), 1);
}
