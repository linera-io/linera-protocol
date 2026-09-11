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
        messages: vec![message.to_posted(MessageKind::Simple)],
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
        Err(InboxError::UnexpectedBundle { .. })
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
        Err(InboxError::UnexpectedBundle { .. })
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
async fn test_inbox_restore_from_checkpoint() {
    let hash = CryptoHash::test_hash("1");
    let cutoff = Cursor {
        height: BlockHeight::from(2),
        index: 0,
    };

    // Fresh inbox: all cursors get seeded to the cutoff.
    {
        let mut view = InboxStateView::new().await;
        view.restore_from_checkpoint(cutoff).await.unwrap();
        assert_eq!(*view.restored_cursor.get(), cutoff);
        assert_eq!(*view.next_cursor_to_remove.get(), cutoff);
        assert_eq!(*view.next_cursor_to_add.get(), cutoff);
        assert_eq!(view.added_bundles.count(), 0);
        assert_eq!(view.removed_bundles.count(), 0);
    }

    // Inbox with deliveries straddling the cutoff: bundles strictly below the
    // cutoff are dropped, those at or above it are kept, `next_cursor_to_add`
    // stays at the higher pre-restore value, and the anticipated-remove queue
    // is cleared.
    {
        let mut view = InboxStateView::new().await;
        view.add_bundle(make_bundle(hash, 0, 0, [0])).await.unwrap();
        view.add_bundle(make_bundle(hash, 1, 0, [1])).await.unwrap();
        view.add_bundle(make_bundle(hash, 2, 0, [2])).await.unwrap();
        view.add_bundle(make_bundle(hash, 3, 0, [3])).await.unwrap();
        let pre_restore_next_add = *view.next_cursor_to_add.get();
        assert!(pre_restore_next_add > cutoff);

        view.restore_from_checkpoint(cutoff).await.unwrap();

        assert_eq!(*view.restored_cursor.get(), cutoff);
        assert_eq!(*view.next_cursor_to_remove.get(), cutoff);
        assert_eq!(*view.next_cursor_to_add.get(), pre_restore_next_add);
        let remaining = view.added_bundles.elements().await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].cursor().height, BlockHeight::from(2));
        assert_eq!(remaining[1].cursor().height, BlockHeight::from(3));
    }

    // Anticipated removes from a now-rolled-back pre-restore block are cleared,
    // even when they sit above the cutoff.
    {
        let mut view = InboxStateView::new().await;
        view.remove_bundle(&make_bundle(hash, 3, 0, [3]))
            .await
            .unwrap();
        assert_eq!(view.removed_bundles.count(), 1);
        view.restore_from_checkpoint(cutoff).await.unwrap();
        assert_eq!(view.removed_bundles.count(), 0);
    }

    // Attempting to restore from an earlier checkpoint than one we've already
    // bootstrapped from is a dispatch-level invariant violation.
    {
        let mut view = InboxStateView::new().await;
        let later = Cursor {
            height: BlockHeight::from(5),
            index: 0,
        };
        view.restore_from_checkpoint(later).await.unwrap();
        assert_matches!(
            view.restore_from_checkpoint(cutoff).await,
            Err(ChainError::InternalError(_))
        );
    }
}

/// A push with no predecessor marks the lane as settled below its first bundle:
/// consuming a settled bundle that was never delivered here reconciles as a no-op — even
/// past an already-added later bundle, where it used to fail as `UnexpectedBundle` and
/// wedge the lane for good.
#[tokio::test]
async fn test_inbox_settled_lane_add_then_remove() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // The sender's checkpoint settled the lane below height 8; the height-5 bundle was
    // never delivered here.
    view.note_sender_pruned_below(make_bundle(hash, 8, 0, [8]).cursor())
        .await
        .unwrap();
    assert!(view.add_bundle(make_bundle(hash, 8, 0, [8])).await.unwrap());
    // A certified block consumes the missed height-5 bundle: ignored, and not
    // anticipated — the sender will never push it.
    assert!(!view
        .remove_bundle(&make_bundle(hash, 5, 0, [5]))
        .await
        .unwrap());
    assert_eq!(view.removed_bundles.count(), 0);
    // The added bundle is untouched and consumes normally.
    assert!(view
        .remove_bundle(&make_bundle(hash, 8, 0, [8]))
        .await
        .unwrap());
    assert_eq!(view.added_bundles.count(), 0);
}

/// The consume-first ordering: a settled bundle was consumed by anticipation before the
/// no-predecessor push arrived. The push must drop the stale anticipated entry — the
/// push it awaits will never come — instead of failing to reconcile against it.
#[tokio::test]
async fn test_inbox_settled_lane_remove_then_add() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // A certified block consumes the height-5 bundle before anything was added.
    assert!(!view
        .remove_bundle(&make_bundle(hash, 5, 0, [5]))
        .await
        .unwrap());
    assert_eq!(view.removed_bundles.count(), 1);
    // The no-predecessor push at height 8 arrives: the anticipated entry is dropped and
    // the new bundle is added.
    view.note_sender_pruned_below(make_bundle(hash, 8, 0, [8]).cursor())
        .await
        .unwrap();
    assert_eq!(view.removed_bundles.count(), 0);
    assert!(view.add_bundle(make_bundle(hash, 8, 0, [8])).await.unwrap());
    // A late re-delivery of a settled bundle is dropped, not an error.
    assert!(!view.add_bundle(make_bundle(hash, 5, 0, [5])).await.unwrap());
}

/// Delivered bundles stay live below the settled point: a pending bundle in
/// `added_bundles` — e.g. a `CheckpointAck` sent by a checkpoint block itself, which no
/// anchor tracks — must still reconcile normally, and corruption at its cursor is still
/// caught.
#[tokio::test]
async fn test_inbox_settled_lane_keeps_pending_bundles() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // The height-5 bundle was delivered before the no-predecessor push at height 8.
    assert!(view.add_bundle(make_bundle(hash, 5, 0, [5])).await.unwrap());
    view.note_sender_pruned_below(make_bundle(hash, 8, 0, [8]).cursor())
        .await
        .unwrap();
    assert!(view.add_bundle(make_bundle(hash, 8, 0, [8])).await.unwrap());
    assert_eq!(view.added_bundles.count(), 2);
    // Consuming a different bundle at the pending bundle's cursor still fails.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 5, 0, [9])).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // The pending bundle itself consumes normally.
    assert!(view
        .remove_bundle(&make_bundle(hash, 5, 0, [5]))
        .await
        .unwrap());
    assert!(view
        .remove_bundle(&make_bundle(hash, 8, 0, [8]))
        .await
        .unwrap());
    assert_eq!(view.added_bundles.count(), 0);
}
