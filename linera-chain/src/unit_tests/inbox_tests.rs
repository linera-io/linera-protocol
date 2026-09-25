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
    assert!(view
        .add_bundle(make_bundle(hash, 0, 0, [0]), false)
        .await
        .unwrap());
    // Remove the same bundle
    assert!(view
        .remove_bundle(&make_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 0, 0, [0]), false).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to remove an old bundle.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Add two more bundles.
    assert!(view
        .add_bundle(make_bundle(hash, 0, 1, [1]), false)
        .await
        .unwrap());
    assert!(view
        .add_bundle(make_bundle(hash, 1, 0, [2]), false)
        .await
        .unwrap());
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
    assert!(!view
        .add_bundle(make_bundle(hash, 0, 0, [0]), false)
        .await
        .unwrap());
    // Fail to remove an old bundle.
    assert_matches!(
        view.remove_bundle(&make_bundle(hash, 0, 0, [0])).await,
        Err(InboxError::IncorrectOrder { .. })
    );
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 0, 0, [0]), false).await,
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
        view.add_bundle(make_bundle(hash, 0, 1, [0]), false).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to add non-matching bundle (hash).
    assert_matches!(
        view.add_bundle(make_bundle(CryptoHash::test_hash("2"), 0, 1, [1]), false)
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // NOT OK to forget about previous consumed bundles while backfilling.
    assert_matches!(
        view.add_bundle(make_bundle(hash, 1, 0, [2]), false).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // OK to backfill the two consumed bundles, with one skippable bundle in the middle.
    assert!(!view
        .add_bundle(make_bundle(hash, 0, 1, [1]), false)
        .await
        .unwrap());
    // Cannot add an unskippable bundle that was visibly skipped already.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 1, 0, [2]), false)
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    assert!(!view
        .add_bundle(make_bundle(hash, 1, 0, [2]), false)
        .await
        .unwrap());
    assert!(!view
        .add_bundle(make_bundle(hash, 1, 1, [3]), false)
        .await
        .unwrap());
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
        .add_bundle(make_unskippable_bundle(hash, 0, 0, [0]), false)
        .await
        .unwrap());
    // Remove the same bundle
    assert!(view
        .remove_bundle(&make_unskippable_bundle(hash, 0, 0, [0]))
        .await
        .unwrap());
    // Fail to add an old bundle.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 0, 0, [0]), false)
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
        .add_bundle(make_unskippable_bundle(hash, 0, 1, [1]), false)
        .await
        .unwrap());
    assert!(view
        .add_bundle(make_unskippable_bundle(hash, 1, 0, [2]), false)
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
        .add_bundle(make_unskippable_bundle(hash, 0, 0, [0]), false)
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
        view.add_bundle(make_unskippable_bundle(hash, 0, 0, [0]), false)
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
        view.add_bundle(make_unskippable_bundle(hash, 0, 1, [0]), false)
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // Fail to add non-matching bundle (hash).
    assert_matches!(
        view.add_bundle(
            make_unskippable_bundle(CryptoHash::test_hash("2"), 0, 1, [1]),
            false
        )
        .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // NOT OK to forget about previous consumed bundles while backfilling.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 1, 1, [3]), false)
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // OK to add the two bundles.
    assert!(!view
        .add_bundle(make_unskippable_bundle(hash, 0, 1, [1]), false)
        .await
        .unwrap());
    // Cannot add an unskippable bundle that was visibly skipped already.
    assert_matches!(
        view.add_bundle(make_unskippable_bundle(hash, 1, 0, [2]), false)
            .await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    assert!(!view
        .add_bundle(make_unskippable_bundle(hash, 1, 1, [3]), false)
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
        .add_bundle(make_unskippable_bundle(hash, 0, 1, [1]), false)
        .await
        .unwrap());
    assert!(view
        .add_bundle(make_bundle(hash, 1, 0, [2]), false)
        .await
        .unwrap());
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

/// Sparse catch-up: a chain that consumed bundles by anticipation accepts a later bundle
/// directly, dropping the anticipated entries the sparse sender will never deliver.
#[tokio::test]
async fn test_inbox_sparse_catchup_drops_consumed_anticipations() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    // Consume heights 1 and 2 by anticipation, as a validator current on this chain but
    // behind on the sender does. `next_cursor_to_add` stays at zero throughout.
    view.remove_bundle(&make_bundle(hash, 1, 0, [1]))
        .await
        .unwrap();
    view.remove_bundle(&make_bundle(hash, 2, 0, [2]))
        .await
        .unwrap();
    assert_eq!(view.removed_bundles.count(), 2);
    assert_eq!(
        view.next_block_height_to_receive().unwrap(),
        BlockHeight::from(0)
    );
    // Without the flag the sparse delivery is rejected as a mismatch.
    let mut strict = view.clone_unchecked().unwrap();
    assert_matches!(
        strict.add_bundle(make_bundle(hash, 3, 0, [3]), false).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
    // With it, the stale anticipations are dropped and the bundle is queued.
    assert!(view
        .add_bundle(make_bundle(hash, 3, 0, [3]), true)
        .await
        .unwrap());
    assert_eq!(view.removed_bundles.count(), 0);
    assert_eq!(view.added_bundles.count(), 1);
}

/// Sparse catch-up must not disturb an exact anticipation match: a bundle that *does*
/// correspond to the front of the queue is still reconciled against it, not dropped.
#[tokio::test]
async fn test_inbox_sparse_catchup_still_matches_exact_anticipation() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    view.remove_bundle(&make_bundle(hash, 1, 0, [1]))
        .await
        .unwrap();
    // Delivered bundle matches the anticipated one: consumed from the queue, not added.
    assert!(!view
        .add_bundle(make_bundle(hash, 1, 0, [1]), true)
        .await
        .unwrap());
    assert_eq!(view.removed_bundles.count(), 0);
    assert_eq!(view.added_bundles.count(), 0);
    // A mismatching bundle at the same cursor is still rejected under the flag.
    let mut view = InboxStateView::new().await;
    view.remove_bundle(&make_bundle(hash, 1, 0, [1]))
        .await
        .unwrap();
    assert_matches!(
        view.add_bundle(make_bundle(hash, 1, 0, [9]), true).await,
        Err(InboxError::UnexpectedBundle { .. })
    );
}

/// Sparse catch-up does not relax the ordering rule itself: a bundle below
/// `next_cursor_to_add` is still out of order, flag or not.
#[tokio::test]
async fn test_inbox_sparse_catchup_still_rejects_out_of_order() {
    let hash = CryptoHash::test_hash("1");
    let mut view = InboxStateView::new().await;
    assert!(view
        .add_bundle(make_bundle(hash, 5, 0, [5]), true)
        .await
        .unwrap());
    assert_matches!(
        view.add_bundle(make_bundle(hash, 4, 0, [4]), true).await,
        Err(InboxError::IncorrectOrder { .. })
    );
}
