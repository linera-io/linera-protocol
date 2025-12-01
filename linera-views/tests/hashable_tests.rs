// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_base::crypto::CryptoHash;
use linera_views::{
    common::HasherOutput,
    context::MemoryContext,
    hashable_wrapper::WrappedHashableContainerView,
    historical_hash_wrapper::HistoricallyHashableView,
    register_view::{HashedRegisterView, RegisterView},
    views::{HashableView, RootView, View},
};
use linera_views_derive::CryptoHashRootView;

#[derive(CryptoHashRootView)]
struct TestType<C> {
    pub inner: RegisterView<C, String>,
    pub wrap: WrappedHashableContainerView<C, RegisterView<C, String>, HasherOutput>,
}

// TODO(#560): Implement the same for CryptoHash
#[tokio::test]
async fn check_hashable_container_hash() -> Result<()> {
    let context = MemoryContext::new_for_testing(());
    let test = TestType::load(context).await?;
    let hash1 = test.inner.hash().await?;
    let hash2 = test.wrap.hash().await?;
    assert_eq!(hash1, hash2);
    Ok(())
}

#[tokio::test]
async fn check_hashable_hash() -> Result<()> {
    let context = MemoryContext::new_for_testing(());
    let mut view = HashedRegisterView::<_, u32>::load(context).await?;
    let hash0 = view.hash().await?;
    let val = view.get_mut();
    *val = 32;
    let hash32 = view.hash().await?;
    assert_ne!(hash0, hash32);
    view.clear();
    assert_eq!(hash0, view.hash().await?);
    Ok(())
}

#[derive(View)]
struct TestInnerType<C> {
    pub field1: RegisterView<C, u32>,
    pub field2: RegisterView<C, u32>,
    pub field3: RegisterView<C, Option<CryptoHash>>,
}

#[derive(RootView)]
struct TestType2<C> {
    pub field1: RegisterView<C, u32>,
    pub field2: HistoricallyHashableView<C, TestInnerType<C>>,
}

#[tokio::test]
async fn check_hashable_not_overwriting_field() -> Result<()> {
    let context = MemoryContext::new_for_testing(());

    // Let's store some data in the view.
    let mut test = TestType2::load(context.clone()).await?;
    let hash1 = CryptoHash::from([0u8; 32]);
    test.field2.field1.set(1);
    test.field2.field2.set(2);
    test.field2.field3.set(Some(hash1));
    // Pre-#4983, this would overwrite the contents of test.field2.field3, because of a
    // base key collision.
    test.save().await?;

    // Let's reload the view.
    let test = TestType2::load(context).await?;
    let stored_hash = test.field2.field3.get();
    // Assert that the data has not been overwritten.
    assert_eq!(stored_hash, &Some(hash1));

    Ok(())
}
