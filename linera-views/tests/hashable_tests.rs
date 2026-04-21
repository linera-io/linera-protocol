// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_views::{
    common::HasherOutput,
    context::MemoryContext,
    hashable_wrapper::WrappedHashableContainerView,
    lazy_register_view::{HashedLazyRegisterView, LazyRegisterView},
    register_view::{HashedRegisterView, RegisterView},
    views::{HashableView, View},
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

// `HashedLazyRegisterView` must hash to the same value as `HashedRegisterView`
// over equivalent content. The chain-state view hash is protocol-visible, so
// swapping a `HashedRegisterView` field for a `HashedLazyRegisterView` field
// must not alter it.
#[tokio::test]
async fn hashed_register_and_hashed_lazy_register_have_same_hash() -> Result<()> {
    let context = MemoryContext::new_for_testing(());
    let eager = RegisterView::<_, u64>::load(context.clone()).await?;
    let hash_eager = eager.hash().await?;
    let lazy = LazyRegisterView::<_, u64>::load(context).await?;
    let hash_lazy = lazy.hash().await?;
    assert_eq!(hash_eager, hash_lazy);

    let context = MemoryContext::new_for_testing(());
    let mut eager = HashedRegisterView::<_, u64>::load(context.clone()).await?;
    *eager.get_mut() = 7;
    let hash_eager = eager.hash().await?;
    let mut lazy = HashedLazyRegisterView::<_, u64>::load(context).await?;
    lazy.set(7);
    let hash_lazy = lazy.hash().await?;
    assert_eq!(hash_eager, hash_lazy);
    Ok(())
}

#[tokio::test]
async fn lazy_register_view_evict() -> Result<()> {
    let context = MemoryContext::new_for_testing(());
    let mut view = LazyRegisterView::<_, u64>::load(context).await?;
    assert_eq!(*view.get().await?, 0);
    view.evict();
    assert_eq!(*view.get().await?, 0);
    view.set(42);
    view.evict();
    assert_eq!(*view.get().await?, 42);
    Ok(())
}
