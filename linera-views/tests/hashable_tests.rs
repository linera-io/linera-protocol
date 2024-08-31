// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_views::{
    common::HasherOutput,
    context::create_test_memory_context,
    hashable_wrapper::WrappedHashableContainerView,
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
    let context = create_test_memory_context();
    let test = TestType::load(context).await?;
    let hash1 = test.inner.hash().await?;
    let hash2 = test.wrap.hash().await?;
    assert_eq!(hash1, hash2);
    Ok(())
}

#[tokio::test]
async fn check_hashable_hash() -> Result<()> {
    let context = create_test_memory_context();
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
