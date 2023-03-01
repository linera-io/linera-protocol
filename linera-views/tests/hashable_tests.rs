// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use linera_views_derive::{
    CryptoHashRootView, CryptoHashView, GraphQLView, RootView,
};
use linera_views::common::Context;
use linera_views::common::HashOutput;
use linera_views::views::View;
use linera_views::register_view::RegisterView;
use linera_views::hashable_wrapper::WrappedHashableContainerView;
use linera_views::memory::make_test_context;
use linera_views::views::HashableView;

#[derive(CryptoHashRootView)]
struct TestType<C> {
    pub inner: RegisterView<C,String>,
    pub wrap: WrappedHashableContainerView<C, RegisterView<C,String>, HashOutput>,
}

// TODO: Implement the same for CryptoHash
#[tokio::test]
async fn check_hashable_container_hash() {
    let context = make_test_context().await;
    let test = TestType::load(context).await.unwrap();
    let hash1 = test.inner.hash().await.unwrap();
    let hash2 = test.wrap.hash().await.unwrap();
    assert_eq!(hash1,hash2);
}
