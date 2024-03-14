// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::SimpleObject;
use linera_sdk::{
    base::AccountOwner,
    views::{linera_views, MapView, RootView, ViewStorageContext},
};
use non_fungible::{Nft, TokenId};
use std::collections::BTreeSet;

/// The application state.
#[derive(RootView, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct NonFungibleToken {
    // Map from token ID to the NFT data
    pub nfts: MapView<TokenId, Nft>,
    // Map from owners to the set of NFT token IDs they own
    pub owned_token_ids: MapView<AccountOwner, BTreeSet<TokenId>>,
}
