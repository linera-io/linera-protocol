// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use async_graphql::SimpleObject;
use gen_nft::{Nft, TokenId};
use linera_sdk::{
    base::AccountOwner,
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};

/// The application state.
#[derive(RootView, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct GenNftState {
    // Map from token ID to the NFT data
    pub nfts: MapView<TokenId, Nft>,
    // Map from owners to the set of NFT token IDs they own
    pub owned_token_ids: MapView<AccountOwner, BTreeSet<TokenId>>,
    // Counter of NFTs minted in this chain, used for hash uniqueness
    pub num_minted_nfts: RegisterView<u64>,
}
