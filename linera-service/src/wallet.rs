// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{ChainDescription, ChainOrigin},
    identifiers::ChainId,
};
use linera_core::wallet;
pub use linera_core::wallet::PersistentWallet as Wallet;

struct ChainDetails {
    is_default: bool,
    is_admin: bool,
    origin: Option<ChainOrigin>,
    chain_id: ChainId,
    user_chain: wallet::Chain,
}

impl ChainDetails {
    fn new(chain_id: ChainId, wallet: &Wallet) -> Self {
        let Some(user_chain) = wallet.get(chain_id) else {
            panic!("Chain {} not found.", chain_id);
        };
        ChainDetails {
            is_default: Some(chain_id) == wallet.default_chain(),
            is_admin: chain_id == wallet.genesis_config().admin_chain_id(),
            chain_id,
            origin: wallet
                .genesis_config()
                .chains
                .iter()
                .find(|description| description.id() == chain_id)
                .map(ChainDescription::origin),
            user_chain,
        }
    }

    fn print_paragraph(&self) {
        println!("-----------------------");
        println!("{:<20}  {}", "Chain ID:", self.chain_id);

        let mut tags = Vec::new();
        if self.is_default {
            tags.push("DEFAULT");
        }
        if self.is_admin {
            tags.push("ADMIN");
        }
        if self.user_chain.is_follow_only() {
            tags.push("FOLLOW-ONLY");
        }
        if !tags.is_empty() {
            println!("{:<20}  {}", "Tags:", tags.join(", "));
        }

        match self.origin {
            Some(ChainOrigin::Root(_)) | None => {
                println!("{:<20}  -", "Parent chain:");
            }
            Some(ChainOrigin::Child { parent, .. }) => {
                println!("{:<20}  {parent}", "Parent chain:");
            }
        }

        if let Some(owner) = &self.user_chain.owner {
            println!("{:<20}  {owner}", "Default owner:");
        } else {
            println!("{:<20}  No owner key", "Default owner:");
        }

        println!("{:<20}  {}", "Timestamp:", self.user_chain.timestamp);
        println!("{:<20}  {}", "Blocks:", self.user_chain.next_block_height);

        if let Some(epoch) = self.user_chain.epoch {
            println!("{:<20}  {epoch}", "Epoch:");
        } else {
            println!("{:<20}  -", "Epoch:");
        }

        if let Some(hash) = self.user_chain.block_hash {
            println!("{:<20}  {hash}", "Latest block hash:");
        }

        if self.user_chain.pending_proposal.is_some() {
            println!("{:<20}  present", "Pending proposal:");
        }
    }
}

/// Extension methods for `Wallet` that provide display/presentation functionality.
pub trait WalletExt {
    fn pretty_print(&self, chain_ids: Vec<ChainId>);
}

impl WalletExt for Wallet {
    fn pretty_print(&self, chain_ids: Vec<ChainId>) {
        let chain_ids: Vec<_> = chain_ids.into_iter().collect();
        let total_chains = chain_ids.len();

        let plural_s = if total_chains == 1 { "" } else { "s" };
        tracing::info!("Found {total_chains} chain{plural_s}");

        let mut chains = chain_ids
            .into_iter()
            .map(|chain_id| ChainDetails::new(chain_id, self))
            .collect::<Vec<_>>();
        // Print first the default, then the admin chain, then other root chains, and finally the
        // child chains.
        chains.sort_unstable_by_key(|chain| {
            let root_id = chain
                .origin
                .and_then(|origin| origin.root())
                .unwrap_or(u32::MAX);
            let chain_id = chain.chain_id;
            (!chain.is_default, !chain.is_admin, root_id, chain_id)
        });
        for chain in chains {
            chain.print_paragraph();
        }
        println!("------------------------");
    }
}
