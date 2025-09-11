// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{ChainDescription, ChainOrigin},
    identifiers::ChainId,
};
pub use linera_client::wallet::*;

pub fn pretty_print(wallet: &Wallet, chain_ids: impl IntoIterator<Item = ChainId>) {
    let chain_ids: Vec<_> = chain_ids.into_iter().collect();
    let total_chains = chain_ids.len();

    if total_chains == 0 {
        println!("No chains in wallet.");
        return;
    }

    println!("\n\x1b[1mWALLET CHAINS ({} total)\x1b[0m", total_chains);

    let mut chains = chain_ids
        .into_iter()
        .map(|chain_id| ChainDetails::new(chain_id, wallet))
        .collect::<Vec<_>>();
    // Print first the default, then the admin chain, then other root chains, and finally the
    // child chains.
    chains.sort_unstable_by_key(|chain| {
        let root_id = chain
            .origin
            .and_then(|origin| origin.root())
            .unwrap_or(u32::MAX);
        let chain_id = chain.user_chain.chain_id;
        (!chain.is_default, !chain.is_admin, root_id, chain_id)
    });
    for chain in chains {
        println!();
        chain.print_paragraph();
    }
}

struct ChainDetails<'a> {
    is_default: bool,
    is_admin: bool,
    origin: Option<ChainOrigin>,
    user_chain: &'a UserChain,
}

impl<'a> ChainDetails<'a> {
    fn new(chain_id: ChainId, wallet: &'a Wallet) -> Self {
        let Some(user_chain) = wallet.chains.get(&chain_id) else {
            panic!("Chain {} not found.", chain_id);
        };
        ChainDetails {
            is_default: Some(chain_id) == wallet.default,
            is_admin: chain_id == wallet.genesis_admin_chain(),
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
        let title = if self.is_admin {
            "Admin Chain".to_string()
        } else {
            match self.origin {
                Some(ChainOrigin::Root(i)) => format!("Root Chain {i}"),
                _ => "Child Chain".to_string(),
            }
        };
        let default_marker = if self.is_default { " [DEFAULT]" } else { "" };

        // Print chain header in bold
        println!("\x1b[1m{}{}\x1b[0m", title, default_marker);
        println!("  Chain ID:     {}", self.user_chain.chain_id);
        if let Some(owner) = &self.user_chain.owner {
            println!("  Owner:        {owner}");
        } else {
            println!("  Owner:        No owner key");
        }
        println!("  Timestamp:    {}", self.user_chain.timestamp);
        println!("  Blocks:       {}", self.user_chain.next_block_height);
        if let Some(epoch) = self.user_chain.epoch {
            println!("  Epoch:        {epoch}");
        } else {
            println!("  Epoch:        -");
        }
        if let Some(hash) = self.user_chain.block_hash {
            println!("  Latest Block: {}", hash);
        }
        if self.user_chain.pending_proposal.is_some() {
            println!("  Status:       ⚠ Pending proposal");
        }
    }
}
