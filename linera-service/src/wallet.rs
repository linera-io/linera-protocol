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

    println!("\n\x1b[1mWALLET CHAINS ({} total)\x1b[0m\n", total_chains);

    for chain_id in chain_ids {
        print_chain_paragraph(chain_id, wallet);
    }
}

fn print_chain_paragraph(chain_id: ChainId, wallet: &Wallet) {
    let Some(user_chain) = wallet.chains.get(&chain_id) else {
        panic!("Chain {} not found.", chain_id);
    };
    let title = if chain_id == wallet.genesis_admin_chain() {
        "Admin Chain".to_string()
    } else {
        match wallet
            .genesis_config()
            .chains
            .iter()
            .find(|description| description.id() == chain_id)
            .map(ChainDescription::origin)
        {
            Some(ChainOrigin::Root(0)) => "Admin Chain".to_string(),
            Some(ChainOrigin::Root(i)) => format!("Root Chain {i}"),
            _ => "Child Chain".to_string(),
        }
    };
    let default_marker = if Some(chain_id) == wallet.default {
        " [DEFAULT]"
    } else {
        ""
    };
    let owner_str = user_chain
        .owner
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "No owner key".to_string());

    // Print chain header in bold
    println!("\x1b[1m{}{}\x1b[0m", title, default_marker);
    println!("  Chain ID:     {chain_id}");
    println!("  Owner:        {owner_str}");
    println!("  Timestamp:    {}", user_chain.timestamp);
    println!("  Blocks:       {}", user_chain.next_block_height);
    if let Some(hash) = user_chain.block_hash {
        println!("  Latest Block: {}", hash);
    }
    if user_chain.pending_proposal.is_some() {
        println!("  Status:       ⚠ Pending proposal");
    }
    println!();
}
