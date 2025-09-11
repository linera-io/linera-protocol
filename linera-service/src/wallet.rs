// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, Color, ContentArrangement,
    Table,
};
use linera_base::{
    data_types::{ChainDescription, ChainOrigin},
    identifiers::ChainId,
};
pub use linera_client::wallet::*;

pub fn pretty_print(wallet: &Wallet, chain_ids: impl IntoIterator<Item = ChainId>) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Chain ID").add_attribute(Attribute::Bold),
            Cell::new("Your Owner Account").add_attribute(Attribute::Bold),
            Cell::new("Latest Block").add_attribute(Attribute::Bold),
        ]);
    let root_chains = wallet
        .genesis_config()
        .chains
        .iter()
        .map(|description| (description.id(), description))
        .collect::<BTreeMap<_, _>>();
    for chain_id in chain_ids {
        let Some(user_chain) = wallet.chains.get(&chain_id) else {
            panic!("Chain {} not found.", chain_id);
        };
        let description = root_chains.get(&chain_id).copied();
        update_table_with_chain(
            &mut table,
            chain_id,
            description,
            user_chain,
            Some(chain_id) == wallet.default,
        );
    }
    println!("{}", table);
}

fn update_table_with_chain(
    table: &mut Table,
    chain_id: ChainId,
    description: Option<&ChainDescription>,
    user_chain: &UserChain,
    is_default_chain: bool,
) {
    let origin = match description.map(ChainDescription::origin) {
        Some(ChainOrigin::Root(0)) => "Admin (Root 0)".to_string(),
        Some(ChainOrigin::Root(i)) => format!("Root {i}"),
        _ => "Child".to_string(),
    };
    let color = is_default_chain
        .then_some(Color::Green)
        .unwrap_or(Color::Reset);
    let chain_id_cell = Cell::new(format!("{chain_id}\n{origin}")).fg(color);
    let account_cell = if let Some(owner) = &user_chain.owner {
        Cell::new(format!("{owner}"))
    } else {
        Cell::new("-")
    };
    let timestamp = user_chain.timestamp;
    let block_cell = if let (Some(hash), Ok(height)) = (
        user_chain.block_hash,
        user_chain.next_block_height.try_sub_one(),
    ) {
        Cell::new(format!(
            "Hash:      {hash}\n\
             Height:    {height}\n\
             Timestamp: {timestamp}"
        ))
    } else {
        Cell::new(format!(
            "-\n\
             Chain created: {timestamp}"
        ))
    };
    table.add_row(vec![chain_id_cell, account_cell, block_cell]);
}
