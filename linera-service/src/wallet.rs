// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, Color, ContentArrangement,
    Table,
};
use linera_base::identifiers::ChainId;
pub use linera_client::wallet::*;

pub fn pretty_print(wallet: &Wallet, chain_ids: impl IntoIterator<Item = ChainId>) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Chain ID").add_attribute(Attribute::Bold),
            Cell::new("Latest Block").add_attribute(Attribute::Bold),
        ]);

    let admin_chain_id = wallet.genesis_admin_chain();
    for chain_id in chain_ids {
        let Some(user_chain) = wallet.chains.get(&chain_id) else {
            panic!("Chain {} not found.", chain_id);
        };
        update_table_with_chain(
            &mut table,
            chain_id,
            user_chain,
            Some(chain_id) == wallet.default,
            chain_id == admin_chain_id,
        );
    }
    println!("{}", table);
}

fn update_table_with_chain(
    table: &mut Table,
    chain_id: ChainId,
    user_chain: &UserChain,
    is_default_chain: bool,
    is_admin_chain: bool,
) {
    let epoch = user_chain.epoch;
    let mut chain_id_str = format!("{}", chain_id);

    // Add labels below the chain ID.
    let mut labels = Vec::new();
    if is_default_chain {
        labels.push("default");
    }
    if is_admin_chain {
        labels.push("admin");
    }
    if !labels.is_empty() {
        chain_id_str.push_str(&format!("\n{}", labels.join(", ")));
    }

    let chain_id_cell = if is_default_chain {
        Cell::new(chain_id_str).fg(Color::Green)
    } else {
        Cell::new(chain_id_str)
    };
    let epoch_str = match epoch {
        None => "-".to_string(),
        Some(epoch) => format!("{}", epoch),
    };
    let account_owner = user_chain.owner;
    table.add_row(vec![
        chain_id_cell,
        Cell::new(format!(
            r#"AccountOwner:       {}
Block Hash:         {}
Timestamp:          {}
Next Block Height:  {}
Epoch:              {}"#,
            account_owner
                .as_ref()
                .map_or_else(|| "-".to_string(), |o| o.to_string()),
            user_chain
                .block_hash
                .map_or_else(|| "-".to_string(), |bh| bh.to_string()),
            user_chain.timestamp,
            user_chain.next_block_height,
            epoch_str
        )),
    ]);
}
