// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, Color, ContentArrangement,
    Table,
};
use linera_base::identifiers::{ChainId, Owner};
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
    for chain_id in chain_ids {
        let Some(user_chain) = wallet.chains.get(&chain_id) else {
            panic!("Chain {} not found.", chain_id);
        };
        update_table_with_chain(
            &mut table,
            chain_id,
            user_chain,
            Some(chain_id) == wallet.default,
        );
    }
    println!("{}", table);
}

fn update_table_with_chain(
    table: &mut Table,
    chain_id: ChainId,
    user_chain: &UserChain,
    is_default_chain: bool,
) {
    let chain_id_cell = if is_default_chain {
        Cell::new(format!("{}", chain_id)).fg(Color::Green)
    } else {
        Cell::new(format!("{}", chain_id))
    };
    table.add_row(vec![
        chain_id_cell,
        Cell::new(format!(
            r#"Public Key:         {}
Owner:              {}
Block Hash:         {}
Timestamp:          {}
Next Block Height:  {}"#,
            user_chain
                .key_pair
                .as_ref()
                .map(|kp| kp.public().to_string())
                .unwrap_or_else(|| "-".to_string()),
            user_chain
                .key_pair
                .as_ref()
                .map(|kp| Owner::from(kp.public()))
                .map(|o| o.to_string())
                .unwrap_or_else(|| "-".to_string()),
            user_chain
                .block_hash
                .map(|bh| bh.to_string())
                .unwrap_or_else(|| "-".to_string()),
            user_chain.timestamp,
            user_chain.next_block_height
        )),
    ]);
}
