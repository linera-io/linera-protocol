// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera chain helpers: Credit-to-Address20 parsing and burn serialization.

use linera_base::{
    data_types::Amount,
    identifiers::{AccountOwner, ApplicationId},
};
use linera_chain::data_types::Transaction;
use linera_execution::Message;

/// Find all Credit-to-Address20 messages in a block's transactions for a given app.
///
/// Returns `(owner, amount)` pairs for each matching credit.
pub(crate) fn find_address20_credits(
    transactions: &[Transaction],
    fungible_app_id: ApplicationId,
) -> Vec<(AccountOwner, Amount)> {
    let mut credits = Vec::new();
    for txn in transactions {
        if let Transaction::ReceiveMessages(bundle) = txn {
            for posted in &bundle.bundle.messages {
                if let Message::User {
                    application_id,
                    bytes,
                } = &posted.message
                {
                    if *application_id == fungible_app_id {
                        if let Some(credit) = try_parse_credit_to_address20(bytes.as_slice()) {
                            credits.push(credit);
                        }
                    }
                }
            }
        }
    }
    credits
}

pub(crate) fn serialize_burn_operation(owner: &AccountOwner, amount: &Amount) -> Vec<u8> {
    bcs::to_bytes(&wrapped_fungible::WrappedFungibleOperation::Burn {
        owner: *owner,
        amount: *amount,
    })
    .expect("failed to BCS-serialize Burn operation")
}

/// Extract (owner, amount) from a fungible Credit message if the target is Address20.
fn try_parse_credit_to_address20(bytes: &[u8]) -> Option<(AccountOwner, Amount)> {
    if let Ok(fungible::Message::Credit { target, amount, .. }) = bcs::from_bytes(bytes) {
        matches!(target, AccountOwner::Address20(_)).then_some((target, amount))
    } else {
        None
    }
}
