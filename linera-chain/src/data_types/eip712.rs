// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EIP-712 structured typed data hashing for block proposals.
//!
//! This module implements the EIP-712 signing hash computation so that EVM wallet
//! users can see human-readable block proposal details when signing.

use std::sync::LazyLock;

use alloy_primitives::{hex, keccak256};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Round},
    identifiers::{AccountOwner, ApplicationId, ChainId},
};
use linera_execution::{Operation, SystemOperation};

use super::{MessageAction, ProposalContent, Transaction};

// -- EIP-712 type strings --
//
// Per the EIP-712 spec, the type string for a struct includes all referenced types
// sorted alphabetically and concatenated after the primary type.

const EIP712_DOMAIN_TYPE: &str = "EIP712Domain(string name,string version)";

const LINERA_BLOCK_PROPOSAL_TYPE: &str = "\
LineraBlockProposal(\
bytes32 chainId,\
uint64 epoch,\
uint64 height,\
uint64 timestamp,\
string round,\
bytes32 contentHash,\
TransferOp[] transfers,\
ClaimOp[] claims,\
ReceiveMsg[] incomingMessages,\
UserOp[] userOperations,\
bytes32[] otherTransactionHashes\
)\
ClaimOp(string owner,string targetChain,string recipient,uint128 amount)\
ReceiveMsg(string origin,string action,uint32 messageCount)\
TransferOp(string owner,string recipient,uint128 amount)\
UserOp(string applicationId,bytes32 dataHash)";

const TRANSFER_OP_TYPE: &str = "TransferOp(string owner,string recipient,uint128 amount)";
const CLAIM_OP_TYPE: &str =
    "ClaimOp(string owner,string targetChain,string recipient,uint128 amount)";
const RECEIVE_MSG_TYPE: &str = "ReceiveMsg(string origin,string action,uint32 messageCount)";
const USER_OP_TYPE: &str = "UserOp(string applicationId,bytes32 dataHash)";

// -- Precomputed type hashes --

static DOMAIN_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(EIP712_DOMAIN_TYPE).0);
static PROPOSAL_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(LINERA_BLOCK_PROPOSAL_TYPE).0);
static TRANSFER_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(TRANSFER_OP_TYPE).0);
static CLAIM_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(CLAIM_OP_TYPE).0);
static RECEIVE_MSG_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(RECEIVE_MSG_TYPE).0);
static USER_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(USER_OP_TYPE).0);

// -- Domain separator (constant: name="Linera", version="1") --

static DOMAIN_SEPARATOR: LazyLock<[u8; 32]> = LazyLock::new(|| {
    let mut buf = Vec::with_capacity(96);
    buf.extend_from_slice(&*DOMAIN_TYPE_HASH);
    buf.extend_from_slice(&keccak256("Linera").0);
    buf.extend_from_slice(&keccak256("1").0);
    keccak256(&buf).0
});

// -- ABI encoding helpers --

/// Left-pads a u64 to 32 bytes (big-endian).
fn encode_u64(value: u64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[24..32].copy_from_slice(&value.to_be_bytes());
    buf
}

/// Left-pads a u32 to 32 bytes (big-endian).
fn encode_u32(value: u32) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[28..32].copy_from_slice(&value.to_be_bytes());
    buf
}

/// Left-pads a u128 to 32 bytes (big-endian).
fn encode_u128(value: u128) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[16..32].copy_from_slice(&value.to_be_bytes());
    buf
}

/// Encodes a string as keccak256(string_bytes).
fn encode_string(s: &str) -> [u8; 32] {
    keccak256(s).0
}

/// Encodes a bytes32 value (pass-through, already 32 bytes).
fn encode_bytes32(value: [u8; 32]) -> [u8; 32] {
    value
}

// -- String formatting --

fn format_account_owner(owner: &AccountOwner) -> String {
    owner.to_string()
}

fn format_chain_id(chain_id: &ChainId) -> String {
    chain_id.to_string()
}

fn format_account(chain_id: &ChainId, owner: &AccountOwner) -> String {
    format!(
        "{}:{}",
        format_account_owner(owner),
        format_chain_id(chain_id)
    )
}

fn format_round(round: &Round) -> String {
    match round {
        Round::Fast => "Fast".to_string(),
        Round::MultiLeader(n) => format!("MultiLeader({})", n),
        Round::SingleLeader(n) => format!("SingleLeader({})", n),
        Round::Validator(n) => format!("Validator({})", n),
    }
}

fn format_application_id(app_id: &ApplicationId) -> String {
    format!("{}", app_id.application_description_hash)
}

// -- Struct hashing --

/// `hashStruct(TransferOp)` = `keccak256(typeHash || encodeData(...))`
fn hash_transfer_op(
    owner: &AccountOwner,
    recipient_chain: &ChainId,
    recipient_owner: &AccountOwner,
    amount: Amount,
) -> [u8; 32] {
    let mut buf = Vec::with_capacity(5 * 32);
    buf.extend_from_slice(&*TRANSFER_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_account_owner(owner)));
    buf.extend_from_slice(&encode_string(&format_account(
        recipient_chain,
        recipient_owner,
    )));
    buf.extend_from_slice(&encode_u128(amount.to_attos()));
    keccak256(&buf).0
}

/// `hashStruct(ClaimOp)` = `keccak256(typeHash || encodeData(...))`
fn hash_claim_op(
    owner: &AccountOwner,
    target_chain: &ChainId,
    recipient_chain: &ChainId,
    recipient_owner: &AccountOwner,
    amount: Amount,
) -> [u8; 32] {
    let mut buf = Vec::with_capacity(6 * 32);
    buf.extend_from_slice(&*CLAIM_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_account_owner(owner)));
    buf.extend_from_slice(&encode_string(&format_chain_id(target_chain)));
    buf.extend_from_slice(&encode_string(&format_account(
        recipient_chain,
        recipient_owner,
    )));
    buf.extend_from_slice(&encode_u128(amount.to_attos()));
    keccak256(&buf).0
}

/// `hashStruct(ReceiveMsg)` = `keccak256(typeHash || encodeData(...))`
fn hash_receive_msg(origin: &ChainId, action: &MessageAction, message_count: u32) -> [u8; 32] {
    let action_str = match action {
        MessageAction::Accept => "Accept",
        MessageAction::Reject => "Reject",
    };
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&*RECEIVE_MSG_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_chain_id(origin)));
    buf.extend_from_slice(&encode_string(action_str));
    buf.extend_from_slice(&encode_u32(message_count));
    keccak256(&buf).0
}

/// `hashStruct(UserOp)` = `keccak256(typeHash || encodeData(...))`
fn hash_user_op(application_id: &ApplicationId, data: &[u8]) -> [u8; 32] {
    let data_hash = keccak256(data).0;
    let mut buf = Vec::with_capacity(3 * 32);
    buf.extend_from_slice(&*USER_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_application_id(application_id)));
    buf.extend_from_slice(&encode_bytes32(data_hash));
    keccak256(&buf).0
}

/// Encodes a dynamic array of struct hashes: `keccak256(concat(hashStruct(elem)...))`.
/// Empty arrays produce `keccak256(b"")`.
fn encode_hash_array(hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(hashes.len() * 32);
    for h in hashes {
        buf.extend_from_slice(h);
    }
    keccak256(&buf).0
}

// -- Categorized transactions --

struct CategorizedTransactions {
    transfers: Vec<[u8; 32]>,
    claims: Vec<[u8; 32]>,
    incoming_messages: Vec<[u8; 32]>,
    user_operations: Vec<[u8; 32]>,
    other_hashes: Vec<[u8; 32]>,
}

fn categorize_transactions(transactions: &[Transaction]) -> CategorizedTransactions {
    let mut result = CategorizedTransactions {
        transfers: Vec::new(),
        claims: Vec::new(),
        incoming_messages: Vec::new(),
        user_operations: Vec::new(),
        other_hashes: Vec::new(),
    };

    for tx in transactions {
        match tx {
            Transaction::ExecuteOperation(Operation::System(sys_op)) => match sys_op.as_ref() {
                SystemOperation::Transfer {
                    owner,
                    recipient,
                    amount,
                } => {
                    result.transfers.push(hash_transfer_op(
                        owner,
                        &recipient.chain_id,
                        &recipient.owner,
                        *amount,
                    ));
                }
                SystemOperation::Claim {
                    owner,
                    target_id,
                    recipient,
                    amount,
                } => {
                    result.claims.push(hash_claim_op(
                        owner,
                        target_id,
                        &recipient.chain_id,
                        &recipient.owner,
                        *amount,
                    ));
                }
                _ => {
                    let hash: [u8; 32] = CryptoHash::new(tx).into();
                    result.other_hashes.push(hash);
                }
            },
            Transaction::ExecuteOperation(Operation::User {
                application_id,
                bytes,
            }) => {
                result
                    .user_operations
                    .push(hash_user_op(application_id, bytes));
            }
            Transaction::ReceiveMessages(bundle) => {
                result.incoming_messages.push(hash_receive_msg(
                    &bundle.origin,
                    &bundle.action,
                    bundle.bundle.messages.len() as u32,
                ));
            }
        }
    }

    result
}

/// Computes the EIP-712 signing hash for a `ProposalContent`.
///
/// Returns `keccak256("\x19\x01" || domainSeparator || hashStruct(proposal))`.
pub fn eip712_signing_hash(content: &ProposalContent) -> [u8; 32] {
    let block = &content.block;
    let categorized = categorize_transactions(&block.transactions);

    // BCS hash of the full ProposalContent for integrity binding.
    let content_hash: [u8; 32] = CryptoHash::new(content).into();

    // hashStruct(LineraBlockProposal)
    let mut buf = Vec::with_capacity(12 * 32);
    buf.extend_from_slice(&*PROPOSAL_TYPE_HASH);
    buf.extend_from_slice(&encode_bytes32(block.chain_id.0.into()));
    buf.extend_from_slice(&encode_u64(block.epoch.0 as u64));
    buf.extend_from_slice(&encode_u64(block.height.0));
    buf.extend_from_slice(&encode_u64(block.timestamp.micros()));
    buf.extend_from_slice(&encode_string(&format_round(&content.round)));
    buf.extend_from_slice(&encode_bytes32(content_hash));
    buf.extend_from_slice(&encode_hash_array(&categorized.transfers));
    buf.extend_from_slice(&encode_hash_array(&categorized.claims));
    buf.extend_from_slice(&encode_hash_array(&categorized.incoming_messages));
    buf.extend_from_slice(&encode_hash_array(&categorized.user_operations));
    buf.extend_from_slice(&encode_hash_array(&categorized.other_hashes));
    let struct_hash = keccak256(&buf).0;

    // Final EIP-712 signing hash
    let mut final_buf = Vec::with_capacity(2 + 32 + 32);
    final_buf.extend_from_slice(b"\x19\x01");
    final_buf.extend_from_slice(&*DOMAIN_SEPARATOR);
    final_buf.extend_from_slice(&struct_hash);
    keccak256(&final_buf).0
}

// -- JSON typed data builder --

/// Formats a `[u8; 32]` as a `0x`-prefixed hex string (66 chars).
fn format_bytes32(bytes: [u8; 32]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Formats a `MessageAction` as a display string.
fn format_message_action(action: &MessageAction) -> &'static str {
    match action {
        MessageAction::Accept => "Accept",
        MessageAction::Reject => "Reject",
    }
}

/// Categorized transaction values for JSON serialization.
struct CategorizedValues {
    transfers: Vec<serde_json::Value>,
    claims: Vec<serde_json::Value>,
    incoming_messages: Vec<serde_json::Value>,
    user_operations: Vec<serde_json::Value>,
    other_hashes: Vec<serde_json::Value>,
}

/// Same categorization logic as `categorize_transactions`, but produces JSON values
/// instead of hashes.
fn categorize_transactions_values(transactions: &[Transaction]) -> CategorizedValues {
    let mut result = CategorizedValues {
        transfers: Vec::new(),
        claims: Vec::new(),
        incoming_messages: Vec::new(),
        user_operations: Vec::new(),
        other_hashes: Vec::new(),
    };

    for tx in transactions {
        match tx {
            Transaction::ExecuteOperation(Operation::System(sys_op)) => match sys_op.as_ref() {
                SystemOperation::Transfer {
                    owner,
                    recipient,
                    amount,
                } => {
                    result.transfers.push(serde_json::json!({
                        "owner": format_account_owner(owner),
                        "recipient": format_account(&recipient.chain_id, &recipient.owner),
                        "amount": amount.to_attos().to_string(),
                    }));
                }
                SystemOperation::Claim {
                    owner,
                    target_id,
                    recipient,
                    amount,
                } => {
                    result.claims.push(serde_json::json!({
                        "owner": format_account_owner(owner),
                        "targetChain": format_chain_id(target_id),
                        "recipient": format_account(&recipient.chain_id, &recipient.owner),
                        "amount": amount.to_attos().to_string(),
                    }));
                }
                _ => {
                    let hash: [u8; 32] = CryptoHash::new(tx).into();
                    result
                        .other_hashes
                        .push(serde_json::json!(format_bytes32(hash)));
                }
            },
            Transaction::ExecuteOperation(Operation::User {
                application_id,
                bytes,
            }) => {
                let data_hash = keccak256(bytes).0;
                result.user_operations.push(serde_json::json!({
                    "applicationId": format_application_id(application_id),
                    "dataHash": format_bytes32(data_hash),
                }));
            }
            Transaction::ReceiveMessages(bundle) => {
                result.incoming_messages.push(serde_json::json!({
                    "origin": format_chain_id(&bundle.origin),
                    "action": format_message_action(&bundle.action),
                    "messageCount": bundle.bundle.messages.len() as u32,
                }));
            }
        }
    }

    result
}

/// Produces the full EIP-712 typed data JSON for a `ProposalContent`.
///
/// The JSON can be sent to MetaMask via `eth_signTypedData_v4`, or parsed by the
/// generic `compute_eip712_hash` in `linera-base` to produce the signing hash.
pub fn eip712_typed_data_json(content: &ProposalContent) -> String {
    let block = &content.block;
    let categorized = categorize_transactions_values(&block.transactions);
    let content_hash: [u8; 32] = CryptoHash::new(content).into();

    let typed_data = serde_json::json!({
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"}
            ],
            "LineraBlockProposal": [
                {"name": "chainId", "type": "bytes32"},
                {"name": "epoch", "type": "uint64"},
                {"name": "height", "type": "uint64"},
                {"name": "timestamp", "type": "uint64"},
                {"name": "round", "type": "string"},
                {"name": "contentHash", "type": "bytes32"},
                {"name": "transfers", "type": "TransferOp[]"},
                {"name": "claims", "type": "ClaimOp[]"},
                {"name": "incomingMessages", "type": "ReceiveMsg[]"},
                {"name": "userOperations", "type": "UserOp[]"},
                {"name": "otherTransactionHashes", "type": "bytes32[]"}
            ],
            "TransferOp": [
                {"name": "owner", "type": "string"},
                {"name": "recipient", "type": "string"},
                {"name": "amount", "type": "uint128"}
            ],
            "ClaimOp": [
                {"name": "owner", "type": "string"},
                {"name": "targetChain", "type": "string"},
                {"name": "recipient", "type": "string"},
                {"name": "amount", "type": "uint128"}
            ],
            "ReceiveMsg": [
                {"name": "origin", "type": "string"},
                {"name": "action", "type": "string"},
                {"name": "messageCount", "type": "uint32"}
            ],
            "UserOp": [
                {"name": "applicationId", "type": "string"},
                {"name": "dataHash", "type": "bytes32"}
            ]
        },
        "primaryType": "LineraBlockProposal",
        "domain": {
            "name": "Linera",
            "version": "1"
        },
        "message": {
            "chainId": format_bytes32(block.chain_id.0.into()),
            "epoch": block.epoch.0 as u64,
            "height": block.height.0,
            "timestamp": block.timestamp.micros(),
            "round": format_round(&content.round),
            "contentHash": format_bytes32(content_hash),
            "transfers": categorized.transfers,
            "claims": categorized.claims,
            "incomingMessages": categorized.incoming_messages,
            "userOperations": categorized.user_operations,
            "otherTransactionHashes": categorized.other_hashes,
        }
    });

    typed_data.to_string()
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::{BlockHeight, Epoch, Round},
        identifiers::{Account, AccountOwner, ChainId},
    };

    use super::*;
    use crate::data_types::{ProposalContent, ProposedBlock};

    fn test_chain_id() -> ChainId {
        ChainId(CryptoHash::new(&TestString::new("ChainId")))
    }

    fn empty_proposal() -> ProposalContent {
        ProposalContent {
            block: ProposedBlock {
                chain_id: test_chain_id(),
                epoch: Epoch(11),
                transactions: vec![],
                height: BlockHeight(11),
                timestamp: 190000000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::SingleLeader(11),
            outcome: None,
        }
    }

    #[test]
    fn test_domain_separator_deterministic() {
        let sep1 = *DOMAIN_SEPARATOR;
        let sep2 = *DOMAIN_SEPARATOR;
        assert_eq!(sep1, sep2);
        assert_ne!(sep1, [0u8; 32]);
    }

    #[test]
    fn test_type_hash_strings_sorted() {
        // Verify that referenced types in the proposal type string are alphabetically sorted.
        let type_str = LINERA_BLOCK_PROPOSAL_TYPE;
        // Find the closing paren of the primary type, then check the order of referenced types.
        let after_primary = type_str.find(')').unwrap() + 1;
        let referenced = &type_str[after_primary..];
        let type_names: Vec<&str> = referenced
            .split(')')
            .filter(|s| !s.is_empty())
            .map(|s| s.split('(').next().unwrap())
            .collect();
        let mut sorted = type_names.clone();
        sorted.sort();
        assert_eq!(type_names, sorted);
    }

    #[test]
    fn test_empty_proposal_hash() {
        let proposal = empty_proposal();
        let hash = eip712_signing_hash(&proposal);
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn test_full_proposal_deterministic() {
        let proposal = empty_proposal();
        let hash1 = eip712_signing_hash(&proposal);
        let hash2 = eip712_signing_hash(&proposal);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_proposals_different_hashes() {
        let proposal1 = empty_proposal();
        let mut proposal2 = empty_proposal();
        proposal2.block.height = BlockHeight(12);
        let hash1 = eip712_signing_hash(&proposal1);
        let hash2 = eip712_signing_hash(&proposal2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_transfer_op_encoding() {
        let owner = AccountOwner::Address20([0xAB; 20]);
        let recipient_chain = test_chain_id();
        let recipient_owner = AccountOwner::Address20([0xCD; 20]);
        let amount = Amount::from_tokens(100);
        let hash1 = hash_transfer_op(&owner, &recipient_chain, &recipient_owner, amount);
        let hash2 = hash_transfer_op(&owner, &recipient_chain, &recipient_owner, amount);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, [0u8; 32]);

        // Different amount -> different hash
        let hash3 = hash_transfer_op(
            &owner,
            &recipient_chain,
            &recipient_owner,
            Amount::from_tokens(200),
        );
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_proposal_with_transfer() {
        use linera_execution::Operation;

        let chain_id = test_chain_id();
        let owner = AccountOwner::Address20([0xAB; 20]);
        let recipient = Account {
            chain_id,
            owner: AccountOwner::Address20([0xCD; 20]),
        };

        let transfer =
            Transaction::ExecuteOperation(Operation::System(Box::new(SystemOperation::Transfer {
                owner,
                recipient,
                amount: Amount::from_tokens(50),
            })));

        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id,
                epoch: Epoch(1),
                transactions: vec![transfer],
                height: BlockHeight(0),
                timestamp: 1000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::Fast,
            outcome: None,
        };

        let hash = eip712_signing_hash(&proposal);
        let empty_hash = eip712_signing_hash(&empty_proposal());
        assert_ne!(hash, empty_hash);
    }

    /// Verifies that `compute_eip712_hash(eip712_typed_data_json(content))` produces the
    /// exact same hash as `eip712_signing_hash(content)` for an empty proposal.
    #[test]
    fn test_json_hash_matches_direct_hash_empty() {
        let proposal = empty_proposal();
        let direct_hash = eip712_signing_hash(&proposal);

        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);

        assert_eq!(direct_hash, json_hash);
    }

    /// Same cross-check but with a proposal containing a transfer transaction.
    #[test]
    fn test_json_hash_matches_direct_hash_with_transfer() {
        use linera_execution::Operation;

        let chain_id = test_chain_id();
        let owner = AccountOwner::Address20([0xAB; 20]);
        let recipient = Account {
            chain_id,
            owner: AccountOwner::Address20([0xCD; 20]),
        };

        let transfer =
            Transaction::ExecuteOperation(Operation::System(Box::new(SystemOperation::Transfer {
                owner,
                recipient,
                amount: Amount::from_tokens(50),
            })));

        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id,
                epoch: Epoch(1),
                transactions: vec![transfer],
                height: BlockHeight(0),
                timestamp: 1000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::Fast,
            outcome: None,
        };

        let direct_hash = eip712_signing_hash(&proposal);
        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);

        assert_eq!(direct_hash, json_hash);
    }
}
