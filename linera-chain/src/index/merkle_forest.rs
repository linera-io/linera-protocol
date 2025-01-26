use linera_base::{crypto::CryptoHash, tree::Tree};

use crate::{block_header::BlockHeader, transaction::{Transaction, TransactionOutcome}};

use super::trie::Trie;


type WorldTree = Trie;

pub struct Forest {
    pub world_tree: WorldTree,
    pub message_forest: Vec<Tree<Transaction>>,
    pub operation_forest: Vec<Tree<Transaction>>,
    pub receipts_forest: Vec<Tree<TransactionOutcome>>,
    pub block_headers: Vec<(BlockHeader, CryptoHash)>, // inverse mapping (header_hash -> height) can be inserted in the world tree
}

