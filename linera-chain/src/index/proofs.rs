use linera_base::tree::Tree;
use serde::{Deserialize, Serialize};


/// simple proof chache for binary merkle tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    pub hashes: Vec<u32>,
    pub index: u32,
}

/// extrention for tree-like data structures
pub trait MerkleProof {
    type Proof;

    fn build_proof(&self, index: u32) -> Result<Self::Proof, String>;
}

impl<T> MerkleProof for Tree<T> {
    type Proof = Proof;

    fn build_proof(&self, index: u32) -> Result<Proof, String> {
        let number_of_leaves = self.number_of_leaves;
        if number_of_leaves < index + 1 {
            return Err(format!("index: {index} not found in the tree"));
        }

        let mut hashes = Vec::new();

        let virtual_leaves_size= number_of_leaves.next_power_of_two();
        let virtual_tree_size = 2 * virtual_leaves_size - 1;
        let phantom_leaves_size = (virtual_leaves_size - number_of_leaves).next_power_of_two() / 2;
        let phantom_subtree_size = (2 * phantom_leaves_size).saturating_sub(2);
        let mut phantom_partition = phantom_subtree_size - phantom_leaves_size;

        let mut cursor = virtual_leaves_size + index - 1;
        while 0 != cursor {
            let mut virtual_cursor = cursor + phantom_partition;
            hashes.push(virtual_cursor ^ 1);
            virtual_cursor = ((virtual_cursor + (1 & virtual_cursor)) >> 1) - 1;
            cursor = virtual_cursor - ((phantom_partition + (0 != phantom_partition) as u32 * 2) >> 1);
            phantom_partition = phantom_partition.saturating_sub(phantom_partition + 2 >> 1);
        }

        Ok(Proof { hashes, index })
    }
}

// TODO
mod tests {}
