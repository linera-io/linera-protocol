//! Binary Merkle Tree for Linera transactions and various other objects
//! for efficient zero-knowledge validity proofs of inclusion.

use std::{fmt::Debug, marker::PhantomData};

use serde::{Deserialize, Serialize};

use crate::crypto::{BcsHashable, CryptoHash};

/// Intermediate node for a binary merkle Tree
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    /// Hash of this intermediate node.
    /// Computed as the hash of its children from left to right.
    pub hash: CryptoHash,
}

/// Just a simple binary merkle tree
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Tree<T> {
    /// All the nodes contained by this Tree.
    /// Nodes are stored in Breadth-First / Level order.
    /// If the number of nodes at any level is odd then
    /// only the last node is duplicated only at that level, so
    /// that the Tree only contains minimum number of nodes required.
    /// The rest of information can be computed on-the-fly dynamically.
    pub elements: Vec<Node>,

    /// Number of leaves, cached only for convenience purposes.
    pub number_of_leaves: u32,

    /// Type bound indenity for a Tree.
    pub _phantom_leaves: PhantomData<T>,
}

fn hash_node_pair(left: &Node, right: &Node) -> CryptoHash {
    use sha3::digest::Digest;

    let mut hasher = sha3::Sha3_256::default();
    hasher.update(left.hash.as_bytes());
    hasher.update(right.hash.as_bytes());
    hasher.finalize()[..].try_into().expect("not possible")
}

fn build_tree(nodes: &mut Vec<Node>, hashed_leaves: &mut Vec<Node>) {
    if hashed_leaves.len() == 1 {
        nodes.push(hashed_leaves.last().expect("not possible").clone());
        return;
    }

    if hashed_leaves.len() % 2 == 1 {
        hashed_leaves.push(hashed_leaves.last().expect("not possible").clone());
    }

    let mut hashed_nodes = Vec::new();
    for pair in hashed_leaves.chunks(2) {
        hashed_nodes.push(Node { hash: hash_node_pair(&pair[0], &pair[1]) });
    }
    build_tree(nodes, &mut hashed_nodes);
    nodes.append(hashed_leaves);
}

// base methods
// other methods can be implemented at module or crate level
impl<T> Tree<T>
where
    T: Debug + Clone + for<'de> BcsHashable<'de>
{
    /// Constructs a simple binary merkle tree from the provided values.
    pub fn from_values(values: &[T]) -> Self {
        if values.len() == 0 {
            return Self { elements: Vec::new(), _phantom_leaves: PhantomData, number_of_leaves: 0u32 }
        }

        let mut hashed_leaves = Vec::new();
        let mut nodes = Vec::new();

        for value in values {
            hashed_leaves.push(Node { hash: CryptoHash::new(value) });
        }

        build_tree(&mut nodes, &mut hashed_leaves);

        Self { elements: nodes, _phantom_leaves: PhantomData, number_of_leaves: values.len() as u32 }
    }

    /// Takes a shared reference of the root the tree.
    pub fn root(&self) -> Option<&CryptoHash> {
        self.elements.first().map(|node| &node.hash)
    }
}
