//! Defines a common trie functionality for various Linera Tries

use std::fmt::Debug;

/// Trie node definition.
pub trait TrieNode: Debug {
    /// Key upto this point.
    fn path(&self) -> &[u8];

    /// Does this node contains a child at this character.
    fn contains(&self, chr: u8) -> bool;

    /// Get a reference to the children of this node.
    fn daughters(&self) -> &[Option<Box<dyn TrieNode>>];

    /// Follow a path character to a child pointer.
    fn walk(&self, chr: u8) -> Option<&dyn TrieNode>;

    /// Does this node contains a value.
    fn value(&self) -> Option<&dyn TrieValue>;

    /// Takes a exclusive reference to value contained by this node.
    fn value_as_mut(&mut self) -> Option<&mut dyn TrieValue>;

    /// Are all the slots of this node filled.
    fn is_filled(&self) -> bool;

    /// Inserts a Leaf at this character path.
    fn insert(&mut self, chr: u8, path: &[u8], value: Box<dyn TrieValue>, should_overwrite: bool) -> bool;
}


/// What defines a Trie.
pub trait Trie {
    /// Takes a shared reference of the root of this trie.
    fn root(&self) -> &dyn TrieNode;

    /// Insert a node at this character.
    fn insert(&mut self, key: &str, value: Box<dyn TrieValue>) -> bool;

    /// Remove a value at this path
    fn remove(&mut self, key: &str) -> Box<dyn TrieValue>;

    /// Does this trie contains a node at this path.
    fn contains(&self, key: &str) -> bool;
}


/// Generic trait bound for a trie value.
pub trait TrieValue: Debug {}
