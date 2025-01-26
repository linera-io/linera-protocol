
use linera_base::{codec::Codec, crypto::CryptoHash};

use super::node::Node256;


#[derive(Debug)]
pub struct Trie {
    pub root: Node256,
    pub blocks: Vec<TrieBlock>,
}


/// In a TrieBlock, the values at the leaves point to the
/// WorldTree leaves, maybe by a raw pointer.
/// So a TrieBlock is just a indexed reference to all the values in that block
/// which resides in the WorldTee.
#[derive(Debug)]
pub struct TrieBlock {
    pub root: PhantomNode,
    pub hash: CryptoHash, // merkle skip list
    pub nodes: Vec<[u8; 32]>, // keys of leaves
}

type PhantomNode = Node256;


/// Its a TODO
/// When persisting a WorldTree in the views, the Tree
/// itself need not be of concern.
/// The leaves of TrieBlocks would have to be serialized
/// in their respective vectors with a 1-1 mapping of keys, and not
/// the intermediate nodes themselves.
/// Then all the Tries, inclusive of the World, can be dynamically constructed
/// by inserting those key - value leaves block by block.
impl Codec for TrieBlock {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized {
        todo!()
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized {
        todo!()
    }
}

impl Codec for Trie {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized {
        todo!()
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized {
        todo!()
    }
}
