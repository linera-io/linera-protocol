use linera_base::trie::TrieValue;
use thiserror::Error;

use super::node::{Adaption, AdaptiveRadixTrieNode};

#[derive(Error, Debug)]
pub enum Error {
    #[error("leaves' path must be 32 bytes")]
    InvalidLeafPath,

    #[error("leaf at the path to be inserted is already occupied")]
    LeafAlreadyExists,

    #[error("cannot insert leaf somehow")]
    LeafInsertError,
}

// its quite clumsy and inefficient right now;
// TODO implement multi-threading
// for a Concurrent Trie. Should be
// quite simple.
struct Walker<'a> {
    root: Option<&'a mut dyn AdaptiveRadixTrieNode>,
    path: &'a [u8],                     // the path to walk
    index: usize,                       // absolute index into the path
    phantom_index: usize,               // index into the currently-visited node's compressed path
    value: Option<Box<dyn TrieValue>>,
    walked: bool,
    should_overwrite: bool,
    result: bool,
}

impl<'a> Walker<'a> {
    pub fn new(root: &'a mut dyn AdaptiveRadixTrieNode) -> Self {
        Walker {
            root: Some(root),
            path: &[],
            index: 0,
            phantom_index: 0,
            value: None,
            walked: false,
            should_overwrite: false,
            result: false,
        }
    }

    fn reset(&mut self) {
        self.path = &[];
        self.index = 0;
        self.phantom_index = 0;
        self.value = None;
        self.walked = false;
        self.should_overwrite = false;
        self.result = false;
    }

    pub fn insert(
        &mut self,
        key: &'a [u8],
        value: Box<dyn TrieValue>,
        should_overwrite: bool,
    ) -> Result<(), Error> {

        if 32 != key.len() {
            return Err(Error::InvalidLeafPath)
        }

        self.path = key;
        self.value = Some(value);

        let mut root = self.root.take().unwrap();

        root = self.recursive_insert(root);

        self.root.replace(root);

        let result = match (should_overwrite, self.result) {
            (true, false) => Err(Error::LeafInsertError),
            (false, false) => Err(Error::LeafAlreadyExists),
            (_, true) => Ok(()),
        };

        self.reset();
        result
    }

    // TODO
    // Get back to the root of trie
    // hashing the intermediate nodes along the way.
    fn recursive_insert<'b>(&mut self, root: &'b mut dyn AdaptiveRadixTrieNode) -> &'b mut dyn AdaptiveRadixTrieNode {
        if 32 == self.phantom_index {
            self.walked = true;
            self.result = root.insert(self.path[self.phantom_index - 1], &self.path, self.value.take().unwrap(), self.should_overwrite);
            return root;
        }

        // check for splicing
        let mut should_splice = None;
        if let Some(node) = root.walk(self.path[self.phantom_index]) {
            let node_path = node.path();
            for i in 0..node_path.len() {
                if node_path[i] != self.path[i] {
                    should_splice = Some((node_path[0..i].to_vec(), node_path[i]));
                    break;
                }
            }
        }

        if let Some(splicing_data) = should_splice {
            root.adapt(self.path[self.phantom_index], Adaption::Splice(splicing_data))
        }

        // check if next node needs to be promoted
        let mut next_is_filled = false;
        if let Some(node) = root.walk(self.path[self.phantom_index]) {
            let phantom_index = node.path().len();
            if 32 != phantom_index    // if not leaf; leaves are only spliced not promoted
                && !node.contains(self.path[phantom_index])
                && node.is_filled()
            {
                next_is_filled = true;
            }

        }

        if next_is_filled {
            root.adapt(self.path[root.path().len()], Adaption::Promote);
        }

        if let Some(node) = root.walk(self.path[self.phantom_index]) {
            self.phantom_index = node.path().len();
            self.index += 1;
            let _ = self.recursive_insert(node);
        }

        if !self.walked {
            self.result = root.insert(self.path[self.phantom_index], &self.path, self.value.take().unwrap(), self.should_overwrite);
            self.walked = true;
        }

        root
    }
}

