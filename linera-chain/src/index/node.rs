use linera_base::trie::{TrieNode, TrieValue};


// for a node X
//
// path field refers to a
// maybe compressed path
// to be lazily expanded
// most paths are likely to be compressed
//
// value field can be used for literary anything

pub trait AdaptiveRadixTrieNode: TrieNode {
    fn id(&self) -> NodeId;

    fn adapt(&mut self, adapt_at: u8, adaption: Adaption);

    fn promote(&mut self) -> Box<dyn AdaptiveRadixTrieNode>;

    fn walk(&mut self, chr: u8) -> Option<&mut dyn AdaptiveRadixTrieNode>;
}

pub enum Adaption {
    Promote,
    Splice((Vec<u8>, u8)),
}

#[derive(Debug)]
pub struct Node256 {
    pub path: Vec<u8>,
    pub value: Option<Box<dyn TrieValue>>,
    pub count: u8,
    pub pointers: [Option<Box<dyn AdaptiveRadixTrieNode>>; 256],
}

#[derive(Debug)]
pub struct Node4 {
    pub path: Vec<u8>,
    pub value: Option<Box<dyn TrieValue>>,
    pub keys: [u8; 4],
    pub count: u8,
    pub pointers: [Option<Box<dyn AdaptiveRadixTrieNode>>; 4],
}

#[derive(Debug)]
pub struct Node16 {
    pub path: Vec<u8>,
    pub value: Option<Box<dyn TrieValue>>,
    pub keys: [u8; 16],
    pub count: u8,
    pub pointers: [Option<Box<dyn AdaptiveRadixTrieNode>>; 16],
}

#[derive(Debug)]
pub struct Node48 {
    pub path: Vec<u8>,
    pub value: Option<Box<dyn TrieValue>>,
    pub indexes: [i8; 256],
    pub count: u8,
    pub pointers: [Option<Box<dyn AdaptiveRadixTrieNode>>; 48],
}

#[derive(Debug)]
pub struct Leaf {
    pub path: Vec<u8>,
    pub value: Option<Box<dyn TrieValue>>,  // leaves always contain some value
}

pub enum NodeId {
    Empty,
    Leaf,
    Node4,
    Node16,
    Node48,
    Node256,
}

impl Leaf {
    pub fn new(path: &[u8], value: Box<dyn TrieValue>) -> Self {
        Leaf {
            path: path.to_vec(),
            value: Some(value),
        }
    }
}

impl TrieNode for Leaf {
    fn path(&self) -> &[u8] {
        todo!()
    }

    fn contains(&self, _chr: u8) -> bool {
        false
    }

    fn daughters(&self) -> &[Option<Box<dyn TrieNode>>] {
        &[]
    }

    fn walk(&self, _chr: u8) -> Option<&dyn TrieNode> {
        None
    }

    fn value(&self) -> Option<&dyn TrieValue> {
        todo!()
    }

    fn value_as_mut(&mut self) -> Option<&mut dyn TrieValue> {
        todo!()
    }

    fn is_filled(&self) -> bool {
        true
    }

    fn insert(&mut self, _chr: u8, _path: &[u8], value: Box<dyn TrieValue>, should_overwrite: bool) -> bool {
        if !should_overwrite {
            return false
        }

        let _ = self.value.replace(value);
        true
    }
}

impl AdaptiveRadixTrieNode for Leaf {
    fn id(&self) -> NodeId {
        NodeId::Leaf
    }

    fn adapt(&mut self, _adapt_at: u8, adaption: Adaption) {
        match adaption {
            Adaption::Promote => panic!("Leaves are the ends of the world. No point in promotion."),
            Adaption::Splice(_) => panic!("Leaves are the ends of the world. No sense in slicing."),
        }
    }

    fn promote(&mut self) -> Box<dyn AdaptiveRadixTrieNode> {
        panic!("Leaves are the ends of the world. No point in promotion.")
    }

    fn walk(&mut self, _chr: u8) -> Option<&mut dyn AdaptiveRadixTrieNode> {
        None
    }
}

impl Default for Node4 {
    fn default() -> Self {
        Self {
            path: Vec::new(),
            value: None,
            keys: [0u8; 4],
            count: 0,
            pointers: [const { None }; 4],
        }
    }
}

impl TrieNode for Node4 {
    fn insert(&mut self, chr: u8, path: &[u8], value: Box<dyn TrieValue>, should_overwrite: bool) -> bool {
        let mut i = 0;

        loop {
            if i == self.count as usize {
                self.count += 1;
                break;
            }

            if chr != self.keys[i] {
                i += 1;
                continue;
            }

            if !should_overwrite {
                return false;
            }

            break;
        }

        self.keys[i] = chr;
        self.pointers[i] = Some(Box::new(Leaf::new(path, value)));

        true
    }

    fn path(&self) -> &[u8] {
        todo!()
    }

    fn contains(&self, chr: u8) -> bool {
        let mut i = 0;

        loop {
            if self.count == i as u8 {
                return false;
            }

            if chr != self.keys[i] {
                i += 1;
                continue;
            }

            if self.pointers[i].is_none() {
                return false;
            }

            break;
        }

        true
    }

    fn daughters(&self) -> &[Option<Box<dyn TrieNode>>] {
        todo!()
    }

    fn walk(&self, chr: u8) -> Option<&dyn TrieNode> {
        todo!()
    }

    fn value(&self) -> Option<&dyn TrieValue> {
        todo!()
    }

    fn value_as_mut(&mut self) -> Option<&mut dyn TrieValue> {
        todo!()
    }

    fn is_filled(&self) -> bool {
        // use count
        todo!()
    }
}

impl AdaptiveRadixTrieNode for Node4 {
    fn id(&self) -> NodeId {
        NodeId::Node4
    }

    fn walk(&mut self, chr: u8) -> Option<&mut dyn AdaptiveRadixTrieNode> {
        todo!()
    }

    fn adapt(&mut self, adapt_at: u8, adaption: Adaption) {
        match adaption {
            Adaption::Promote => {
                let node = <Self as AdaptiveRadixTrieNode>::walk(self, adapt_at).unwrap();
                let new_node = node.promote();

                for i in 0..4 {
                    if adapt_at == self.keys[i] {
                        self.pointers[i] = Some(new_node);
                        break;
                    }
                }
            },

            Adaption::Splice((suffixed_path, suffix_at)) => {
                let mut node = Node4::default();
                node.path = suffixed_path;
                node.keys[0] = suffix_at;
                node.count = 1;

                for i in 0..4 {
                    if adapt_at == self.keys[i] {

                        node.pointers[0] = self.pointers[i].take();
                        let _ = self.pointers[i].replace(Box::new(node));
                        break;
                    }
                }
            },
        }
    }

    fn promote(&mut self) -> Box<dyn AdaptiveRadixTrieNode> {
        let path = self.path.clone();
        let value = self.value.take();

        let mut keys = [0u8; 16];
        for i in 0..4 {
            keys[i] = self.keys[i];
        }

        let count = self.count;

        let mut pointers = [const { None }; 16];
        for i in 0..4 {
            pointers[i] = self.pointers[i].take();
        }

        let node = Node16 {
            path,
            value,
            keys,
            count,
            pointers,
        };

        Box::new(node)
    }
}

impl TrieNode for Node16 {
    fn path(&self) -> &[u8] {
        todo!()
    }

    fn contains(&self, chr: u8) -> bool {
        let mut i = 0;

        loop {
            if self.count == i as u8 {
                return false;
            }

            if chr != self.keys[i] {
                i += 1;
                continue;
            }

            if self.pointers[i].is_none() {
                return false;
            }

            break;
        }

        true
    }

    fn daughters(&self) -> &[Option<Box<dyn TrieNode>>] {
        todo!()
    }

    fn walk(&self, chr: u8) -> Option<&dyn TrieNode> {
        todo!()
    }

    fn value(&self) -> Option<&dyn TrieValue> {
        todo!()
    }

    fn value_as_mut(&mut self) -> Option <&mut dyn TrieValue> {
        todo!()
    }

    fn is_filled(&self) -> bool {
        todo!()
    }

    fn insert(&mut self, chr: u8, path: &[u8], value: Box<dyn TrieValue>, should_overwrite: bool) -> bool {
        let mut i = 0;

        loop {
            if i == self.count as usize {
                self.count += 1;
                break;
            }

            if chr != self.keys[i] {
                i += 1;
                continue;
            }

            if !should_overwrite {
                return false;
            }

            break;
        }

        self.keys[i] = chr;
        self.pointers[i] = Some(Box::new(Leaf::new(path, value)));

        true
    }
}

impl AdaptiveRadixTrieNode for Node16 {
    fn id(&self) -> NodeId {
        todo!()
    }

    fn adapt(&mut self, adapt_at: u8, adaption: Adaption) {
        match adaption {
            Adaption::Promote => {
                let node = <Self as AdaptiveRadixTrieNode>::walk(self, adapt_at).unwrap();
                let new_node = node.promote();

                for i in 0..16 {
                    if adapt_at == self.keys[i] {
                        self.pointers[i] = Some(new_node);
                        break;
                    }
                }
            },

            Adaption::Splice((suffixed_path, suffix_at)) => {
                let mut node = Node4::default();
                node.path = suffixed_path;
                node.keys[0] = suffix_at;
                node.count = 1;

                for i in 0..16 {
                    if adapt_at == self.keys[i] {

                        node.pointers[0] = self.pointers[i].take();
                        let _ = self.pointers[i].replace(Box::new(node));
                        break;
                    }
                }
            },
        }
    }

    fn promote(&mut self) -> Box<dyn AdaptiveRadixTrieNode> {
        let path = self.path.clone();
        let value = self.value.take();

        let mut indexes = [-1; 256];
        for i in 0..16 {
            indexes[self.keys[i as usize] as usize] = i;
        }

        let count = self.count;

        let mut pointers = [const { None }; 48];
        for i in 0..16 {
            pointers[i] = self.pointers[i].take();
        }

        let node = Node48 {
            path,
            value,
            indexes,
            count,
            pointers,
        };

        Box::new(node)
    }

    fn walk(&mut self, chr: u8) -> Option<&mut dyn AdaptiveRadixTrieNode> {
        todo!()
    }
}

impl TrieNode for Node48 {
    fn path(&self) -> &[u8] {
        todo!()
    }

    fn contains(&self, chr: u8) -> bool {
        if -1 == self.indexes[chr as usize] {
            return false
        }

        true
    }

    fn daughters(&self) -> &[Option<Box<dyn TrieNode>>] {
        todo!()
    }

    fn walk(&self, chr: u8) -> Option<&dyn TrieNode> {
        todo!()
    }

    fn value(&self) -> Option<&dyn TrieValue> {
        todo!()
    }

    fn value_as_mut(&mut self) -> Option<&mut dyn TrieValue> {
        todo!()
    }

    fn is_filled(&self) -> bool {
        todo!()
    }

    fn insert(&mut self, chr: u8, path: &[u8], value: Box<dyn TrieValue>, should_overwrite: bool) -> bool {
        if !should_overwrite
            &&
            -1 != self.indexes[chr as usize]
        {
            return false
        }

        let leaf = Leaf::new(path, value);

        self.pointers[self.count as usize] = Some(Box::new(leaf));
        self.indexes[chr as usize] = self.count as i8;
        self.count += 1;

        true
    }
}

impl AdaptiveRadixTrieNode for Node48 {
    fn id(&self) -> NodeId {
        todo!()
    }

    fn adapt(&mut self, adapt_at: u8, adaption: Adaption) {
        match adaption {
            Adaption::Promote => {
                let node = <Self as AdaptiveRadixTrieNode>::walk(self, adapt_at).unwrap();
                let new_node = node.promote();

                let index = self.indexes[adapt_at as usize].unsigned_abs(); // little hacky
                let _ = self.pointers[index as usize].replace(new_node);
            },

            Adaption::Splice((suffixed_path, suffix_at)) => {
                let mut node = Node4::default();
                node.path = suffixed_path;
                node.keys[0] = suffix_at;
                node.count = 1;

                let index = self.indexes[adapt_at as usize].unsigned_abs() as usize;
                node.pointers[0] = self.pointers[index].take();
                let _ = self.pointers[index].replace(Box::new(node));
            },
        }
    }

    fn promote(&mut self) -> Box<dyn AdaptiveRadixTrieNode> {
        let path = self.path.clone();
        let value = self.value.take();
        let count = self.count;

        let mut pointers = [const { None }; 256];
        for chr in 0..256 {
            match self.indexes[chr] {
                -1 => continue,
                index => pointers[chr] = self.pointers[index.unsigned_abs() as usize].take(),
            }
        }

        let node = Node256 {
            path,
            value,
            count,
            pointers,
        };

        Box::new(node)
    }

    fn walk(&mut self, chr: u8) -> Option<&mut dyn AdaptiveRadixTrieNode> {
        todo!()
    }
}

impl TrieNode for Node256 {
    fn path(&self) -> &[u8] {
        todo!()
    }

    fn contains(&self, chr: u8) -> bool {
        self.pointers[chr as usize].is_some()
    }

    fn daughters(&self) -> &[Option<Box<dyn TrieNode>>] {
        todo!()
    }

    fn walk(&self, chr: u8) -> Option<&dyn TrieNode> {
        todo!()
    }

    fn value(&self) -> Option<&dyn TrieValue> {
        todo!()
    }

    fn value_as_mut(&mut self) -> Option<&mut dyn TrieValue> {
        todo!()
    }

    fn is_filled(&self) -> bool {
        false
    }

    fn insert(&mut self, chr: u8, path: &[u8], value: Box<dyn TrieValue>, should_overwrite: bool) -> bool {
        if !should_overwrite
            &&
            self.contains(chr)
        {
            return false
        }

        let leaf = Leaf::new(path, value);

        self.pointers[self.count as usize] = Some(Box::new(leaf));
        self.count += 1;

        true
    }
}

impl AdaptiveRadixTrieNode for Node256 {
    fn id(&self) -> NodeId {
        todo!()
    }

    fn adapt(&mut self, adapt_at: u8, adaption: Adaption) {
        match adaption {
            Adaption::Promote => {
                let node = <Self as AdaptiveRadixTrieNode>::walk(self, adapt_at).unwrap();
                let new_node = node.promote();

                let _ = self.pointers[adapt_at as usize].replace(new_node);
            },

            Adaption::Splice((suffixed_path, suffix_at)) => {
                let mut node = Node4::default();
                node.path = suffixed_path;
                node.keys[0] = suffix_at;
                node.count = 1;

                node.pointers[0] = self.pointers[adapt_at as usize].take();
                let _ = self.pointers[adapt_at as usize].replace(Box::new(node));
            },
        }
    }

    fn promote(&mut self) -> Box<dyn AdaptiveRadixTrieNode> {
        panic!("Node256 is the whole world. Nothing exists beyond it.")
    }

    fn walk(&mut self, chr: u8) -> Option<&mut dyn AdaptiveRadixTrieNode> {
        todo!()
    }
}
