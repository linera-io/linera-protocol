use std::{fmt::Debug, sync::Arc};

use linera_base::identifiers::{hash_bytes, Account, Metadata};
use tokio::sync::{Mutex, MutexGuard};

/// can be cheaply cloned.
#[derive(Debug, Clone)]
pub struct GlobalContext {
    pub assets: Arc<dyn Trie>,
}

type Lock = Mutex<()>; // Just for now.
type Guard<'a> = MutexGuard<'a, ()>;
type Key = [u8; 32];

/// Placeholder trait
pub trait Trie: Send + Sync + Debug {
    // Since this will be a very throughput intesnive memory location, it has to
    // be made into a lock-free data structure. Something like Solana's Concurrent Tree.
    fn lock<'a>(&'a self) -> Guard<'a>;

    fn contains(&self, key: &Key) -> bool;

    fn write(&self, key: &Key, value: Metadata, owner: Account) -> bool;

    fn update(&self, key: &Key, owner: Account) -> bool;

    fn read(&self, key: &Key) -> Option<(Account, Metadata)>;
}

pub fn get_asset_key(key: &[u8]) -> Key {
    let key  = hash_bytes(key);
    <[u8; 32]>::try_from(&key.as_bytes()[..]).unwrap()
}

impl GlobalContext {
    pub fn new() -> Self {
        todo!()
    }

    pub fn persist(&mut self) {
        // TODO Manually serialize and make a view out of it
        todo!()
    }
}
