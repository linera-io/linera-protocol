// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(web)]
pub trait AutoTraits: 'static {}
#[cfg(web)]
impl<T: 'static> AutoTraits for T {}

#[cfg(not(web))]
trait_set::trait_set! {
    pub trait AutoTraits = Send + Sync + 'static;
}

trait_set::trait_set! {
    pub trait Network = crate::node::ValidatorNodeProvider + AutoTraits;
    pub trait Storage = linera_storage::Storage + Clone + Send + Sync + 'static;
}

pub trait Environment: AutoTraits {
    type Storage: Storage<Context = Self::StorageContext>;
    type Network: Network<Node = Self::ValidatorNode>;
    type ValidatorNode: crate::node::ValidatorNode + AutoTraits + Clone;
    type StorageContext: linera_views::context::Context<Extra: linera_execution::ExecutionRuntimeContext>
        + Send
        + Sync
        + 'static;

    fn storage(&self) -> &Self::Storage;
    fn network(&self) -> &Self::Network;
}

pub struct Impl<Storage, Network> {
    pub storage: Storage,
    pub network: Network,
}

impl<S: Storage, N: Network> Environment for Impl<S, N> {
    type Storage = S;
    type Network = N;
    type ValidatorNode = N::Node;
    type StorageContext = S::Context;

    fn storage(&self) -> &S {
        &self.storage
    }

    fn network(&self) -> &N {
        &self.network
    }
}

cfg_if::cfg_if! {
    if #[cfg(with_testing)] {
        pub type TestStorage = linera_storage::DbStorage<linera_views::memory::MemoryStore, linera_storage::TestClock>;
        pub type TestNetwork = crate::test_utils::NodeProvider<TestStorage>;
        pub type Test = Impl<TestStorage, TestNetwork>;
    }
}
