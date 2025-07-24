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
    pub trait Signer = linera_base::crypto::Signer + AutoTraits;
    pub trait Storage = linera_storage::Storage + Clone + Send + Sync + 'static;
}

pub trait Environment: AutoTraits {
    type Storage: Storage<Context = Self::StorageContext>;
    type Network: Network<Node = Self::ValidatorNode>;
    type Signer: Signer;
    type ValidatorNode: crate::node::ValidatorNode + AutoTraits + Clone;
    type StorageContext: linera_views::context::Context<Extra: linera_execution::ExecutionRuntimeContext>
        + Send
        + Sync
        + 'static;

    fn storage(&self) -> &Self::Storage;
    fn network(&self) -> &Self::Network;
    fn signer(&self) -> &Self::Signer;
}

pub struct Impl<Storage, Network, Signer> {
    pub storage: Storage,
    pub network: Network,
    pub signer: Signer,
}

impl<St: Storage, N: Network, Si: Signer> Environment for Impl<St, N, Si> {
    type Storage = St;
    type Network = N;
    type Signer = Si;
    type ValidatorNode = N::Node;
    type StorageContext = St::Context;

    fn storage(&self) -> &St {
        &self.storage
    }

    fn network(&self) -> &N {
        &self.network
    }

    fn signer(&self) -> &Si {
        &self.signer
    }
}

cfg_if::cfg_if! {
    if #[cfg(with_testing)] {
        pub type TestStorage = linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::TestClock>;
        pub type TestNetwork = crate::test_utils::NodeProvider<TestStorage>;
        pub type TestSigner = linera_base::crypto::InMemorySigner;
        pub type Test = Impl<TestStorage, TestNetwork, TestSigner>;
    }
}
