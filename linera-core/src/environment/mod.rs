// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The runtime environment (storage, network, signer and wallet) used by the client.

/// The wallet abstraction tracking the set of chains followed by the client.
pub mod wallet;

use linera_base::util::traits::AutoTraits;

trait_set::trait_set! {
    /// The provider of validator nodes used to communicate with the network.
    pub trait Network = crate::node::ValidatorNodeProvider + AutoTraits;
    /// The signer used to sign blocks and other messages on behalf of chain owners.
    pub trait Signer = linera_base::crypto::Signer + AutoTraits;
    /// The persistent storage backend for chains and certificates.
    pub trait Storage = linera_storage::Storage + Clone + AutoTraits;
    /// The wallet tracking the set of chains followed by the client.
    pub trait Wallet = wallet::Wallet + AutoTraits;
}

/// The collection of services (storage, network, signer and wallet) available to the client.
pub trait Environment: AutoTraits {
    /// The persistent storage backend.
    type Storage: Storage<Context = Self::StorageContext>;
    /// The provider of validator nodes.
    type Network: Network<Node = Self::ValidatorNode>;
    /// The signer used to sign on behalf of chain owners.
    type Signer: Signer;
    /// The wallet tracking the chains followed by the client.
    type Wallet: Wallet;

    /// The validator node type produced by the network provider.
    type ValidatorNode: crate::node::ValidatorNode + AutoTraits + Clone;
    /// The storage context used by the storage backend.
    type StorageContext: linera_views::context::Context<Extra: linera_execution::ExecutionRuntimeContext>
        + AutoTraits;

    /// Returns a reference to the storage backend.
    fn storage(&self) -> &Self::Storage;
    /// Returns a reference to the network provider.
    fn network(&self) -> &Self::Network;
    /// Returns a reference to the signer.
    fn signer(&self) -> &Self::Signer;
    /// Returns a reference to the wallet.
    fn wallet(&self) -> &Self::Wallet;
}

/// A concrete [`Environment`] assembled from its individual components.
pub struct Impl<
    Storage,
    Network,
    Signer = linera_base::crypto::InMemorySigner,
    Wallet = wallet::Memory,
> {
    /// The persistent storage backend.
    pub storage: Storage,
    /// The provider of validator nodes.
    pub network: Network,
    /// The signer used to sign on behalf of chain owners.
    pub signer: Signer,
    /// The wallet tracking the chains followed by the client.
    pub wallet: Wallet,
}

impl<St: Storage, N: Network, Si: Signer, W: Wallet> Environment for Impl<St, N, Si, W> {
    type Storage = St;
    type Network = N;
    type Signer = Si;
    type Wallet = W;

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

    fn wallet(&self) -> &W {
        &self.wallet
    }
}

cfg_if::cfg_if! {
    if #[cfg(with_testing)] {
        /// An in-memory storage backend used in tests.
        pub type TestStorage = linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::TestClock>;
        /// A network provider backed by in-memory test nodes.
        pub type TestNetwork = crate::test_utils::NodeProvider<TestStorage>;
        /// An in-memory signer used in tests.
        pub type TestSigner = linera_base::crypto::InMemorySigner;
        /// An in-memory wallet used in tests.
        pub type TestWallet = crate::wallet::Memory;
        /// A fully in-memory [`Environment`] used in tests.
        pub type Test = Impl<TestStorage, TestNetwork, TestSigner, TestWallet>;
    }
}
