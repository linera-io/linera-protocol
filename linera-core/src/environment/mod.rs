// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod wallet;

use linera_base::util::traits::AutoTraits;

trait_set::trait_set! {
    pub trait Network = crate::node::ValidatorNodeProvider + AutoTraits;
    pub trait Signer = linera_base::crypto::Signer + AutoTraits;
    // TODO(#5064): we shouldn't hard-code `Send` + `Sync` here
    pub trait Storage = linera_storage::Storage + Clone + AutoTraits;
    pub trait Wallet = wallet::Wallet + AutoTraits;
}

pub trait Environment: AutoTraits {
    type Storage: Storage<Context = Self::StorageContext>;
    type Network: Network<Node = Self::ValidatorNode>;
    type Signer: Signer;
    type Wallet: Wallet;

    type ValidatorNode: crate::node::ValidatorNode + AutoTraits + Clone;
    // TODO(#5064): we shouldn't hard-code `Send` + `Sync` here
    type StorageContext: linera_views::context::Context<Extra: linera_execution::ExecutionRuntimeContext>
        + AutoTraits;

    fn storage(&self) -> &Self::Storage;
    fn network(&self) -> &Self::Network;
    fn signer(&self) -> &Self::Signer;
    fn wallet(&self) -> &Self::Wallet;
}

pub struct Impl<
    Storage,
    Network,
    Signer = linera_base::crypto::InMemorySigner,
    Wallet = wallet::Memory,
> {
    pub storage: Storage,
    pub network: Network,
    pub signer: Signer,
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
        pub type TestStorage = linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::TestClock>;
        pub type TestNetwork = crate::test_utils::NodeProvider<TestStorage>;
        pub type TestSigner = linera_base::crypto::InMemorySigner;
        pub type TestWallet = crate::wallet::Memory;
        pub type Test = Impl<TestStorage, TestNetwork, TestSigner, TestWallet>;
    }
}
