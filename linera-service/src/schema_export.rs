// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// use std::{num::NonZeroUsize, sync::Arc, time::Duration};

// use linera_client::{
//     chain_listener::ChainListenerConfig,
//     client_context::ClientContext,
//     config::{GenesisConfig, WalletState},
//     persistent::Memory,
//     wallet::Wallet,
// };
// use linera_core::{
//     client::{BlanketMessagePolicy, Client},
//     join_set_ext::JoinSet,
//     node::CrossChainMessageDelivery,
//     test_utils::{MemoryStorageBuilder, NodeProvider, StorageBuilder},
//     DEFAULT_GRACE_PERIOD,
// };
// use linera_service::node_service::NodeService;
// use linera_storage::{DbStorage, WallClock};
// use linera_views::memory::{MemoryStore, MemoryStoreConfig, TEST_MEMORY_MAX_STREAM_QUERIES};

// #[derive(clap::Parser)]
// #[command(
//     name = "linera-schema-export",
//     about = "Export the GraphQL schema for the core data in a Linera chain",
//     version = linera_version::VersionInfo::default_clap_str(),
// )]
// struct Options {}

// type TestStorage = DbStorage<MemoryStore, WallClock>;
// type TestProvider = NodeProvider<TestStorage>;

// async fn new_dummy_client_context() -> ClientContext<TestProvider, TestStorage, Memory<Wallet>> {
//     // let storage_builder = MemoryStorageBuilder::default();
//     // let storage = storage_builder.build().await.expect("storage");
//     // let wallet = Memory::new(Wallet::new(GenesisConfig::default(), Some(37)));

//     // let client = Arc::new(Client::new(
//     //     builder.make_node_provider(),
//     //     storage.clone(),
//     //     10,
//     //     CrossChainMessageDelivery::NonBlocking,
//     //     false,
//     //     [chain_id0],
//     //     format!("Client node for {:.8}", chain_id0),
//     //     NonZeroUsize::new(20).expect("Chain worker LRU cache size must be non-zero"),
//     //     DEFAULT_GRACE_PERIOD,
//     //     Duration::from_secs(1),
//     // ));

//     // ClientContext {
//     //     wallet: WalletState::new(wallet),
//     //     client,
//     //     send_timeout: Duration::from_millis(1000),
//     //     recv_timeout: Duration::from_millis(1000),
//     //     retry_delay: Duration::from_millis(10),
//     //     max_retries: 10,
//     //     chain_listeners: JoinSet::new(),
//     //     blanket_message_policy: BlanketMessagePolicy::Accept,
//     //     restrict_chain_ids_to: None,
//     // }
//     unimplemented!()
// }

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // let _options = <Options as clap::Parser>::parse();

    // let store_config = MemoryStoreConfig::new(TEST_MEMORY_MAX_STREAM_QUERIES);
    // let namespace = "schema_export";
    // let storage =
    //     DbStorage::<MemoryStore, _>::maybe_create_and_connect(&store_config, namespace, None)
    //         .await
    //         .expect("storage");
    // let config = ChainListenerConfig::default();
    // let context = new_dummy_client_context().await;
    // let service = NodeService::new(
    //     config,
    //     std::num::NonZeroU16::new(8080).unwrap(),
    //     None,
    //     storage,
    //     context,
    // )
    // .await;
    // let schema = service.schema().sdl();
    // print!("{}", schema);
    Ok(())
}
