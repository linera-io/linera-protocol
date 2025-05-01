// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `linera-base` types.

use std::fmt::Debug;

use linera_witty::{Layout, WitLoad, WitStore};
use test_case::test_case;

use crate::{
    crypto::{AccountPublicKey, CryptoHash},
    data_types::{Amount, BlockHeight, Resources, SendMessageRequest, TimeDelta, Timestamp},
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, MessageId, ModuleId},
    ownership::{ChainOwnership, TimeoutConfig},
    vm::VmRuntime,
};

/// Test roundtrip of types used in the WIT interface.
#[test_case(CryptoHash::test_hash("hash"); "of_crypto_hash")]
#[test_case(AccountPublicKey::test_key(255); "of_public_key")]
#[test_case(Amount::from_tokens(500); "of_amount")]
#[test_case(BlockHeight(1095); "of_block_height")]
#[test_case(Timestamp::from(6_400_003); "of_timestamp")]
#[test_case(resources_test_case(); "of_resources")]
#[test_case(send_message_request_test_case(); "of_send_message_request")]
#[test_case(AccountOwner::from(CryptoHash::test_hash("owner")); "of_owner")]
#[test_case(account_test_case(); "of_account")]
#[test_case(ChainId(CryptoHash::test_hash("chain_id")); "of_chain_id")]
#[test_case(message_id_test_case(); "of_message_id")]
#[test_case(application_id_test_case(); "of_application_id")]
#[test_case(module_id_test_case(); "of_module_id")]
#[test_case(timeout_config_test_case(); "of_timeout_config")]
#[test_case(chain_ownership_test_case(); "of_chain_ownership")]
fn test_wit_roundtrip<T>(input: T)
where
    T: Debug + Eq + WitLoad + WitStore,
    <T::Layout as Layout>::Flat: Copy + Debug + Eq,
{
    linera_witty::test::test_memory_roundtrip(&input).expect("Memory WIT roundtrip test failed");
    linera_witty::test::test_flattening_roundtrip(&input)
        .expect("Flattening WIT roundtrip test failed");
}

/// Creates a dummy [`Resources`] instance to use for the WIT roundtrip test.
fn resources_test_case() -> Resources {
    Resources {
        bytes_to_read: 1_474_560,
        bytes_to_write: 571,
        blobs_to_read: 71,
        blobs_to_publish: 73,
        blob_bytes_to_read: 67,
        blob_bytes_to_publish: 71,
        fuel: 1_000,
        message_size: 4,
        messages: 93,
        read_operations: 12,
        write_operations: 2,
        storage_size_delta: 700_000_000,
        service_as_oracle_queries: 7,
        http_requests: 3,
    }
}

/// Creates a dummy [`SendMessageRequest`] instance to use for the WIT roundtrip test.
fn send_message_request_test_case() -> SendMessageRequest<Vec<u8>> {
    SendMessageRequest {
        authenticated: true,
        is_tracked: false,
        destination: ChainId(CryptoHash::test_hash("chain_id_0")),
        grant: Resources {
            bytes_to_read: 200,
            bytes_to_write: 0,
            blobs_to_read: 100,
            blobs_to_publish: 1000,
            blob_bytes_to_read: 10,
            blob_bytes_to_publish: 100,
            fuel: 8,
            message_size: 1,
            messages: 0,
            read_operations: 1,
            write_operations: 0,
            storage_size_delta: 0,
            service_as_oracle_queries: 0,
            http_requests: 0,
        },
        message: (0..=255).cycle().take(2_000).collect(),
    }
}

/// Creates a dummy [`Account`] instance to use for the WIT roundtrip test.
fn account_test_case() -> Account {
    Account {
        chain_id: ChainId(CryptoHash::test_hash("chain_id_10")),
        owner: AccountOwner::from(CryptoHash::test_hash("account")),
    }
}

/// Creates a dummy [`MessageId`] instance to use for the WIT roundtrip test.
fn message_id_test_case() -> MessageId {
    MessageId {
        chain_id: ChainId(CryptoHash::test_hash("chain_id_3")),
        height: BlockHeight(9_812_394),
        index: 7,
    }
}

/// Creates a dummy [`ApplicationId`] instance to use for the WIT roundtrip test.
fn application_id_test_case() -> ApplicationId {
    ApplicationId::new(CryptoHash::test_hash("application description"))
}

/// Creates a dummy [`ModuleId`] instance to use for the WIT roundtrip test.
fn module_id_test_case() -> ModuleId {
    ModuleId::new(
        CryptoHash::test_hash("another contract bytecode"),
        CryptoHash::test_hash("another service bytecode"),
        VmRuntime::Wasm,
    )
}

/// Creates a dummy [`TimeoutConfig`] instance to use for the WIT roundtrip test.
fn timeout_config_test_case() -> TimeoutConfig {
    TimeoutConfig {
        fast_round_duration: Some(TimeDelta::from_micros(20)),
        base_timeout: TimeDelta::from_secs(4),
        timeout_increment: TimeDelta::from_millis(125),
        fallback_duration: TimeDelta::from_secs(1_000),
    }
}

/// Creates a dummy [`ChainOwnership`] instance to use for the WIT roundtrip test.
fn chain_ownership_test_case() -> ChainOwnership {
    let super_owners = ["Alice", "Bob"]
        .into_iter()
        .map(|owner_name| AccountOwner::from(CryptoHash::test_hash(owner_name)))
        .collect();

    let owners = ["Carol", "Dennis", "Eve"]
        .into_iter()
        .enumerate()
        .map(|(index, owner_name)| {
            (
                AccountOwner::from(CryptoHash::test_hash(owner_name)),
                index as u64,
            )
        })
        .collect();

    ChainOwnership {
        super_owners,
        owners,
        multi_leader_rounds: 5,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig {
            fast_round_duration: None,
            base_timeout: TimeDelta::ZERO,
            timeout_increment: TimeDelta::from_secs(3_600),
            fallback_duration: TimeDelta::from_secs(10_000),
        },
    }
}
