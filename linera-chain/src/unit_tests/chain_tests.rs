// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{
    collections::{BTreeMap, BTreeSet},
    iter, thread,
    time::{Duration, Instant},
};

use assert_matches::assert_matches;
use axum::{routing::get, Router};
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, ValidatorPublicKey},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, Blob, BlockHeight, Bytecode, Epoch,
        Timestamp,
    },
    http,
    identifiers::{AccountOwner, ApplicationId, ChainId, MessageId, ModuleId},
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    system::{OpenChainConfig, Recipient},
    test_utils::{ExpectedCall, MockApplication},
    BaseRuntime, ContractRuntime, ExecutionError, ExecutionRuntimeConfig, ExecutionRuntimeContext,
    Message, MessageKind, Operation, ResourceControlPolicy, ServiceRuntime, SystemMessage,
    SystemOperation, TestExecutionRuntimeContext,
};
use linera_views::{
    context::{Context as _, MemoryContext, ViewContext},
    memory::MemoryStore,
    views::{View, ViewError},
};
use test_case::test_case;

use crate::{
    block::{Block, ConfirmedBlock},
    data_types::{
        BlockExecutionOutcome, IncomingBundle, MessageAction, MessageBundle, ProposedBlock,
    },
    test::{make_child_block, make_first_block, BlockTestExt, HttpServer, MessageTestExt},
    ChainError, ChainExecutionContext, ChainStateView,
};

impl ChainStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>:
        linera_views::context::Context + Clone + Send + Sync + 'static,
    ViewError:
        From<<MemoryContext<TestExecutionRuntimeContext> as linera_views::context::Context>::Error>,
{
    pub async fn new(chain_id: ChainId) -> Self {
        let exec_runtime_context =
            TestExecutionRuntimeContext::new(chain_id, ExecutionRuntimeConfig::default());
        let context = MemoryContext::new_for_testing(exec_runtime_context);
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

fn make_app_description() -> (ApplicationDescription, Blob, Blob) {
    let contract = Bytecode::new(b"contract".into());
    let service = Bytecode::new(b"service".into());
    let contract_blob = Blob::new_contract_bytecode(contract.compress());
    let service_blob = Blob::new_service_bytecode(service.compress());
    let vm_runtime = VmRuntime::Wasm;

    let module_id = ModuleId::new(contract_blob.id().hash, service_blob.id().hash, vm_runtime);
    (
        ApplicationDescription {
            module_id,
            creator_chain_id: admin_id(),
            block_height: BlockHeight(2),
            application_index: 0,
            required_application_ids: vec![],
            parameters: vec![],
        },
        contract_blob,
        service_blob,
    )
}

fn admin_id() -> ChainId {
    ChainId::root(0)
}

fn make_admin_message_id(height: BlockHeight) -> MessageId {
    MessageId {
        chain_id: admin_id(),
        height,
        index: 0,
    }
}

fn make_open_chain_config() -> OpenChainConfig {
    let committee = Committee::make_simple(vec![(
        ValidatorPublicKey::test_key(1),
        AccountPublicKey::test_key(1),
    )]);
    OpenChainConfig {
        ownership: ChainOwnership::single(AccountPublicKey::test_key(0).into()),
        admin_id: admin_id(),
        epoch: Epoch::ZERO,
        committees: iter::once((Epoch::ZERO, committee)).collect(),
        balance: Amount::from_tokens(10),
        application_permissions: Default::default(),
    }
}

#[tokio::test]
async fn test_block_size_limit() {
    let time = Timestamp::from(0);
    let message_id = make_admin_message_id(BlockHeight(3));
    let chain_id = ChainId::child(message_id);
    let mut chain = ChainStateView::new(chain_id).await;

    // The size of the executed valid block below.
    let maximum_block_size = 856;

    // Initialize the chain.
    let mut config = make_open_chain_config();
    config.committees.insert(
        Epoch(0),
        Committee::new(
            BTreeMap::from([(
                ValidatorPublicKey::test_key(1),
                ValidatorState {
                    network_address: ValidatorPublicKey::test_key(1).to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::test_key(1),
                },
            )]),
            ResourceControlPolicy {
                maximum_block_size,
                ..ResourceControlPolicy::default()
            },
        ),
    );

    chain
        .execute_init_message(message_id, &config, time, time)
        .await
        .unwrap();
    let open_chain_bundle = IncomingBundle {
        origin: admin_id(),
        bundle: MessageBundle {
            certificate_hash: CryptoHash::test_hash("certificate"),
            height: BlockHeight(1),
            transaction_index: 0,
            timestamp: time,
            messages: vec![Message::System(SystemMessage::OpenChain(Box::new(config)))
                .to_posted(0, MessageKind::Protected)],
        },
        action: MessageAction::Accept,
    };

    let valid_block = make_first_block(chain_id).with_incoming_bundle(open_chain_bundle.clone());

    // Any block larger than the valid block is rejected.
    let invalid_block = valid_block
        .clone()
        .with_operation(SystemOperation::Transfer {
            owner: AccountOwner::CHAIN,
            recipient: Recipient::root(0),
            amount: Amount::ONE,
        });

    let result = chain
        .execute_block(&invalid_block, time, None, &[], None)
        .await;
    assert_matches!(
        result,
        Err(ChainError::ExecutionError(
            execution_error,
            ChainExecutionContext::Operation(1),
        )) if matches!(*execution_error, ExecutionError::BlockTooLarge)
    );

    // The valid block is accepted...
    let outcome = chain
        .execute_block(&valid_block, time, None, &[], None)
        .await
        .unwrap();
    let block = Block::new(valid_block, outcome);

    // ...because its size is at the allowed limit.
    assert_eq!(
        bcs::serialized_size(&block).unwrap(),
        maximum_block_size as usize
    );
}

#[tokio::test]
async fn test_application_permissions() -> anyhow::Result<()> {
    let time = Timestamp::from(0);
    let message_id = make_admin_message_id(BlockHeight(3));
    let chain_id = ChainId::child(message_id);
    let mut chain = ChainStateView::new(chain_id).await;

    // Create a mock application.
    let (app_description, contract_blob, service_blob) = make_app_description();
    let application_id = ApplicationId::from(&app_description);
    let application = MockApplication::default();
    let extra = &chain.context().extra();
    extra
        .user_contracts()
        .insert(application_id, application.clone().into());
    extra
        .add_blobs([
            contract_blob,
            service_blob,
            Blob::new_application_description(&app_description),
        ])
        .await?;

    // Initialize the chain, with a chain application.
    let config = OpenChainConfig {
        application_permissions: ApplicationPermissions::new_single(application_id),
        ..make_open_chain_config()
    };
    chain
        .execute_init_message(message_id, &config, time, time)
        .await?;
    let open_chain_message = Message::System(SystemMessage::OpenChain(Box::new(config)));

    // The OpenChain message must be included in the first block. Also register the app.
    let bundle = IncomingBundle {
        origin: admin_id(),
        bundle: MessageBundle {
            certificate_hash: CryptoHash::test_hash("certificate"),
            height: BlockHeight(1),
            transaction_index: 0,
            timestamp: Timestamp::from(0),
            messages: vec![open_chain_message.to_posted(0, MessageKind::Protected)],
        },
        action: MessageAction::Accept,
    };

    // An operation that doesn't belong to the app isn't allowed.
    let invalid_block = make_first_block(chain_id)
        .with_incoming_bundle(bundle.clone())
        .with_simple_transfer(chain_id, Amount::ONE);
    let result = chain
        .execute_block(&invalid_block, time, None, &[], None)
        .await;
    assert_matches!(result, Err(ChainError::AuthorizedApplications(app_ids))
        if app_ids == vec![application_id]
    );

    // After registering, an app operation can already be used in the first block.
    application.expect_call(ExpectedCall::execute_operation(|_, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    let app_operation = Operation::User {
        application_id,
        bytes: b"foo".to_vec(),
    };
    let valid_block = make_first_block(chain_id)
        .with_incoming_bundle(bundle)
        .with_operation(app_operation.clone());
    let outcome = chain
        .execute_block(&valid_block, time, None, &[], None)
        .await?;
    let value = ConfirmedBlock::new(outcome.with(valid_block));
    chain.apply_confirmed_block(&value, time).await?;

    // In the second block, other operations are still not allowed.
    let invalid_block = make_child_block(&value.clone())
        .with_simple_transfer(chain_id, Amount::ONE)
        .with_operation(app_operation.clone());
    let result = chain
        .execute_block(&invalid_block, time, None, &[], None)
        .await;
    assert_matches!(result, Err(ChainError::AuthorizedApplications(app_ids))
        if app_ids == vec![application_id]
    );

    // Also, blocks without an application operation or incoming message are forbidden.
    let invalid_block = make_child_block(&value.clone());
    let result = chain
        .execute_block(&invalid_block, time, None, &[], None)
        .await;
    assert_matches!(result, Err(ChainError::MissingMandatoryApplications(app_ids))
        if app_ids == vec![application_id]
    );

    // But app operations continue to work.
    application.expect_call(ExpectedCall::execute_operation(|_, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    let valid_block = make_child_block(&value).with_operation(app_operation);
    let outcome = chain
        .execute_block(&valid_block, time, None, &[], None)
        .await?;
    let value = ConfirmedBlock::new(outcome.with(valid_block));
    chain.apply_confirmed_block(&value, time).await?;

    Ok(())
}

/// Tests if services can execute as oracles if the total execution time is less than the limit.
#[test_case(&[100]; "single service as oracle call")]
#[test_case(&[50, 50]; "two service as oracle calls")]
#[test_case(&[90, 10]; "long and short service as oracle calls")]
#[test_case(&[33, 33, 33]; "three service as oracle calls")]
#[tokio::test]
async fn test_service_as_oracles(service_oracle_execution_times_ms: &[u64]) -> anyhow::Result<()> {
    let maximum_service_oracle_execution_ms = 300;
    let service_oracle_call_count = service_oracle_execution_times_ms.len();
    let service_oracle_execution_times = service_oracle_execution_times_ms
        .iter()
        .copied()
        .map(Duration::from_millis);

    let (application, application_id, mut chain, block, time) =
        prepare_test_with_dummy_mock_application(ResourceControlPolicy {
            maximum_service_oracle_execution_ms,
            ..ResourceControlPolicy::default()
        })
        .await?;

    application.expect_call(ExpectedCall::execute_operation(move |runtime, _| {
        for _ in 0..service_oracle_call_count {
            runtime.query_service(application_id, vec![])?;
        }
        Ok(vec![])
    }));

    for service_oracle_execution_time in service_oracle_execution_times {
        application.expect_call(ExpectedCall::handle_query(move |_, _| {
            thread::sleep(service_oracle_execution_time);
            Ok(vec![])
        }));
    }

    application.expect_call(ExpectedCall::default_finalize());

    chain.execute_block(&block, time, None, &[], None).await?;

    Ok(())
}

/// Tests if execution fails if services executing as oracles exceed the time limit.
#[test_case(&[120]; "single service as oracle call")]
#[test_case(&[60, 60]; "two service as oracle calls")]
#[test_case(&[105, 15]; "long and short service as oracle calls")]
#[test_case(&[50, 50, 50]; "three service as oracle calls")]
#[test_case(&[60, 60, 60]; "first two service as oracle calls exceeds limit")]
#[tokio::test]
async fn test_service_as_oracle_exceeding_time_limit(
    service_oracle_execution_times_ms: &[u64],
) -> anyhow::Result<()> {
    let maximum_service_oracle_execution_ms = 110;
    let service_oracle_call_count = service_oracle_execution_times_ms.len();
    let service_oracle_execution_times = service_oracle_execution_times_ms
        .iter()
        .copied()
        .map(Duration::from_millis);

    let (application, application_id, mut chain, block, time) =
        prepare_test_with_dummy_mock_application(ResourceControlPolicy {
            maximum_service_oracle_execution_ms,
            ..ResourceControlPolicy::default()
        })
        .await?;

    application.expect_call(ExpectedCall::execute_operation(move |runtime, _| {
        for _ in 0..service_oracle_call_count {
            runtime.query_service(application_id, vec![])?;
        }
        Ok(vec![])
    }));

    for service_oracle_execution_time in service_oracle_execution_times {
        application.expect_call(ExpectedCall::handle_query(move |_, _| {
            thread::sleep(service_oracle_execution_time);
            Ok(vec![])
        }));
    }

    application.expect_call(ExpectedCall::default_finalize());

    let result = chain.execute_block(&block, time, None, &[], None).await;

    let Err(ChainError::ExecutionError(execution_error, ChainExecutionContext::Operation(0))) =
        result
    else {
        panic!("Expected a block execution error, got: {result:#?}");
    };

    assert_matches!(
        *execution_error,
        ExecutionError::MaximumServiceOracleExecutionTimeExceeded
    );

    Ok(())
}

/// Tests if execution fails early if services call `check_execution_time`.
#[test_case(&[1200]; "single service as oracle call")]
#[test_case(&[600, 600]; "two service as oracle calls")]
#[test_case(&[1050, 150]; "long and short service as oracle calls")]
#[test_case(&[500, 500, 500]; "three service as oracle calls")]
#[test_case(&[600, 600, 600]; "first two service as oracle calls exceeds limit")]
#[tokio::test]
async fn test_service_as_oracle_timeout_early_stop(
    service_oracle_execution_times_ms: &[u64],
) -> anyhow::Result<()> {
    let maximum_service_oracle_execution_ms = 700;
    let poll_interval = Duration::from_millis(100);
    let maximum_expected_execution_time =
        Duration::from_millis(maximum_service_oracle_execution_ms) + 2 * poll_interval;

    let service_oracle_call_count = service_oracle_execution_times_ms.len();
    let service_oracle_execution_times = service_oracle_execution_times_ms
        .iter()
        .copied()
        .map(Duration::from_millis);

    let (application, application_id, mut chain, block, time) =
        prepare_test_with_dummy_mock_application(ResourceControlPolicy {
            maximum_service_oracle_execution_ms,
            ..ResourceControlPolicy::default()
        })
        .await?;

    application.expect_call(ExpectedCall::execute_operation(move |runtime, _| {
        for _ in 0..service_oracle_call_count {
            runtime.query_service(application_id, vec![])?;
        }
        Ok(vec![])
    }));

    for service_oracle_execution_time in service_oracle_execution_times {
        application.expect_call(ExpectedCall::handle_query(move |runtime, _| {
            let execution_time = Instant::now();
            while execution_time.elapsed() < service_oracle_execution_time {
                runtime.check_execution_time()?;
                thread::sleep(poll_interval);
            }
            Ok(vec![])
        }));
    }

    application.expect_call(ExpectedCall::default_finalize());

    let execution_start = Instant::now();
    let result = chain.execute_block(&block, time, None, &[], None).await;
    let execution_time = execution_start.elapsed();

    let Err(ChainError::ExecutionError(execution_error, ChainExecutionContext::Operation(0))) =
        result
    else {
        panic!("Expected a block execution error, got: {result:#?}");
    };

    assert_matches!(
        *execution_error,
        ExecutionError::MaximumServiceOracleExecutionTimeExceeded
    );

    assert!(execution_time <= maximum_expected_execution_time);

    Ok(())
}

/// Tests service-as-oracle response size limit.
#[test_case(50, 49 => matches Ok(_); "smaller than limit")]
#[test_case(
    50, 51
    => matches Err(ChainError::ExecutionError(execution_error, _))
        if matches!(*execution_error, ExecutionError::ServiceOracleResponseTooLarge);
    "larger than limit"
)]
#[tokio::test]
async fn test_service_as_oracle_response_size_limit(
    limit: u64,
    response_size: usize,
) -> Result<BlockExecutionOutcome, ChainError> {
    let (application, application_id, mut chain, block, time) =
        prepare_test_with_dummy_mock_application(ResourceControlPolicy {
            maximum_oracle_response_bytes: limit,
            ..ResourceControlPolicy::default()
        })
        .await
        .expect("Failed to set up test with mock application");

    application.expect_call(ExpectedCall::execute_operation(move |runtime, _| {
        runtime.query_service(application_id, vec![])?;
        Ok(vec![])
    }));

    application.expect_call(ExpectedCall::handle_query(move |_runtime, _| {
        Ok(vec![0; response_size])
    }));

    application.expect_call(ExpectedCall::default_finalize());

    chain.execute_block(&block, time, None, &[], None).await
}

/// Tests contract HTTP response size limit.
#[test_case(150, 140, 139 => matches Ok(_); "smaller than both limits")]
#[test_case(
    150, 140, 141
    => matches Err(ChainError::ExecutionError(execution_error, _))
        if matches!(*execution_error, ExecutionError::HttpResponseSizeLimitExceeded { .. });
    "larger than http limit"
)]
#[test_case(
    140, 150, 142
    => matches Err(ChainError::ExecutionError(execution_error, _))
        if matches!(*execution_error, ExecutionError::HttpResponseSizeLimitExceeded { .. });
    "larger than oracle limit"
)]
#[test_case(
    140, 150, 1000
    => matches Err(ChainError::ExecutionError(execution_error, _))
        if matches!(*execution_error, ExecutionError::HttpResponseSizeLimitExceeded { .. });
    "larger than both limits"
)]
#[tokio::test]
async fn test_contract_http_response_size_limit(
    oracle_limit: u64,
    http_limit: u64,
    response_size: usize,
) -> Result<BlockExecutionOutcome, ChainError> {
    let response_header_size = 84;
    let response_body_size = response_size - response_header_size;

    let http_server = HttpServer::start(Router::new().route(
        "/",
        get(move || async move { vec![b'a'; response_body_size] }),
    ))
    .await
    .expect("Failed to start test HTTP server");

    let (application, _application_id, mut chain, block, time) =
        prepare_test_with_dummy_mock_application(ResourceControlPolicy {
            maximum_oracle_response_bytes: oracle_limit,
            maximum_http_response_bytes: http_limit,
            http_request_allow_list: BTreeSet::from_iter([http_server.hostname()]),
            ..ResourceControlPolicy::default()
        })
        .await
        .expect("Failed to set up test with mock application");

    application.expect_call(ExpectedCall::execute_operation(move |runtime, _| {
        runtime.perform_http_request(http::Request::get(http_server.url()))?;
        Ok(vec![])
    }));

    application.expect_call(ExpectedCall::default_finalize());

    chain.execute_block(&block, time, None, &[], None).await
}

/// Tests service HTTP response size limit.
#[test_case(150, 140, 139 => matches Ok(_); "smaller than both limits")]
#[test_case(140, 150, 142 => matches Ok(_); "larger than oracle limit")]
#[test_case(
    150, 140, 141
    => matches Err(ChainError::ExecutionError(execution_error, _))
        if matches!(*execution_error, ExecutionError::HttpResponseSizeLimitExceeded { .. });
    "larger than http limit"
)]
#[test_case(
    140, 150, 1000
    => matches Err(ChainError::ExecutionError(execution_error, _))
        if matches!(*execution_error, ExecutionError::HttpResponseSizeLimitExceeded { .. });
    "larger than both limits"
)]
#[tokio::test]
async fn test_service_http_response_size_limit(
    oracle_limit: u64,
    http_limit: u64,
    response_size: usize,
) -> Result<BlockExecutionOutcome, ChainError> {
    let response_header_size = 84;
    let response_body_size = response_size - response_header_size;

    let http_server = HttpServer::start(Router::new().route(
        "/",
        get(move || async move { vec![b'a'; response_body_size] }),
    ))
    .await
    .expect("Failed to start test HTTP server");

    let (application, application_id, mut chain, block, time) =
        prepare_test_with_dummy_mock_application(ResourceControlPolicy {
            maximum_oracle_response_bytes: oracle_limit,
            maximum_http_response_bytes: http_limit,
            http_request_allow_list: BTreeSet::from_iter([http_server.hostname()]),
            ..ResourceControlPolicy::default()
        })
        .await
        .expect("Failed to set up test with mock application");

    application.expect_call(ExpectedCall::execute_operation(move |runtime, _| {
        runtime.query_service(application_id, vec![])?;
        Ok(vec![])
    }));

    application.expect_call(ExpectedCall::handle_query(move |runtime, _| {
        runtime.perform_http_request(http::Request::get(http_server.url()))?;
        Ok(vec![])
    }));

    application.expect_call(ExpectedCall::default_finalize());

    chain.execute_block(&block, time, None, &[], None).await
}

/// Sets up a test with a dummy [`MockApplication`].
///
/// Creates and initializes a [`ChainStateView`] configured with the
/// `maximum_service_oracle_execution_ms` policy. Registers the dummy application on the chain, and
/// creates a block proposal with a dummy operation.
async fn prepare_test_with_dummy_mock_application(
    policy: ResourceControlPolicy,
) -> anyhow::Result<(
    MockApplication,
    ApplicationId,
    ChainStateView<ViewContext<TestExecutionRuntimeContext, MemoryStore>>,
    ProposedBlock,
    Timestamp,
)> {
    let time = Timestamp::from(0);
    let message_id = make_admin_message_id(BlockHeight(3));
    let chain_id = ChainId::child(message_id);
    let mut chain = ChainStateView::new(chain_id).await;

    let mut config = make_open_chain_config();
    config.committees.insert(
        Epoch(0),
        Committee::new(
            BTreeMap::from([(
                ValidatorPublicKey::test_key(1),
                ValidatorState {
                    network_address: ValidatorPublicKey::test_key(1).to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::test_key(1),
                },
            )]),
            policy,
        ),
    );

    chain
        .execute_init_message(message_id, &config, time, time)
        .await?;

    // Create a mock application.
    let (app_description, contract_blob, service_blob) = make_app_description();
    let application_id = ApplicationId::from(&app_description);
    let application = MockApplication::default();
    let extra = &chain.context().extra();
    extra
        .user_contracts()
        .insert(application_id, application.clone().into());
    extra
        .user_services()
        .insert(application_id, application.clone().into());
    extra
        .add_blobs([
            contract_blob,
            service_blob,
            Blob::new_application_description(&app_description),
        ])
        .await?;

    let block = make_first_block(chain_id).with_operation(Operation::User {
        application_id,
        bytes: vec![],
    });

    Ok((application, application_id, chain, block, time))
}
