// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
};

use assert_matches::assert_matches;
use axum::{routing::get, Router};
use linera_base::{
    crypto::{AccountPublicKey, ValidatorPublicKey},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, Blob, BlockHeight, Bytecode,
        ChainDescription, ChainOrigin, Epoch, InitialChainConfig, Timestamp,
    },
    http,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, ModuleId},
    ownership::ChainOwnership,
    time::{Duration, Instant},
    vm::VmRuntime,
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    test_utils::{ExpectedCall, MockApplication},
    BaseRuntime, ContractRuntime, ExecutionError, ExecutionRuntimeConfig, ExecutionRuntimeContext,
    Operation, ResourceControlPolicy, ServiceRuntime, SystemOperation, TestExecutionRuntimeContext,
};
use linera_views::{
    context::{Context as _, MemoryContext, ViewContext},
    memory::MemoryStore,
    views::View,
};
use test_case::test_case;

use crate::{
    block::{Block, ConfirmedBlock},
    data_types::{BlockExecutionOutcome, ProposedBlock},
    test::{make_child_block, make_first_block, BlockTestExt, HttpServer},
    ChainError, ChainExecutionContext, ChainStateView,
};

impl ChainStateView<MemoryContext<TestExecutionRuntimeContext>> {
    pub async fn new(chain_id: ChainId) -> Self {
        let exec_runtime_context =
            TestExecutionRuntimeContext::new(chain_id, ExecutionRuntimeConfig::default());
        let context = MemoryContext::new_for_testing(exec_runtime_context);
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

struct TestEnvironment {
    admin_chain_description: ChainDescription,
    created_descriptions: BTreeMap<ChainId, ChainDescription>,
}

impl TestEnvironment {
    fn new() -> Self {
        let config = InitialChainConfig {
            ownership: ChainOwnership::single(AccountPublicKey::test_key(0).into()),
            epoch: Epoch::ZERO,
            min_active_epoch: Epoch::ZERO,
            max_active_epoch: Epoch::ZERO,
            balance: Amount::from_tokens(10),
            application_permissions: Default::default(),
        };
        let origin = ChainOrigin::Root(0);
        let admin_chain_description = ChainDescription::new(origin, config, Default::default());
        let admin_chain_id = admin_chain_description.id();
        Self {
            admin_chain_description: admin_chain_description.clone(),
            created_descriptions: [(admin_chain_id, admin_chain_description)]
                .into_iter()
                .collect(),
        }
    }

    fn admin_chain_id(&self) -> ChainId {
        self.admin_chain_description.id()
    }

    fn description_blobs(&self) -> impl Iterator<Item = Blob> + '_ {
        self.created_descriptions
            .values()
            .map(Blob::new_chain_description)
    }

    fn make_open_chain_config(&self) -> InitialChainConfig {
        self.admin_chain_description.config().clone()
    }

    fn make_app_description(&self) -> (ApplicationDescription, Blob, Blob) {
        let contract = Bytecode::new(b"contract".into());
        let service = Bytecode::new(b"service".into());
        self.make_app_from_bytecodes(contract, service)
    }

    fn make_app_from_bytecodes(
        &self,
        contract: Bytecode,
        service: Bytecode,
    ) -> (ApplicationDescription, Blob, Blob) {
        let contract_blob = Blob::new_contract_bytecode(contract.compress());
        let service_blob = Blob::new_service_bytecode(service.compress());
        let vm_runtime = VmRuntime::Wasm;

        let module_id = ModuleId::new(contract_blob.id().hash, service_blob.id().hash, vm_runtime);
        (
            ApplicationDescription {
                module_id,
                creator_chain_id: self.admin_chain_id(),
                block_height: BlockHeight(2),
                application_index: 0,
                required_application_ids: vec![],
                parameters: vec![],
            },
            contract_blob,
            service_blob,
        )
    }

    fn make_child_chain_description_with_config(
        &mut self,
        height: u64,
        config: InitialChainConfig,
    ) -> ChainDescription {
        let origin = ChainOrigin::Child {
            parent: self.admin_chain_id(),
            block_height: BlockHeight(height),
            chain_index: 0,
        };
        let description = ChainDescription::new(origin, config, Timestamp::from(0));
        self.created_descriptions
            .insert(description.id(), description.clone());
        description
    }
}

fn committee_blob(policy: ResourceControlPolicy) -> Blob {
    let committee = Committee::new(
        BTreeMap::from([(
            ValidatorPublicKey::test_key(1),
            ValidatorState {
                network_address: ValidatorPublicKey::test_key(1).to_string(),
                votes: 1,
                account_public_key: AccountPublicKey::test_key(1),
            },
        )]),
        policy,
    );
    Blob::new_committee(bcs::to_bytes(&committee).expect("serializing a committee should succeed"))
}

#[tokio::test]
async fn test_block_size_limit() -> anyhow::Result<()> {
    let mut env = TestEnvironment::new();

    let time = Timestamp::from(0);

    // The size of the executed valid block below.
    let maximum_block_size = 260;

    let config = env.make_open_chain_config();

    let chain_desc = env.make_child_chain_description_with_config(3, config);
    let chain_id = chain_desc.id();
    let owner = chain_desc
        .config()
        .ownership
        .all_owners()
        .next()
        .copied()
        .unwrap();

    let mut chain = ChainStateView::new(chain_id).await;
    let policy = ResourceControlPolicy {
        maximum_block_size,
        ..ResourceControlPolicy::default()
    };
    chain
        .context()
        .extra()
        .add_blobs([committee_blob(policy)])
        .await?;
    chain
        .context()
        .extra()
        .add_blobs(env.description_blobs())
        .await?;

    // Initialize the chain.

    chain.initialize_if_needed(time).await.unwrap();

    let valid_block = make_first_block(chain_id)
        .with_authenticated_owner(Some(owner))
        .with_operation(SystemOperation::Transfer {
            owner: AccountOwner::CHAIN,
            recipient: Account::chain(env.admin_chain_id()),
            amount: Amount::ONE,
        });

    // Any block larger than the valid block is rejected.
    let invalid_block = valid_block
        .clone()
        .with_operation(SystemOperation::Transfer {
            owner: AccountOwner::CHAIN,
            recipient: Account::chain(env.admin_chain_id()),
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
    let (outcome, _) = chain
        .execute_block(&valid_block, time, None, &[], None)
        .await
        .unwrap();
    let block = Block::new(valid_block, outcome);

    // ...because its size is at the allowed limit.
    assert_eq!(
        bcs::serialized_size(&block).unwrap(),
        maximum_block_size as usize
    );

    Ok(())
}

#[tokio::test]
async fn test_application_permissions() -> anyhow::Result<()> {
    let mut env = TestEnvironment::new();

    let time = Timestamp::from(0);

    // Create a mock application.
    let (app_description, contract_blob, service_blob) = env.make_app_description();
    let application_id = ApplicationId::from(&app_description);
    let application = MockApplication::default();

    let (another_app, another_contract, another_service) = env.make_app_from_bytecodes(
        Bytecode::new(b"contractB".into()),
        Bytecode::new(b"serviceB".into()),
    );
    let another_app_id = ApplicationId::from(&another_app);

    let config = InitialChainConfig {
        application_permissions: ApplicationPermissions::new_multiple(vec![
            application_id,
            another_app_id,
        ]),
        ..env.make_open_chain_config()
    };
    let chain_desc = env.make_child_chain_description_with_config(3, config);
    let chain_id = chain_desc.id();

    let mut chain = ChainStateView::new(chain_id).await;

    let context = chain.context();
    let extra = context.extra();
    {
        let pinned = extra.user_contracts().pin();
        pinned.insert(application_id, application.clone().into());
        pinned.insert(another_app_id, application.clone().into());
    }

    extra
        .add_blobs([committee_blob(Default::default())])
        .await?;
    extra.add_blobs(env.description_blobs()).await?;
    extra
        .add_blobs([
            contract_blob,
            service_blob,
            Blob::new_application_description(&app_description),
        ])
        .await?;
    extra
        .add_blobs([
            another_contract,
            another_service,
            Blob::new_application_description(&another_app),
        ])
        .await?;

    // Initialize the chain, with a chain application.
    chain.initialize_if_needed(time).await?;

    // An operation that doesn't belong to the app isn't allowed.
    let invalid_block = make_first_block(chain_id).with_simple_transfer(chain_id, Amount::ONE);
    let result = chain
        .execute_block(&invalid_block, time, None, &[], None)
        .await;
    assert_matches!(result, Err(ChainError::AuthorizedApplications(app_ids))
        if app_ids == vec![application_id, another_app_id]
    );

    // After registering, an app operation can already be used in the first block.
    application.expect_call(ExpectedCall::execute_operation(|_, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    application.expect_call(ExpectedCall::execute_operation(|_, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    let app_operation = Operation::User {
        application_id,
        bytes: b"foo".to_vec(),
    };
    let another_app_operation = Operation::User {
        application_id: another_app_id,
        bytes: b"bar".to_vec(),
    };

    let valid_block = make_first_block(chain_id)
        .with_operation(app_operation.clone())
        .with_operation(another_app_operation.clone());

    let (outcome, _) = chain
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
        if app_ids == vec![application_id, another_app_id]
    );
    // Also, blocks without all authorized applications operation, or incoming message, are forbidden.
    let invalid_block = make_child_block(&value).with_operation(another_app_operation.clone());
    let result = chain
        .execute_block(&invalid_block, time, None, &[], None)
        .await;
    assert_matches!(result, Err(ChainError::MissingMandatoryApplications(app_ids))
        if app_ids == vec![application_id]
    );
    // But app operations continue to work.
    application.expect_call(ExpectedCall::execute_operation(|_, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    application.expect_call(ExpectedCall::execute_operation(|_, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    let valid_block = make_child_block(&value)
        .with_operation(app_operation.clone())
        .with_operation(another_app_operation.clone());
    let (outcome, _) = chain
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

    chain
        .execute_block(&block, time, None, &[], None)
        .await
        .map(|(outcome, _)| outcome)
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

    chain
        .execute_block(&block, time, None, &[], None)
        .await
        .map(|(outcome, _)| outcome)
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

    chain
        .execute_block(&block, time, None, &[], None)
        .await
        .map(|(outcome, _)| outcome)
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
    let mut env = TestEnvironment::new();
    let time = Timestamp::from(0);

    let config = env.make_open_chain_config();

    let committee_blob = committee_blob(policy);

    let chain_desc = env.make_child_chain_description_with_config(3, config);
    let chain_id = chain_desc.id();
    let mut chain = ChainStateView::new(chain_id).await;

    chain
        .context()
        .extra()
        .add_blobs([committee_blob.clone()])
        .await?;

    chain
        .context()
        .extra()
        .add_blobs(env.description_blobs())
        .await?;

    chain.initialize_if_needed(time).await?;

    // Create a mock application.
    let (app_description, contract_blob, service_blob) = env.make_app_description();
    let application_id = ApplicationId::from(&app_description);
    let application = MockApplication::default();
    let context = chain.context();
    let extra = context.extra();
    {
        let pinned = extra.user_contracts().pin();
        pinned.insert(application_id, application.clone().into());
    }
    {
        let pinned = extra.user_services().pin();
        pinned.insert(application_id, application.clone().into());
    }
    extra
        .add_blobs([
            committee_blob,
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
