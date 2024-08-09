// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{iter, sync::Arc};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, ApplicationPermissions, BlockHeight, Timestamp},
    identifiers::{ApplicationId, BytecodeId, ChainId, MessageId},
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::OpenChainConfig,
    test_utils::{ExpectedCall, MockApplication},
    BytecodeLocation, ExecutionRuntimeConfig, ExecutionRuntimeContext, Operation, SystemMessage,
    TestExecutionRuntimeContext, UserApplicationDescription,
};
use linera_views::{
    memory::{MemoryContext, TEST_MEMORY_MAX_STREAM_QUERIES},
    test_utils::generate_test_namespace,
    views::{View, ViewError},
};

use crate::{
    data_types::HashedCertificateValue,
    test::{make_child_block, make_first_block, BlockTestExt, MessageTestExt},
    ChainError, ChainStateView,
};

impl ChainStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>:
        linera_views::common::Context + Clone + Send + Sync + 'static,
    ViewError:
        From<<MemoryContext<TestExecutionRuntimeContext> as linera_views::common::Context>::Error>,
{
    pub async fn new(chain_id: ChainId) -> Self {
        let exec_runtime_context =
            TestExecutionRuntimeContext::new(chain_id, ExecutionRuntimeConfig::default());
        let namespace = generate_test_namespace();
        let context = MemoryContext::new_for_testing(
            TEST_MEMORY_MAX_STREAM_QUERIES,
            &namespace,
            exec_runtime_context,
        );
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

fn make_app_description() -> UserApplicationDescription {
    UserApplicationDescription {
        bytecode_id: BytecodeId::new(make_admin_message_id(BlockHeight(1))),
        bytecode_location: BytecodeLocation {
            certificate_hash: CryptoHash::test_hash("bytecode certificate"),
            transaction_index: 0,
        },
        creation: make_admin_message_id(BlockHeight(2)),
        required_application_ids: vec![],
        parameters: vec![],
    }
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
    let committee = Committee::make_simple(vec![PublicKey::test_key(1).into()]);
    OpenChainConfig {
        ownership: ChainOwnership::single(PublicKey::test_key(0)),
        admin_id: admin_id(),
        epoch: Epoch::ZERO,
        committees: iter::once((Epoch::ZERO, committee)).collect(),
        balance: Amount::from_tokens(10),
        application_permissions: Default::default(),
    }
}

#[tokio::test]
async fn test_application_permissions() {
    let time = Timestamp::from(0);
    let message_id = make_admin_message_id(BlockHeight(3));
    let chain_id = ChainId::child(message_id);
    let mut chain = ChainStateView::new(chain_id).await;

    // Create a mock application.
    let app_description = make_app_description();
    let application_id = ApplicationId::from(&app_description);
    let application = Arc::new(MockApplication::default());
    let extra = &chain.context().extra;
    extra
        .user_contracts()
        .insert(application_id, application.clone());

    // Initialize the chain, with a chain application.
    let config = OpenChainConfig {
        application_permissions: ApplicationPermissions::new_single(application_id),
        ..make_open_chain_config()
    };
    let message = SystemMessage::OpenChain(config).into();
    chain
        .execute_init_message(message_id, &message, time, time)
        .await
        .unwrap();

    // The OpenChain message must be included in the first block. Also register the app.
    let open_chain_message = message.to_simple_incoming(admin_id(), BlockHeight(1));
    let register_app_message = SystemMessage::RegisterApplications {
        applications: vec![app_description],
    }
    .to_simple_incoming(admin_id(), BlockHeight(2));

    // An operation that doesn't belong to the app isn't allowed.
    let invalid_block = make_first_block(chain_id)
        .with_incoming_bundle(open_chain_message.clone())
        .with_incoming_bundle(register_app_message.clone())
        .with_simple_transfer(chain_id, Amount::ONE);
    let result = chain.execute_block(&invalid_block, time, None).await;
    assert_matches!(result, Err(ChainError::AuthorizedApplications(app_ids))
        if app_ids == vec![application_id]
    );

    // After registering, an app operation can already be used in the first block.
    application.expect_call(ExpectedCall::execute_operation(|_, _, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    let app_operation = Operation::User {
        application_id,
        bytes: b"foo".to_vec(),
    };
    let valid_block = make_first_block(chain_id)
        .with_incoming_bundle(open_chain_message)
        .with_incoming_bundle(register_app_message.clone())
        .with_operation(app_operation.clone());
    let outcome = chain.execute_block(&valid_block, time, None).await.unwrap();
    let value = HashedCertificateValue::new_confirmed(outcome.with(valid_block));

    // In the second block, other operations are still not allowed.
    let invalid_block = make_child_block(&value)
        .with_simple_transfer(chain_id, Amount::ONE)
        .with_operation(app_operation.clone());
    let result = chain.execute_block(&invalid_block, time, None).await;
    assert_matches!(result, Err(ChainError::AuthorizedApplications(app_ids))
        if app_ids == vec![application_id]
    );

    // Also, blocks without an application operation or incoming message are forbidden.
    let invalid_block = make_child_block(&value);
    let result = chain.execute_block(&invalid_block, time, None).await;
    assert_matches!(result, Err(ChainError::MissingMandatoryApplications(app_ids))
        if app_ids == vec![application_id]
    );

    // But app operations continue to work.
    application.expect_call(ExpectedCall::execute_operation(|_, _, _| Ok(vec![])));
    application.expect_call(ExpectedCall::default_finalize());
    let valid_block = make_child_block(&value).with_operation(app_operation);
    chain.execute_block(&valid_block, time, None).await.unwrap();
}
