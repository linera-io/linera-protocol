// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the [`RuntimeActor`].

use super::{RequestHandler, RuntimeActor, SendRequestExt};
use crate::ExecutionError;
use async_trait::async_trait;
use std::mem;

/// Test if sending a message to a dropped [`RuntimeActor`] doesn't cause a panic.
#[test]
fn test_sending_message_to_dropped_runtime_actor_doesnt_panic() {
    let (actor, sender) = RuntimeActor::new(());

    mem::drop(actor);

    assert!(matches!(
        sender.send_request(|_: oneshot::Sender<()>| ()),
        Err(ExecutionError::MissingRuntimeResponse)
    ));
}

#[async_trait]
impl RequestHandler<()> for () {
    async fn handle_request(&self, _request: ()) -> Result<(), ExecutionError> {
        panic!("Dummy request handler should not be called");
    }
}
