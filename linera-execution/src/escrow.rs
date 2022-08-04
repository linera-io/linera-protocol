use crate::{BlockValidatorSmartContract, ExecutionContext, ExecutionError};
use async_trait::async_trait;
use linera_base::{
    crypto::{PublicKey, Signature},
    ensure,
    execution::{ApplicationResult, Operation},
    messages::Block,
};
use linera_views::views::{AppendOnlyLogOperations, AppendOnlyLogView, Context, ScopedView, View};
use std::collections::HashSet;

pub struct Escrow;

#[async_trait]
impl<C> BlockValidatorSmartContract<C> for Escrow
where
    C: Context
        + AppendOnlyLogOperations<PublicKey>
        + AppendOnlyLogOperations<Vec<u8>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    type Command = Command;

    async fn validate_block(
        execution: &ExecutionContext,
        storage: &mut C,
        block: &Block,
        command: Self::Command,
    ) -> Result<(), ExecutionError> {
        match command {
            Command::RegisterParticipants { participants } => {
                Self::register_participants(participants, storage, block).await
            }
            Command::RegisterCancellation {
                operation,
                signatures,
            } => Self::register_cancellation(operation, signatures, storage, block).await,
            Command::Cancel { signature } => Self::cancel(signature, storage, block).await,
            Command::Complete { signatures } => Self::complete(signatures, storage, block).await,
        }
    }
}

pub enum Command {
    RegisterParticipants {
        participants: Vec<PublicKey>,
    },

    RegisterCancellation {
        operation: Vec<u8>,
        signatures: Vec<Signature>,
    },

    Cancel {
        signature: Signature,
    },

    Complete {
        signatures: Vec<Signature>,
    },
}

impl Escrow {
    pub async fn register_participants<S>(
        participants: Vec<PublicKey>,
        storage: &mut S,
        block: &Block,
    ) -> Result<(), ExecutionError>
    where
        S: Context + AppendOnlyLogOperations<PublicKey> + Clone + Send + Sync + 'static,
    {
        ensure!(
            block.operations.is_empty(),
            ExecutionError::custom("Block to register participants can't have any operations")
        );
        ensure!(
            block.incoming_messages.is_empty(),
            ExecutionError::custom(
                "Block to register participants can't have any incoming messages"
            )
        );
        ensure!(
            !participants.is_empty(),
            ExecutionError::custom("Participant list is empty")
        );

        let mut participants_list: ScopedView<0, AppendOnlyLogView<_, _>> =
            ScopedView::load(storage.clone())
                .await
                .map_err(ExecutionError::storage)?;

        ensure!(
            participants_list.count() == 0,
            ExecutionError::custom("Can only register participants once")
        );

        for participant in participants {
            participants_list.push(participant);
        }

        participants_list
            .commit()
            .await
            .map_err(ExecutionError::storage)?;

        Ok(())
    }

    pub async fn register_cancellation<S>(
        operation: Vec<u8>,
        signatures: Vec<Signature>,
        storage: &mut S,
        block: &Block,
    ) -> Result<(), ExecutionError>
    where
        S: Context
            + AppendOnlyLogOperations<PublicKey>
            + AppendOnlyLogOperations<Vec<u8>>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        Self::check_signatures(Some(&operation), signatures, storage, block).await?;

        let mut cancellations: ScopedView<1, AppendOnlyLogView<_, _>> =
            ScopedView::load(storage.clone())
                .await
                .map_err(ExecutionError::storage)?;

        cancellations.push(operation);
        cancellations
            .commit()
            .await
            .map_err(ExecutionError::storage)?;

        Ok(())
    }

    pub async fn cancel<S>(
        signature: Signature,
        storage: &mut S,
        block: &Block,
    ) -> Result<(), ExecutionError>
    where
        S: Context
            + AppendOnlyLogOperations<PublicKey>
            + AppendOnlyLogOperations<Vec<u8>>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        ensure!(
            block.incoming_messages.is_empty(),
            ExecutionError::custom("Block to cancel escrow can't have any incoming messages")
        );

        Self::check_signature(signature, storage, block).await?;
        Self::check_cancellations(&block.operations, storage).await?;

        Ok(())
    }

    pub async fn complete<S>(
        signatures: Vec<Signature>,
        storage: &mut S,
        block: &Block,
    ) -> Result<(), ExecutionError>
    where
        S: Context + AppendOnlyLogOperations<PublicKey> + Clone + Send + Sync + 'static,
    {
        Self::check_signatures(None, signatures, storage, block).await
    }

    async fn check_signatures<S>(
        prefix: Option<&[u8]>,
        signatures: Vec<Signature>,
        storage: &mut S,
        block: &Block,
    ) -> Result<(), ExecutionError>
    where
        S: Context + AppendOnlyLogOperations<PublicKey> + Clone + Send + Sync + 'static,
    {
        let mut participants_list: ScopedView<0, AppendOnlyLogView<_, _>> =
            ScopedView::load(storage.clone())
                .await
                .map_err(ExecutionError::storage)?;
        let participant_count = participants_list.count();

        ensure!(
            signatures.len() == participant_count,
            ExecutionError::custom("Incorrect number of signatures")
        );

        let mut payload = Vec::new();

        if let Some(prefix) = prefix {
            payload.extend(
                u64::try_from(prefix.len())
                    .map_err(|_| ExecutionError::custom("Operation is too large"))?
                    .to_le_bytes(),
            );
            payload.extend(prefix);
        }

        bincode::serialize_into(&mut payload, &(&block.operations, &block.incoming_messages));

        let participants = participants_list
            .read(0..participant_count)
            .await
            .map_err(ExecutionError::storage)?;

        for (participant, signature) in participants.into_iter().zip(signatures) {
            signature
                .check(&payload, participant)
                .map_err(|_| ExecutionError::custom("Invalid signature"))?;
        }

        Ok(())
    }

    async fn check_signature<S>(
        signature: Signature,
        storage: &mut S,
        block: &Block,
    ) -> Result<(), ExecutionError>
    where
        S: Context + AppendOnlyLogOperations<PublicKey> + Clone + Send + Sync + 'static,
    {
        let payload = bincode::serialize(&(&block.operations, &block.incoming_messages))
            .expect("Block is not serializable");

        let mut participants_list: ScopedView<0, AppendOnlyLogView<_, _>> =
            ScopedView::load(storage.clone())
                .await
                .map_err(ExecutionError::storage)?;
        let participant_count = participants_list.count();

        let participants = participants_list
            .read(0..participant_count)
            .await
            .map_err(ExecutionError::storage)?;

        let signature_is_valid = participants
            .into_iter()
            .any(|participant| signature.check(&payload, participant).is_ok());

        if signature_is_valid {
            Ok(())
        } else {
            Err(ExecutionError::custom("Invalid signature"))
        }
    }

    async fn check_cancellations<S>(
        operations: &[Operation],
        storage: &mut S,
    ) -> Result<(), ExecutionError>
    where
        S: AppendOnlyLogOperations<Vec<u8>> + Clone + Send + Sync + 'static,
    {
        let mut cancellations_list: ScopedView<1, AppendOnlyLogView<_, _>> =
            ScopedView::load(storage.clone())
                .await
                .map_err(ExecutionError::storage)?;
        let cancellations_count = cancellations_list.count();

        let mut cancellations: HashSet<_> = cancellations_list
            .read(0..cancellations_count)
            .await
            .map_err(ExecutionError::storage)?
            .into_iter()
            .collect();

        let serialized_operations: Vec<Vec<u8>> = operations
            .iter()
            .map(bincode::serialize)
            .collect::<Result<_, _>>()
            .expect("Operations aren't serializable");

        for operation in serialized_operations {
            ensure!(
                cancellations.remove(&operation),
                ExecutionError::custom("Block contains an unexpected operation")
            );
        }

        Ok(())
    }
}
