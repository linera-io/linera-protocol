use super::{Runnable, S3Config, StorageConfig};
use crate::config::{CommitteeConfig, GenesisConfig};
use async_trait::async_trait;
use linera_base::{
    chain::ChainState,
    crypto::KeyPair,
    messages::{ChainDescription, ChainId},
    system::Balance,
};
use linera_storage::{LocalStackTestContext, Storage};

/// Test if an empty S3 storage is properly initialized by [`StorageConfig::run_with_storage`].
///
/// Check that the chain states resulting from a mock [`GenesisConfig`] are stored in the S3
/// storage.
#[tokio::test]
#[ignore]
async fn s3_storage_is_initialized() -> Result<(), anyhow::Error> {
    let _localstack = LocalStackTestContext::new().await?;

    let storage_config = StorageConfig::S3 {
        config: S3Config::LocalStack,
        bucket: "linera".parse().expect("Invalid bucket name"),
    };

    let (genesis_config, expected_chain_states) = mock_genesis_config_and_chain_states([7, 10]);

    storage_config
        .run_with_storage(
            &genesis_config,
            CheckChains {
                presence: expected_chain_states,
                abscence: vec![],
            },
        )
        .await?;

    Ok(())
}

/// Test if an initialized S3 storage is not reinitialized by [`StorageConfig::run_with_storage`].
///
/// Starting with an empty S3 storage, initialize it once with a first [`GenesisConfig`]. Drop the
/// resulting [`S3Storage`] instance, then call [`StorageConfig::run_with_storage`] again using a
/// second [`GenesisConfig`].
///
/// Check that the chain states from the first [`GenesisConfig`] are preserved, and the chain
/// states from the second [`GenesisConfig`] are not added to the storage.
#[tokio::test]
#[ignore]
async fn s3_storage_is_not_reinitialized() -> Result<(), anyhow::Error> {
    let _localstack = LocalStackTestContext::new().await?;

    let storage_config = StorageConfig::S3 {
        config: S3Config::LocalStack,
        bucket: "linera".parse().expect("Invalid bucket name"),
    };

    // Create the storage with a mock genesis configuration, and drop it immediately.
    let (first_genesis_config, first_expected_chain_states) =
        mock_genesis_config_and_chain_states([31, 9]);
    storage_config
        .run_with_storage(&first_genesis_config, Skip)
        .await?;

    // Prepare a second genesis configuration to recreate the storage.
    let (mut second_genesis_config, mut second_expected_chain_states) =
        mock_genesis_config_and_chain_states([2, 16]);

    // Add a chain to the second genesis configuration with the same ID as a chain from the first
    // configuration, but with a different balance.
    let mut conflicting_chain = first_genesis_config.chains[1];
    let mut conflicting_chain_state = first_expected_chain_states[1].clone();
    let conflicting_balance = Balance::zero();

    conflicting_chain.2 = conflicting_balance;
    conflicting_chain_state.state.system.balance = conflicting_balance;

    second_genesis_config.chains.push(conflicting_chain);
    second_expected_chain_states.push(conflicting_chain_state);

    storage_config
        .run_with_storage(
            &second_genesis_config,
            CheckChains {
                presence: first_expected_chain_states,
                abscence: second_expected_chain_states,
            },
        )
        .await?;

    Ok(())
}

/// Test if two S3 storages using separate bucket do not have overlapping chains.
///
/// Create two [`S3Storage`] instances, and initialize them with separate [`GenesisConfig`]s. Check
/// that the chain states are only present in their respective [`S3Storage`] instances.
#[tokio::test]
#[ignore]
async fn s3_storage_buckets_are_independent() -> Result<(), anyhow::Error> {
    let _localstack = LocalStackTestContext::new().await?;

    // Prepare a first genesis configuration for the first storage.
    let first_storage_config = StorageConfig::S3 {
        config: S3Config::LocalStack,
        bucket: "first".parse().expect("Invalid bucket name"),
    };

    let (first_genesis_config, first_expected_chain_states) =
        mock_genesis_config_and_chain_states([31, 9]);

    // Prepare a second genesis configuration to recreate the storage.
    let second_storage_config = StorageConfig::S3 {
        config: S3Config::LocalStack,
        bucket: "second".parse().expect("Invalid bucket name"),
    };

    let (second_genesis_config, second_expected_chain_states) =
        mock_genesis_config_and_chain_states([2, 16]);

    // Check that the storages are isolated.
    first_storage_config
        .run_with_storage(
            &first_genesis_config,
            CheckChains {
                presence: first_expected_chain_states.clone(),
                abscence: second_expected_chain_states.clone(),
            },
        )
        .await?;

    second_storage_config
        .run_with_storage(
            &second_genesis_config,
            CheckChains {
                presence: second_expected_chain_states,
                abscence: first_expected_chain_states,
            },
        )
        .await?;

    Ok(())
}

/// A runnable job that does nothing.
struct Skip;

#[async_trait]
impl<S> Runnable<S> for Skip
where
    S: Send + 'static,
{
    type Output = ();

    async fn run(self, _: S) -> Result<Self::Output, anyhow::Error> {
        Ok(())
    }
}

/// A runnable job to check for the presence and/or abscence of chain states.
struct CheckChains {
    presence: Vec<ChainState>,
    abscence: Vec<ChainState>,
}

#[async_trait]
impl<S> Runnable<S> for CheckChains
where
    S: Storage + Send + 'static,
{
    type Output = ();

    async fn run(self, mut storage: S) -> Result<Self::Output, anyhow::Error> {
        for expected_chain_state in &self.presence {
            let chain_state = storage
                .read_chain_or_default(expected_chain_state.state.system.chain_id)
                .await?;

            assert_eq!(&chain_state, expected_chain_state);
        }

        for unexpected_chain_state in &self.abscence {
            let chain_state = storage
                .read_chain_or_default(unexpected_chain_state.state.system.chain_id)
                .await?;

            assert_ne!(&chain_state, unexpected_chain_state);
        }

        Ok(())
    }
}

/// Create a mock [`GenesisConfig`] and the expected [`ChainState`]s that should be created by it.
fn mock_genesis_config_and_chain_states<B>(
    balances: impl IntoIterator<Item = B>,
) -> (GenesisConfig, Vec<ChainState>)
where
    Balance: From<B>,
{
    let admin_id = ChainId::root(0);
    let committee = CommitteeConfig { validators: vec![] };

    let chains: Vec<_> = balances
        .into_iter()
        .map(Balance::from)
        .enumerate()
        .map(|(index, balance)| {
            let owner = KeyPair::generate().public().into();
            (ChainDescription::Root(index), owner, balance)
        })
        .collect();

    let expected_chain_states: Vec<_> = chains
        .iter()
        .map(|(description, owner, balance)| {
            ChainState::create(
                committee.clone().into_committee(),
                admin_id,
                *description,
                *owner,
                *balance,
            )
        })
        .collect();

    let genesis_config = GenesisConfig {
        committee,
        admin_id,
        chains,
    };

    (genesis_config, expected_chain_states)
}
