use super::{S3Config, StorageConfig};
use crate::config::{CommitteeConfig, GenesisConfig};
use linera_base::{
    chain::ChainState,
    crypto::KeyPair,
    execution::Balance,
    messages::{ChainDescription, ChainId},
};
use linera_storage::LocalStackTestContext;

/// Test if an empty S3 storage is properly initialized by [`StorageConfig::make_storage`].
///
/// Check that the chain states resulting from a mock [`GenesisConfig`] are stored in the S3
/// storage.
#[tokio::test]
#[ignore]
async fn s3_storage_is_initialized() -> Result<(), anyhow::Error> {
    let _localstack = LocalStackTestContext::new().await?;

    let storage_config = StorageConfig::S3 {
        config: S3Config::LocalStack,
    };

    let (genesis_config, expected_chain_states) = mock_genesis_config_and_chain_states([7, 10]);
    let mut storage = storage_config.make_storage(&genesis_config).await?;

    for expected_chain_state in expected_chain_states {
        let chain_state = storage
            .read_chain_or_default(expected_chain_state.state.chain_id)
            .await?;

        assert_eq!(chain_state, expected_chain_state);
    }

    Ok(())
}

/// Test if an initialized S3 storage is not reinitialized by [`StorageConfig::make_storage`].
///
/// Starting with an empty S3 storage, initialize it once with a first [`GenesisConfig`]. Drop the
/// resulting [`S3Storage`] instance, then call [`StorageConfig::make_storage`] again using a
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
    };

    // Create the storage with a mock genesis configuration, and drop it immediately.
    let (first_genesis_config, first_expected_chain_states) =
        mock_genesis_config_and_chain_states([31, 9]);
    let _ = storage_config.make_storage(&first_genesis_config).await?;

    // Prepare a second genesis configuration to recreate the storage.
    let (mut second_genesis_config, mut second_expected_chain_states) =
        mock_genesis_config_and_chain_states([2, 16]);

    // Add a chain to the second genesis configuration with the same ID as a chain from the first
    // configuration, but with a different balance.
    let mut conflicting_chain = first_genesis_config.chains[1];
    let mut conflicting_chain_state = first_expected_chain_states[1].clone();
    let conflicting_balance = Balance::zero();

    conflicting_chain.2 = conflicting_balance;
    conflicting_chain_state.state.balance = conflicting_balance;

    second_genesis_config.chains.push(conflicting_chain);
    second_expected_chain_states.push(conflicting_chain_state);

    // Recreate the storage using the second genesis configuration.
    let mut storage = storage_config.make_storage(&second_genesis_config).await?;

    // Check that the chains from the first configuration still exist.
    for expected_chain_state in first_expected_chain_states {
        let chain_state = storage
            .read_chain_or_default(expected_chain_state.state.chain_id)
            .await?;

        assert_eq!(chain_state, expected_chain_state);
    }

    // Check that the chains from the second configuration were not added.
    for unexpected_chain_state in second_expected_chain_states {
        let chain_state = storage
            .read_chain_or_default(unexpected_chain_state.state.chain_id)
            .await?;

        assert_ne!(chain_state, unexpected_chain_state);
    }

    Ok(())
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
