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
