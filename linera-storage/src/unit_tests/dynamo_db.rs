use super::DynamoDbStoreClient;
use crate::Store;
use linera_base::messages::ChainId;
use linera_views::test_utils::LocalStackTestContext;
use std::mem;

/// Test if released guards don't use memory.
#[tokio::test]
async fn guards_dont_leak() -> Result<(), anyhow::Error> {
    let localstack = LocalStackTestContext::new().await?;
    let table = "linera".parse()?;
    let (store, _) = DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table).await?;
    let chain_id = ChainId::root(1);
    // There should be no active guards when initialized
    assert_eq!(store.0.guards.active_guards(), 0);
    // One guard should be active after obtaining a chain
    let chain = store.load_chain(chain_id).await?;
    assert_eq!(store.0.guards.active_guards(), 1);
    // No guards should be active after dropping the chain
    mem::drop(chain);
    assert_eq!(store.0.guards.active_guards(), 0);
    Ok(())
}
