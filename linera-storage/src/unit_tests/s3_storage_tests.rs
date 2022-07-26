use super::S3Storage;
use anyhow::{Context, Error};
use aws_sdk_s3::Endpoint;
use aws_types::SdkConfig;
use std::env;
use tokio::sync::{Mutex, MutexGuard};
#[cfg(test)]
use {
    super::{BucketName, BucketStatus},
    crate::Storage,
    linera_base::{
        chain::ChainState,
        crypto::HashValue,
        execution::{ExecutionState, Operation},
        messages::{Block, BlockHeight, Certificate, ChainDescription, ChainId, Epoch, Value},
    },
};

/// A static lock to prevent multiple tests from using the same LocalStack instance at the same
/// time.
static LOCALSTACK_GUARD: Mutex<()> = Mutex::const_new(());

/// Name of the environment variable with the address to a LocalStack instance.
const LOCALSTACK_ENDPOINT: &str = "LOCALSTACK_ENDPOINT";

/// A type to help tests that need a LocalStack instance.
pub struct LocalStackTestContext {
    base_config: SdkConfig,
    endpoint: Endpoint,
    _guard: MutexGuard<'static, ()>,
}

impl LocalStackTestContext {
    /// Creates an instance of [`LocalStackTestContext`], loading the necessary LocalStack
    /// configuration.
    ///
    /// An address to the LocalStack instance must be specified using a [`LOCALSTACK_ENDPOINT`]
    /// environment variable.
    ///
    /// This also locks the [`LOCALSTACK_GUARD`] to enforce only one test has access to the
    /// LocalStack instance.
    pub async fn new() -> Result<LocalStackTestContext, Error> {
        let base_config = aws_config::load_from_env().await;
        let endpoint = Self::load_endpoint()?;
        let _guard = LOCALSTACK_GUARD.lock().await;

        let context = LocalStackTestContext {
            base_config,
            endpoint,
            _guard,
        };

        context.clear().await?;

        Ok(context)
    }

    /// Creates an [`Endpoint`] using the configuration in the [`LOCALSTACK_ENDPOINT`] environment
    /// variable.
    fn load_endpoint() -> Result<Endpoint, Error> {
        let endpoint_address = env::var(LOCALSTACK_ENDPOINT)
            .with_context(|| {
                format!(
                    "Missing LocalStack endpoint address in {LOCALSTACK_ENDPOINT:?} \
                    environment variable"
                )
            })?
            .parse()
            .context("LocalStack endpoint address is not a valid URI")?;

        Ok(Endpoint::immutable(endpoint_address))
    }

    /// Create a new [`aws_sdk_s3::Config`] for tests, using a LocalStack instance.
    pub fn config(&self) -> aws_sdk_s3::Config {
        aws_sdk_s3::config::Builder::from(&self.base_config)
            .endpoint_resolver(self.endpoint.clone())
            .build()
    }

    /// Create a new [`S3Storage`] instance, using a LocalStack instance.
    pub async fn create_s3_storage(&self) -> Result<S3Storage, Error> {
        let (storage, _) = S3Storage::from_config(self.config()).await?;
        Ok(storage)
    }

    /// Remove all buckets from the LocalStack S3 storage.
    async fn clear(&self) -> Result<(), Error> {
        let client = aws_sdk_s3::Client::from_conf(self.config());

        for bucket in list_buckets(&client).await? {
            let objects = client.list_objects().bucket(&bucket).send().await?;

            for object in objects.contents().into_iter().flatten() {
                if let Some(key) = object.key.as_ref() {
                    client
                        .delete_object()
                        .bucket(&bucket)
                        .key(key)
                        .send()
                        .await?;
                }
            }

            client.delete_bucket().bucket(bucket).send().await?;
        }

        Ok(())
    }
}

/// Test if the necessary buckets are created if needed.
#[tokio::test]
#[ignore]
async fn buckets_are_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_s3::Client::from_conf(localstack.config());

    let initial_buckets = list_buckets(&client).await?;
    assert!(!initial_buckets.contains(&BUCKET.to_owned()));

    let (_storage, bucket_status) = S3Storage::from_config(localstack.config()).await?;

    let buckets = list_buckets(&client).await?;
    assert!(buckets.contains(&BUCKET.to_owned()));
    assert_eq!(bucket_status, BucketStatus::New);

    Ok(())
}

/// Helper function to list the names of buckets registered on S3.
async fn list_buckets(client: &aws_sdk_s3::Client) -> Result<Vec<String>, Error> {
    Ok(client
        .list_buckets()
        .send()
        .await?
        .buckets
        .expect("List of buckets was not returned")
        .into_iter()
        .filter_map(|bucket| bucket.name)
        .collect())
}

/// Test if certificates are stored and retrieved correctly.
#[tokio::test]
#[ignore]
async fn certificate_storage_round_trip() -> Result<(), Error> {
    let block = Block {
        epoch: Epoch::from(0),
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::CloseChain],
        previous_block_hash: None,
        height: BlockHeight::default(),
    };
    let value = Value::ConfirmedBlock {
        block,
        effects: Vec::new(),
        state_hash: HashValue::new(&ExecutionState::new(ChainId::root(1))),
    };
    let certificate = Certificate::new(value, vec![]);

    let localstack = LocalStackTestContext::new().await?;
    let mut storage = localstack.create_s3_storage().await?;

    storage.write_certificate(certificate.clone()).await?;

    let stored_certificate = storage.read_certificate(certificate.hash).await?;

    assert_eq!(certificate, stored_certificate);

    Ok(())
}

/// Test if retrieving inexistent certificates fails.
#[tokio::test]
#[ignore]
async fn retrieval_of_inexistent_certificate() -> Result<(), Error> {
    let certificate_hash = HashValue::new(&ChainDescription::Root(123));

    let localstack = LocalStackTestContext::new().await?;
    let mut storage = localstack.create_s3_storage().await?;

    let result = storage.read_certificate(certificate_hash).await;

    assert!(result.is_err());

    Ok(())
}

/// Test if chain states are stored and retrieved correctly.
#[tokio::test]
#[ignore]
async fn chain_storage_round_trip() -> Result<(), Error> {
    let chain_id = ChainId::root(1);
    let chain_state = ChainState {
        next_block_height: BlockHeight(100),
        ..ChainState::new(chain_id)
    };

    let localstack = LocalStackTestContext::new().await?;
    let mut storage = localstack.create_s3_storage().await?;

    storage.write_chain(chain_state.clone()).await?;

    let stored_chain_state = storage
        .read_chain_or_default(chain_state.state.chain_id)
        .await?;

    assert_eq!(chain_state, stored_chain_state);

    Ok(())
}

/// Test if retrieving inexistent chain states creates new [`ChainState`] instances.
#[tokio::test]
#[ignore]
async fn retrieval_of_inexistent_chain_state() -> Result<(), Error> {
    let chain_id = ChainId::root(5);

    let localstack = LocalStackTestContext::new().await?;
    let mut storage = localstack.create_s3_storage().await?;

    let chain_state = storage.read_chain_or_default(chain_id).await?;
    let expected_chain_state = ChainState::new(chain_id);

    assert_eq!(chain_state, expected_chain_state);

    Ok(())
}

/// Test if chain states are stored and retrieved correctly.
#[tokio::test]
#[ignore]
async fn removal_of_chain_state() -> Result<(), Error> {
    let chain_id = ChainId::root(9);
    let chain_state = ChainState {
        next_block_height: BlockHeight(300),
        ..ChainState::new(chain_id)
    };

    let localstack = LocalStackTestContext::new().await?;
    let mut storage = localstack.create_s3_storage().await?;

    storage.write_chain(chain_state).await?;
    storage.remove_chain(chain_id).await?;

    let retrieved_chain_state = storage.read_chain_or_default(chain_id).await?;
    let expected_chain_state = ChainState::new(chain_id);

    assert_eq!(retrieved_chain_state, expected_chain_state);

    Ok(())
}
