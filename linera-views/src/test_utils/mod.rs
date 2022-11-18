use anyhow::{Context, Error};
use aws_sdk_s3::Endpoint;
use aws_types::SdkConfig;
use rand::Rng;
use std::{collections::HashSet, env};
use tokio::sync::{Mutex, MutexGuard};

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
    pub fn s3_config(&self) -> aws_sdk_s3::Config {
        aws_sdk_s3::config::Builder::from(&self.base_config)
            .endpoint_resolver(self.endpoint.clone())
            .build()
    }

    /// Create a new [`aws_sdk_dynamodb::Config`] for tests, using a LocalStack instance.
    pub fn dynamo_db_config(&self) -> aws_sdk_dynamodb::Config {
        aws_sdk_dynamodb::config::Builder::from(&self.base_config)
            .endpoint_resolver(self.endpoint.clone())
            .build()
    }

    /// Remove all stored data from LocalStack storage.
    async fn clear(&self) -> Result<(), Error> {
        self.remove_buckets().await?;
        self.remove_tables().await?;

        Ok(())
    }

    /// Remove all buckets from the LocalStack S3 storage.
    async fn remove_buckets(&self) -> Result<(), Error> {
        let client = aws_sdk_s3::Client::from_conf(self.s3_config());

        for bucket in list_buckets(&client).await.unwrap_or_default() {
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

    /// Remove all tables from the LocalStack DynamoDB storage.
    async fn remove_tables(&self) -> Result<(), Error> {
        let client = aws_sdk_dynamodb::Client::from_conf(self.dynamo_db_config());

        for table in list_tables(&client).await.unwrap_or_default() {
            client.delete_table().table_name(table).send().await?;
        }

        Ok(())
    }
}

/// Helper function to list the names of buckets registered on S3.
pub async fn list_buckets(client: &aws_sdk_s3::Client) -> Result<Vec<String>, Error> {
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

/// Helper function to list the names of tables registered on DynamoDB.
pub async fn list_tables(client: &aws_sdk_dynamodb::Client) -> Result<Vec<String>, Error> {
    Ok(client
        .list_tables()
        .send()
        .await?
        .table_names
        .expect("List of tables was not returned"))
}

pub fn random_shuffle<T: Clone>(l_val: &mut Vec<T>) {
    let mut rng = rand::thread_rng();
    let n = l_val.len();
    for _ in 0..4 * n {
        let idx1: usize = rng.gen_range(0..n);
        let idx2: usize = rng.gen_range(0..n);
        if idx1 != idx2 {
            let val1 = l_val.get(idx1).unwrap().clone();
            let val2 = l_val.get(idx2).unwrap().clone();
            l_val[idx1] = val2;
            l_val[idx2] = val1;
        }
    }
}

pub fn get_random_vec_bytes(n: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut v = Vec::new();
    for _ in 0..n {
        let val = rng.gen_range(0..256) as u8;
        v.push(val);
    }
    v
}

pub fn get_random_vec_keyvalues(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    loop {
        let mut v_ret = Vec::new();
        let mut set_vect = HashSet::new();
        for _ in 0..n {
            let v1 = get_random_vec_bytes(8);
            let v2 = get_random_vec_bytes(8);
            let v12 = (v1.clone(), v2);
            set_vect.insert(v1);
            v_ret.push(v12);
        }
        if set_vect.len() == n {
            return v_ret;
        }
    }
}
