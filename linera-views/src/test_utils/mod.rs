// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::batch::{
    WriteOperation,
    WriteOperation::{Delete, Put},
};
use rand::{Rng, RngCore};
use std::collections::HashSet;

#[cfg(feature = "aws")]
use {
    anyhow::{Context, Error},
    aws_sdk_s3::Endpoint,
    aws_types::SdkConfig,
    std::env,
    tokio::sync::{Mutex, MutexGuard},
};

#[cfg(feature = "aws")]
/// A static lock to prevent multiple tests from using the same LocalStack instance at the same
/// time.
static LOCALSTACK_GUARD: Mutex<()> = Mutex::const_new(());

#[cfg(feature = "aws")]
/// Name of the environment variable with the address to a LocalStack instance.
const LOCALSTACK_ENDPOINT: &str = "LOCALSTACK_ENDPOINT";

#[cfg(feature = "aws")]
/// A type to help tests that need a LocalStack instance.
pub struct LocalStackTestContext {
    base_config: SdkConfig,
    endpoint: Endpoint,
    _guard: MutexGuard<'static, ()>,
}

#[cfg(feature = "aws")]
impl LocalStackTestContext {
    /// Creates an instance of [`LocalStackTestContext`], loading the necessary LocalStack
    /// configuration.
    ///
    /// An address to the LocalStack instance must be specified using a `LOCALSTACK_ENDPOINT`
    /// environment variable.
    ///
    /// This also locks the `LOCALSTACK_GUARD` to enforce only one test has access to the
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

    /// Creates a new [`aws_sdk_s3::Config`] for tests, using a LocalStack instance.
    pub fn s3_config(&self) -> aws_sdk_s3::Config {
        aws_sdk_s3::config::Builder::from(&self.base_config)
            .endpoint_resolver(self.endpoint.clone())
            .build()
    }

    /// Creates a new [`aws_sdk_dynamodb::Config`] for tests, using a LocalStack instance.
    pub fn dynamo_db_config(&self) -> aws_sdk_dynamodb::Config {
        aws_sdk_dynamodb::config::Builder::from(&self.base_config)
            .endpoint_resolver(self.endpoint.clone())
            .build()
    }

    /// Removes all stored data from LocalStack storage.
    async fn clear(&self) -> Result<(), Error> {
        self.remove_buckets().await?;
        self.remove_tables().await?;

        Ok(())
    }

    /// Removes all buckets from the LocalStack S3 storage.
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

    /// Removes all tables from the LocalStack DynamoDB storage.
    async fn remove_tables(&self) -> Result<(), Error> {
        let client = aws_sdk_dynamodb::Client::from_conf(self.dynamo_db_config());

        for table in list_tables(&client).await.unwrap_or_default() {
            client.delete_table().table_name(table).send().await?;
        }

        Ok(())
    }
}

#[cfg(feature = "aws")]
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

#[cfg(feature = "aws")]
/// Helper function to list the names of tables registered on DynamoDB.
pub async fn list_tables(client: &aws_sdk_dynamodb::Client) -> Result<Vec<String>, Error> {
    Ok(client
        .list_tables()
        .send()
        .await?
        .table_names
        .expect("List of tables was not returned"))
}

/// Shuffles the values entries randomly
pub fn random_shuffle<R: RngCore, T: Clone>(rng: &mut R, values: &mut Vec<T>) {
    let n = values.len();
    for _ in 0..4 * n {
        let index1: usize = rng.gen_range(0, n);
        let index2: usize = rng.gen_range(0, n);
        if index1 != index2 {
            let val1 = values.get(index1).unwrap().clone();
            let val2 = values.get(index2).unwrap().clone();
            values[index1] = val2;
            values[index2] = val1;
        }
    }
}

/// Takes a random number generator, a key_prefix and extends it by n random bytes.
pub fn get_random_byte_vector<R: RngCore>(rng: &mut R, key_prefix: &[u8], n: usize) -> Vec<u8> {
    let mut v = key_prefix.to_vec();
    for _ in 0..n {
        let val = rng.gen_range(0, 256) as u8;
        v.push(val);
    }
    v
}

/// Builds a random k element subset of n
pub fn get_random_kset<R: RngCore>(rng: &mut R, n: usize, k: usize) -> Vec<usize> {
    let mut values = Vec::new();
    for u in 0..n {
        values.push(u);
    }
    random_shuffle(rng, &mut values);
    values[..k].to_vec()
}

/// Takes a random number generator, a key_prefix and generates
/// pairs (key,value) with key obtained by appending 8 bytes at random to key_prefix
/// and value obtained by appending 8 bytes to the trivial vector.
/// We return n such (key,value) pairs which are all distinct
pub fn get_random_key_value_vec_prefix<R: RngCore>(
    rng: &mut R,
    key_prefix: Vec<u8>,
    n: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    loop {
        let mut v_ret = Vec::new();
        let mut vector_set = HashSet::new();
        for _ in 0..n {
            let v1 = get_random_byte_vector(rng, &key_prefix, 8);
            let v2 = get_random_byte_vector(rng, &Vec::new(), 8);
            let v12 = (v1.clone(), v2);
            vector_set.insert(v1);
            v_ret.push(v12);
        }
        if vector_set.len() == n {
            return v_ret;
        }
    }
}

/// Takes a random number generator rng, a number n and returns n random (key,value)
/// which are all distinct with key and value are of length 8.
pub fn get_random_key_value_vec<R: RngCore>(rng: &mut R, n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    get_random_key_value_vec_prefix(rng, Vec::new(), n)
}

type VectorPutDelete = (Vec<(Vec<u8>, Vec<u8>)>, usize);

/// A bunch of puts and some deletes.
pub fn get_random_key_value_operations<R: RngCore>(
    rng: &mut R,
    n: usize,
    k: usize,
) -> VectorPutDelete {
    let key_value_vector = get_random_key_value_vec_prefix(rng, Vec::new(), n);
    (key_value_vector, k)
}

/// A random reordering of the puts and deletes.
/// For something like MapView it should get us the same result whatever way we are calling.
pub fn span_random_reordering_put_delete<R: RngCore>(
    rng: &mut R,
    info_op: VectorPutDelete,
) -> Vec<WriteOperation> {
    let n = info_op.0.len();
    let k = info_op.1;
    let mut indices = Vec::new();
    for i in 0..n {
        indices.push(i);
    }
    random_shuffle(rng, &mut indices);
    let mut indices_rev = vec![0; n];
    for i in 0..n {
        indices_rev[indices[i]] = i;
    }
    let mut pos_remove_vector = vec![Vec::new(); n];
    for (i, pos) in indices_rev.iter().enumerate().take(k) {
        let idx = rng.gen_range(*pos, n);
        pos_remove_vector[idx].push(i);
    }
    let mut operations = Vec::new();
    for i in 0..n {
        let pos = indices[i];
        let pair = info_op.0[pos].clone();
        operations.push(Put {
            key: pair.0,
            value: pair.1,
        });
        for pos_remove in pos_remove_vector[i].clone() {
            let key = info_op.0[pos_remove].0.clone();
            operations.push(Delete { key });
        }
    }
    operations
}
