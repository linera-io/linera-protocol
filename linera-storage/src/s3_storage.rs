use crate::Storage;
use async_trait::async_trait;
use aws_sdk_s3::{error::CreateBucketError, types::SdkError, Client};
use linera_base::{
    chain::ChainState,
    crypto::HashValue,
    error::Error,
    messages::{Certificate, ChainId},
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Display;
use thiserror::Error;

#[cfg(test)]
#[path = "unit_tests/s3_storage_tests.rs"]
mod s3_storage_tests;

/// Bucket ID to use for storing the data.
const BUCKET: &str = "linera";

/// Key prefix for stored certificates.
const CERTIFICATE_PREFIX: &str = "certificates";

/// Chain prefix for stored chain states.
const CHAIN_PREFIX: &str = "chains";

/// Storage layer that uses Amazon S3.
#[derive(Clone, Debug)]
pub struct S3Storage {
    client: Client,
}

impl S3Storage {
    /// Create a new [`S3Storage`] instance.
    ///
    /// Loads any necessary configuration from environment variables, and creates the necessary
    /// buckets if they don't yet exist.
    pub async fn new() -> Result<Self, SdkError<CreateBucketError>> {
        let config = aws_config::load_from_env().await;

        S3Storage::from_config(&config).await
    }

    /// Create a new [`S3Storage`] instance using the provided `config` parameters.
    ///
    /// Creates the necessary buckets if they don't yet exist.
    pub async fn from_config(
        config: impl Into<aws_sdk_s3::Config>,
    ) -> Result<Self, SdkError<CreateBucketError>> {
        let s3_storage = S3Storage {
            client: Client::from_conf(config.into()),
        };

        s3_storage.try_create_bucket().await?;

        Ok(s3_storage)
    }

    /// Tries to create a bucket for storing the data.
    ///
    /// Will not fail if it already exists.
    async fn try_create_bucket(&self) -> Result<(), SdkError<CreateBucketError>> {
        match self.client.create_bucket().bucket(BUCKET).send().await {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError { err, .. }) if err.is_bucket_already_exists() => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Retrieve a generic `Object` from the bucket using the provided `key` prefixed with `prefix`.
    ///
    /// The `Object` is deserialized using [`ron`].
    async fn get_object<Object>(
        &mut self,
        prefix: &str,
        key: impl Display,
    ) -> Result<Object, S3StorageError>
    where
        Object: DeserializeOwned,
    {
        let response = self
            .client
            .get_object()
            .bucket(BUCKET)
            .key(format!("{prefix}-{key}"))
            .send()
            .await?;
        let bytes = response.body.collect().await?.into_bytes();
        let object = ron::de::from_bytes(&bytes)?;

        Ok(object)
    }

    /// Store a generic `object` into the bucket using the provided `key` prefixed with `prefix`.
    ///
    /// The `Object` is serialized using [`ron`].
    async fn put_object<Object>(
        &mut self,
        prefix: &str,
        key: impl Display,
        object: Object,
    ) -> Result<(), S3StorageError>
    where
        Object: Serialize,
    {
        let bytes = Vec::from(
            ron::to_string(&object)
                .expect("Object serialization failed")
                .as_bytes(),
        );

        self.client
            .put_object()
            .bucket(BUCKET)
            .key(format!("{prefix}-{key}"))
            .body(bytes.into())
            .send()
            .await?;

        Ok(())
    }

    /// Remove an object with the provided `key` prefixed with `prefix` from the bucket.
    async fn remove_object(
        &mut self,
        prefix: &str,
        key: impl Display,
    ) -> Result<(), S3StorageError> {
        self.client
            .delete_object()
            .bucket(BUCKET)
            .key(format!("{prefix}-{key}"))
            .send()
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn read_chain_or_default(&mut self, chain_id: ChainId) -> Result<ChainState, Error> {
        match self.get_object(CHAIN_PREFIX, chain_id).await {
            Ok(chain_state) => Ok(chain_state),
            Err(error) if error.is_no_such_key() => Ok(ChainState::new(chain_id)),
            Err(error) => Err(error.into_base_error()),
        }
    }

    async fn write_chain(&mut self, state: ChainState) -> Result<(), Error> {
        self.put_object(CHAIN_PREFIX, state.state.chain_id, state)
            .await
            .map_err(S3StorageError::into_base_error)
    }

    async fn remove_chain(&mut self, chain_id: ChainId) -> Result<(), Error> {
        self.remove_object(CHAIN_PREFIX, chain_id)
            .await
            .map_err(S3StorageError::into_base_error)
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        self.get_object(CERTIFICATE_PREFIX, hash)
            .await
            .map_err(S3StorageError::into_base_error)
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        self.put_object(CERTIFICATE_PREFIX, certificate.hash, certificate)
            .await
            .map_err(S3StorageError::into_base_error)
    }
}

/// Errors that can occur when using [`S3Storage`].
#[derive(Debug, Error)]
pub enum S3StorageError {
    #[error(transparent)]
    Get(#[from] SdkError<aws_sdk_s3::error::GetObjectError>),

    #[error(transparent)]
    Put(#[from] SdkError<aws_sdk_s3::error::PutObjectError>),

    #[error(transparent)]
    Delete(#[from] SdkError<aws_sdk_s3::error::DeleteObjectError>),

    #[error(transparent)]
    ByteStream(#[from] aws_smithy_http::byte_stream::Error),

    #[error(transparent)]
    Deserialization(#[from] ron::Error),
}

impl S3StorageError {
    /// Convert the error into an instance of the main [`Error`] type.
    pub fn into_base_error(self) -> Error {
        Error::StorageIoError {
            error: self.to_string(),
        }
    }

    /// Check if the error is because the key doesn't exist in the storage.
    pub fn is_no_such_key(&self) -> bool {
        matches!(
            self,
            S3StorageError::Get(SdkError::ServiceError {
                err: aws_sdk_s3::error::GetObjectError {
                    kind: aws_sdk_s3::error::GetObjectErrorKind::NoSuchKey(_),
                    ..
                },
                ..
            })
        )
    }
}
