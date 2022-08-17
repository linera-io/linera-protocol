use crate::Storage;
use async_trait::async_trait;
use aws_sdk_s3::{types::SdkError, Client};
use linera_base::{
    chain::ChainState,
    crypto::HashValue,
    ensure,
    error::Error,
    messages::{Certificate, ChainId},
};
use linera_views::localstack;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Display, net::IpAddr, str::FromStr};
use thiserror::Error;

#[cfg(any(test, feature = "test"))]
#[path = "unit_tests/s3_storage_tests.rs"]
pub mod s3_storage_tests;

/// Key prefix for stored certificates.
const CERTIFICATE_PREFIX: &str = "certificates";

/// Chain prefix for stored chain states.
const CHAIN_PREFIX: &str = "chains";

/// Storage layer that uses Amazon S3.
#[derive(Clone, Debug)]
pub struct S3Storage {
    client: Client,
    bucket: BucketName,
}

impl S3Storage {
    /// Create a new [`S3Storage`] instance.
    ///
    /// Loads any necessary configuration from environment variables, and creates the necessary
    /// buckets if they don't yet exist, reporting a [`BucketStatus`] to indicate if the bucket was
    /// created or if it already exists.
    pub async fn new(bucket: BucketName) -> Result<(Self, BucketStatus), S3StorageError> {
        let config = aws_config::load_from_env().await;

        S3Storage::from_config(&config, bucket).await
    }

    /// Create a new [`S3Storage`] instance using the provided `config` parameters.
    ///
    /// Creates the necessary buckets if they don't yet exist, reporting a [`BucketStatus`] to
    /// indicate if the bucket was created or if it already exists.
    pub async fn from_config(
        config: impl Into<aws_sdk_s3::Config>,
        bucket: BucketName,
    ) -> Result<(Self, BucketStatus), S3StorageError> {
        let s3_storage = S3Storage {
            client: Client::from_conf(config.into()),
            bucket,
        };

        let bucket_status = s3_storage.create_bucket_if_needed().await?;

        Ok((s3_storage, bucket_status))
    }

    /// Create a new [`S3Storage`] instance using a LocalStack endpoint.
    ///
    /// Requires a [`LOCALSTACK_ENDPOINT`][localstack::LOCALSTACK_ENDPOINT] environment variable
    /// with the endpoint address to connect to the LocalStack instance.
    ///
    /// Creates the necessary buckets if they don't yet exist, reporting a [`BucketStatus`] to
    /// indicate if the bucket was created or if it already exists.
    pub async fn with_localstack(
        bucket: BucketName,
    ) -> Result<(Self, BucketStatus), LocalStackError> {
        let base_config = aws_config::load_from_env().await;
        let config = aws_sdk_s3::config::Builder::from(&base_config)
            .endpoint_resolver(localstack::get_endpoint()?)
            .build();

        Ok(S3Storage::from_config(config, bucket)
            .await
            .map_err(Box::new)?)
    }

    /// Checks if the bucket already exists and tries to create it if it doesn't.
    ///
    /// Returns a [`BucketStatus`] to indicate if it created the bucket or if already exists.
    async fn create_bucket_if_needed(&self) -> Result<BucketStatus, S3StorageError> {
        match self
            .client
            .get_bucket_location()
            .bucket(self.bucket.as_ref())
            .send()
            .await
        {
            Ok(_) => Ok(BucketStatus::Existing),
            Err(SdkError::ServiceError { err, .. }) if err.code() == Some("NoSuchBucket") => {
                self.client
                    .create_bucket()
                    .bucket(self.bucket.as_ref())
                    .send()
                    .await?;
                Ok(BucketStatus::New)
            }
            Err(error) => Err(error.into()),
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
            .bucket(self.bucket.as_ref())
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
            .bucket(self.bucket.as_ref())
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
            .bucket(self.bucket.as_ref())
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
        self.put_object(CHAIN_PREFIX, state.chain_id(), state)
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

/// Status of the bucket at the time of creation of an [`S3Storage`] instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketStatus {
    /// Bucket was created during the construction of the [`S3Storage`] instance.
    New,
    /// Bucket already existed when the [`S3Storage`] instance was created.
    Existing,
}

/// An AWS Bucket name.
///
/// AWS Bucket names must follow some specific [naming
/// rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketName(String);

impl FromStr for BucketName {
    type Err = InvalidBucketName;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        use InvalidBucketName::*;
        ensure!(string.len() >= 3, TooShort);
        ensure!(string.len() <= 63, TooLong);
        ensure!(!string.starts_with("xn--"), ForbiddenPrefix);
        ensure!(!string.ends_with("-s3alias"), ForbiddenSuffix);
        ensure!(IpAddr::from_str(string).is_err(), IpAmbiguity);
        ensure!(
            Self::start_and_end_of(string).all(Self::is_ascii_lowercase_or_digit),
            InvalidStartOrEnd
        );
        ensure!(
            string.chars().all(Self::is_valid_bucket_name_character),
            InvalidCharacter
        );

        Ok(BucketName(string.to_owned()))
    }
}

impl BucketName {
    /// Check if a `character` is an ASCII lower case letter or a digit (`[a-z0-9]`).
    fn is_ascii_lowercase_or_digit(character: char) -> bool {
        character.is_ascii_lowercase() || character.is_ascii_digit()
    }

    /// Check if a `character` is a valid bucket name character (`[-.a-z0-9]`).
    fn is_valid_bucket_name_character(character: char) -> bool {
        Self::is_ascii_lowercase_or_digit(character) || character == '-' || character == '.'
    }

    /// Extract the first and last characters of `string`, returning them as an iterator.
    ///
    /// # Panics
    ///
    /// Panics if the string is empty.
    fn start_and_end_of(string: &str) -> impl Iterator<Item = char> {
        [
            string
                .chars()
                .next()
                .expect("Check for empty string should come first"),
            string
                .chars()
                .last()
                .expect("Check for empty string should come first"),
        ]
        .into_iter()
    }
}

impl AsRef<String> for BucketName {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

/// Error when validating a bucket name.
#[derive(Debug, Error)]
pub enum InvalidBucketName {
    #[error("Bucket name must have at least 3 characters")]
    TooShort,

    #[error("Bucket name must be at most 63 characters")]
    TooLong,

    #[error("Bucket name must not start with \"xn--\"")]
    ForbiddenPrefix,

    #[error("Bucket name must not end with \"-s3alias\"")]
    ForbiddenSuffix,

    #[error("Bucket name must not be formatted as an IP address")]
    IpAmbiguity,

    #[error("Bucket name must start and end with a number or a lower case letter")]
    InvalidStartOrEnd,

    #[error("Bucket name must only contain lowercase letters, numbers, periods and hyphens")]
    InvalidCharacter,
}

/// Errors that can occur when using [`S3Storage`].
#[derive(Debug, Error)]
pub enum S3StorageError {
    #[error(transparent)]
    CheckIfBucketExists(#[from] SdkError<aws_sdk_s3::error::GetBucketLocationError>),

    #[error(transparent)]
    CreateBucket(#[from] SdkError<aws_sdk_s3::error::CreateBucketError>),

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
    /// Convert the error into an instance of the main [`enum@Error`] type.
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

/// Failure to create an [`S3Storage`] using a LocalStack configuration.
#[derive(Debug, Error)]
pub enum LocalStackError {
    #[error(transparent)]
    Endpoint(#[from] localstack::EndpointError),

    #[error(transparent)]
    InitializeBucket(#[from] Box<S3StorageError>),
}
