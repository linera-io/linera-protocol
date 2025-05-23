// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{any::Any, path::Path};

use async_trait::async_trait;
use indicatif::MultiProgress;
use linera_client::config::{WalrusConfig, WalrusEpoch, WalrusStoreConfig};
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
use walrus_core::{
    encoding::encoded_blob_length_for_n_shards, metadata::BlobMetadataApi, BlobId, EncodingType,
    EpochCount, DEFAULT_ENCODING,
};
use walrus_sdk::{
    client::{resource::RegisterBlobOp, Client, WalrusStoreBlob, WalrusStoreBlobApi},
    config::ClientConfig,
    error::ClientErrorKind,
    store_when::StoreWhen,
    sui::{
        client::{BlobPersistence, PostStoreAction, SuiContractClient},
        config::WalletConfig,
    },
};
use walrus_service::test_utils::test_cluster::E2eTestSetupBuilder;

pub(super) struct WalrusClientInner<T = ()> {
    inner: Client<SuiContractClient>,
    dry_run: bool,
    epochs: WalrusEpoch,
    store_when: StoreWhen,
    post_store: PostStoreAction,
    encoding_type: EncodingType,
    persistence: BlobPersistence,
    _leaked_guard: T,
}

impl<T: ClientInitializer<Guard = T>> WalrusClientInner<T> {
    pub(super) async fn new(config: &WalrusConfig<WalrusStoreConfig>) -> anyhow::Result<Self> {
        if config
            .behaviour
            .encoding_type
            .is_some_and(|encoding| !encoding.is_supported())
        {
            anyhow::bail!(ClientErrorKind::UnsupportedEncodingType(
                config
                    .behaviour
                    .encoding_type
                    .expect("just checked that option is Some")
            ));
        }

        let (client, _leaked_guard) = T::try_make_initial_connection_and_client(config).await?;

        let store_when =
            StoreWhen::from_flags(config.behaviour.force, config.behaviour.ignore_resources);
        let persistence = BlobPersistence::from_deletable(config.behaviour.deletable);
        let post_store = PostStoreAction::from_share(config.behaviour.share);
        let encoding_type = config.behaviour.encoding_type.unwrap_or(DEFAULT_ENCODING);

        if persistence.is_deletable() && post_store == PostStoreAction::Share {
            anyhow::bail!("deletable blobs cannot be shared");
        }

        Ok(Self {
            inner: client,
            dry_run: config.behaviour.dry_run,
            epochs: config.behaviour.epochs,
            store_when,
            post_store,
            persistence,
            encoding_type,
            _leaked_guard,
        })
    }

    pub(super) async fn dispatch(&self, blob: &[u8]) -> anyhow::Result<BlobId> {
        self.dispatch_inner(blob).await
    }

    async fn dispatch_inner(&self, blob: &[u8]) -> anyhow::Result<BlobId> {
        let system_object = self
            .inner
            .sui_client()
            .read_client
            .get_system_object()
            .await?;
        let max_epochs_ahead = system_object.max_epochs_ahead();
        let epochs_ahead = self.epochs.try_into_epoch_count(max_epochs_ahead)?;

        if self.dry_run {
            return self.dispatch_dry_run(blob, epochs_ahead).await;
        }

        tracing::info!("storing blob on Walrus");
        let mut results = self
            .inner
            .reserve_and_store_blobs_retry_committees(
                &[blob],
                self.encoding_type,
                epochs_ahead,
                self.store_when,
                self.persistence,
                self.post_store,
                None,
            )
            .await?;

        assert_eq!(results.len(), 1, "there should be one");
        let result = results.pop().expect("there should be one");

        let blob_id = match result.is_not_stored() {
            true => {
                tracing::error!(
                    "unable to store the blob: {:#?} on walrus",
                    result.blob_id()
                );
                anyhow::bail!(
                    "unable to store the blob: {:#?} on walrus",
                    result.blob_id()
                );
            }

            false => {
                let blob_id = result
                    .blob_id()
                    .expect("BlobId is always some on a successfull storage");
                tracing::info!("successfully stored blob with id: {} on walrus", blob_id);
                blob_id
            }
        };

        Ok(blob_id)
    }

    async fn dispatch_dry_run(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
    ) -> anyhow::Result<BlobId> {
        tracing::info!("performing dry-run store");
        let encoding_config = self.inner.encoding_config();

        let blob_with_identifier =
            WalrusStoreBlob::<String>::new_unencoded(blob, format!("blob_{:06}", 0));
        let unencoded_blob = blob_with_identifier.get_blob();
        let multi_pb = MultiProgress::new();
        let (_sliver_pairs, metadata) =
            self.inner
                .encode_pairs_and_metadata(unencoded_blob, self.encoding_type, &multi_pb)?;

        let blob_id = *metadata.blob_id();

        let unencoded_size = metadata.metadata().unencoded_length();
        let encoded_size = encoded_blob_length_for_n_shards(
            encoding_config.n_shards(),
            unencoded_size,
            self.encoding_type,
        )
        .expect("must be valid as the encoding succeeded");
        let storage_cost = self.inner.get_price_computation().await?.operation_cost(
            &RegisterBlobOp::RegisterFromScratch {
                encoded_length: encoded_size,
                epochs_ahead,
            },
        );

        let output = DryRunOutput {
            blob_id,
            encoding_type: metadata.metadata().encoding_type(),
            unencoded_size,
            encoded_size,
            storage_cost,
        };

        let output_to_print = serde_json::to_string_pretty(&output)?;
        tracing::info!("performed dry-run store with the result: {output_to_print}");

        Ok(blob_id)
    }
}

impl<T> WalrusClientInner<T> {
    fn load_configuration(path: &Path, context: Option<&str>) -> anyhow::Result<ClientConfig> {
        let (config, context) = ClientConfig::load_from_multi_config(path, context)?;
        tracing::info!(
            "using Walrus configuration from '{}' with {} context",
            path.display(),
            context.map_or("default".to_string(), |c| format!("'{}'", c))
        );

        Ok(config)
    }
}

#[async_trait]
pub(super) trait ClientInitializer {
    type Guard;

    async fn try_make_initial_connection_and_client(
        config: &WalrusConfig<WalrusStoreConfig>,
    ) -> anyhow::Result<(Client<SuiContractClient>, Self::Guard)>;
}

#[async_trait]
impl ClientInitializer for Box<dyn Any + Send + Sync> {
    type Guard = Box<dyn Any + Send + Sync>;

    async fn try_make_initial_connection_and_client(
        _config: &WalrusConfig<WalrusStoreConfig>,
    ) -> anyhow::Result<(Client<SuiContractClient>, Self::Guard)> {
        let (_cluster_handle, _test_cluster, client, _system_context) =
            E2eTestSetupBuilder::new().build().await?;

        Ok((
            client.inner,
            Box::new((
                _cluster_handle,
                _test_cluster,
                client.temp_dir,
                _system_context,
            )),
        ))
    }
}

#[async_trait]
impl ClientInitializer for () {
    type Guard = ();

    async fn try_make_initial_connection_and_client(
        config: &WalrusConfig<WalrusStoreConfig>,
    ) -> anyhow::Result<(Client<SuiContractClient>, Self::Guard)> {
        let client_config = WalrusClientInner::<()>::load_configuration(
            config.client_config.as_path(),
            config.context.as_deref(),
        )?;
        let wallet_config = WalletConfig::from_path(config.wallet_config.clone());
        let wallet = WalletConfig::load_wallet_context(
            Some(&wallet_config),
            client_config
                .communication_config
                .sui_client_request_timeout,
        )?;

        let sui_client = client_config.new_contract_client(wallet, None).await?;
        let refresh_handle = client_config
            .refresh_config
            .build_refresher_and_run(sui_client.read_client().clone())
            .await?;
        let client = Client::new_contract_client(client_config, refresh_handle, sui_client).await?;

        Ok((client, ()))
    }
}

#[cfg(not(with_testing))]
pub(super) type WalrusClient = WalrusClientInner;

#[cfg(with_testing)]
pub(super) type WalrusClient = WalrusClientInner<Box<dyn Any + Send + Sync>>;

#[serde_as]
#[derive(Debug, Serialize)]
struct DryRunOutput {
    /// The blob ID.
    #[serde_as(as = "DisplayFromStr")]
    pub blob_id: BlobId,
    /// The size of the unencoded blob (in bytes).
    pub unencoded_size: u64,
    /// The size of the encoded blob (in bytes).
    pub encoded_size: u64,
    /// The storage cost (in MIST).
    pub storage_cost: u64,
    /// The encoding type used for the blob.
    pub encoding_type: EncodingType,
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use futures::FutureExt;
    use linera_client::config::{WalrusConfig, WalrusStoreConfig};

    use crate::{runloops::walrus::client::WalrusClient, test_utils::init_walrus_test_enviroment};

    #[test_log::test(tokio::test)]
    async fn test_client() -> anyhow::Result<()> {
        let path = PathBuf::new();
        let config = WalrusConfig {
            log: path.to_path_buf(),
            client_config: path.to_path_buf(),
            wallet_config: path,
            context: None,
            behaviour: WalrusStoreConfig {
                ..Default::default()
            },
        };

        init_walrus_test_enviroment();

        let client = WalrusClient::new(&config).boxed().await?;
        let blob = b"test";
        let blob_id = client.dispatch(blob).await?;

        assert_eq!(
            blob_id.to_string(),
            "Rp9dTbIgV6BaojCIP08ahuyiDQrMsIp3N_8JYvDbJRY".to_owned()
        );

        Ok(())
    }

    // this may panic the background spawned threads
    // but not the main thread
    // it should be fine as maybe they are just dropped before being used
    #[test_log::test(tokio::test)]
    async fn test_client_dry_run() -> anyhow::Result<()> {
        let path = PathBuf::new();
        let config = WalrusConfig {
            log: path.to_path_buf(),
            client_config: path.to_path_buf(),
            wallet_config: path,
            context: None,
            behaviour: WalrusStoreConfig {
                dry_run: true,
                ..Default::default()
            },
        };

        init_walrus_test_enviroment();

        let client = WalrusClient::new(&config).boxed().await?;
        let blob = b"test";
        let blob_id = client.dispatch(blob).await?;

        assert_eq!(
            blob_id.to_string(),
            "Rp9dTbIgV6BaojCIP08ahuyiDQrMsIp3N_8JYvDbJRY".to_owned()
        );

        Ok(())
    }
}
