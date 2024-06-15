use std::{borrow::Cow, collections::BTreeMap, iter, net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{
    futures_util::Stream,
    parser::types::{DocumentOperations, ExecutableDocument, OperationType},
    resolver_utils::ContainerType,
    Error, MergedObject, Object, OutputType, Request, ScalarType, Schema, ServerError,
    SimpleObject, Subscription,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{extract::Path, http::StatusCode, response, response::IntoResponse, Extension, Router};
use futures::{
    future::{self},
    lock::{Mutex, MutexGuard, OwnedMutexGuard},
    Future,
};
use linera_base::{
    crypto::{CryptoError, CryptoHash, PublicKey},
    data_types::{Amount, ApplicationPermissions, Blob, TimeDelta, Timestamp},
    identifiers::{ApplicationId, BlobId, BytecodeId, ChainId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
    BcsHexParseError,
};
use linera_chain::{data_types::HashedCertificateValue, ChainStateView};
use linera_core::{
    client::{ArcChainClient, ChainClient, ChainClientError},
    data_types::{ClientOutcome, RoundTimeout},
    node::{NotificationStream, ValidatorNode, ValidatorNodeProvider},
    worker::{Notification, Reason},
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::{AdminOperation, Recipient, SystemChannel, UserData},
    Bytecode, Operation, Query, Response, SystemOperation, UserApplicationDescription,
    UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error as ThisError;
use tokio::sync::OwnedRwLockReadGuard;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info};

use crate::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext},
    util,
};

#[derive(SimpleObject, Serialize, Deserialize, Clone)]
pub struct Chains {
    pub list: Vec<ChainId>,
    pub default: Option<ChainId>,
}

pub type ClientMapInner<P, S> = BTreeMap<ChainId, ArcChainClient<P, S>>;
pub(crate) struct ChainClients<P, S>(Arc<Mutex<ClientMapInner<P, S>>>);

impl<P, S> Clone for ChainClients<P, S> {
    fn clone(&self) -> Self {
        ChainClients(self.0.clone())
    }
}

impl<P, S> Default for ChainClients<P, S> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
}

impl<P, S> ChainClients<P, S> {
    async fn client(&self, chain_id: &ChainId) -> Option<ArcChainClient<P, S>> {
        Some(self.0.lock().await.get(chain_id)?.clone())
    }

    pub(crate) async fn client_lock(
        &self,
        chain_id: &ChainId,
    ) -> Option<OwnedMutexGuard<ChainClient<P, S>>> {
        Some(self.client(chain_id).await?.0.lock_owned().await)
    }

    pub(crate) async fn try_client_lock(
        &self,
        chain_id: &ChainId,
    ) -> Result<OwnedMutexGuard<ChainClient<P, S>>, Error> {
        self.client_lock(chain_id)
            .await
            .ok_or_else(|| Error::new(format!("Unknown chain ID: {}", chain_id)))
    }

    pub(crate) async fn map_lock(&self) -> MutexGuard<ClientMapInner<P, S>> {
        self.0.lock().await
    }
}
