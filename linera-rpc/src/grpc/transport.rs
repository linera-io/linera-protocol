// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::NodeOptions;

/// Configuration for creating gRPC transport channels.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// The maximum time to wait when establishing a connection.
    pub connect_timeout: Option<linera_base::time::Duration>,
    /// The maximum time to wait for a request to complete.
    pub timeout: Option<linera_base::time::Duration>,
}

impl From<&'_ NodeOptions> for Options {
    fn from(node_options: &NodeOptions) -> Self {
        Self {
            connect_timeout: Some(node_options.send_timeout),
            timeout: Some(node_options.recv_timeout),
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(web)] {
        pub use tonic_web_wasm_client::{Client as Channel, Error};

        /// Creates a transport channel for the given address.
        pub fn create_channel(address: String, _options: &Options) -> Result<Channel, Error> {
            // TODO(#1817): this should respect `options`
            Ok(tonic_web_wasm_client::Client::new(address))
        }
    } else {
        pub use tonic::transport::{Channel, Error};

        /// Creates a transport channel for the given address.
        pub fn create_channel(
            address: String,
            options: &Options,
        ) -> Result<Channel, Error> {
            let mut endpoint = tonic::transport::Endpoint::from_shared(address)?
                .tls_config(tonic::transport::channel::ClientTlsConfig::default().with_webpki_roots())?
                .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
                .http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .keep_alive_timeout(std::time::Duration::from_secs(10))
                .keep_alive_while_idle(true)
                // Without this, `hyper`'s fixed defaults apply: a 2 MiB stream window and a
                // 5 MiB connection window shared by every concurrent request to the
                // validator. Since responses can be up to `GRPC_MAX_MESSAGE_SIZE`, that caps
                // a download at roughly the window size per round trip.
                .http2_adaptive_window(true);

            if let Some(timeout) = options.connect_timeout {
                endpoint = endpoint.connect_timeout(timeout);
            }
            if let Some(timeout) = options.timeout {
                endpoint = endpoint.timeout(timeout);
            }
            Ok(endpoint.connect_lazy())
        }
    }
}
