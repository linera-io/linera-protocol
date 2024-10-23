// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::NodeOptions;

#[derive(Clone, Debug, Default)]
pub struct Options {
    pub connect_timeout: Option<linera_base::time::Duration>,
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

        pub fn create_channel(address: String, _options: &Options) -> Result<Channel, Error> {
            // TODO(#1817): this should respect `options`
            Ok(tonic_web_wasm_client::Client::new(address))
        }
    } else {
        pub use tonic::transport::{Channel, Error};

        pub fn create_channel(
            address: String,
            options: &Options,
        ) -> Result<Channel, Error> {
            let mut endpoint = tonic::transport::Endpoint::from_shared(address)?
                .tls_config(tonic::transport::channel::ClientTlsConfig::default().with_webpki_roots())?;

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
