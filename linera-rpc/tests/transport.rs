// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(web)]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg_attr(web, wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(web), tokio::test(flavor = "current_thread"))]
#[ignore]
// this test currently must be run manually, as it requires a Linera proxy to be running on 127.0.0.1:9000.
async fn client() {
    use linera_base::time::Duration;
    use linera_core::node::ValidatorNode as _;
    use linera_rpc::config::*;

    let network_config = ValidatorPublicNetworkPreConfig {
        protocol: NetworkProtocol::Grpc(TlsConfig::ClearText),
        host: "127.0.0.1".into(),
        port: 9000,
    };

    let node_options = linera_rpc::node_provider::NodeOptions {
        send_timeout: Duration::from_millis(100),
        recv_timeout: Duration::from_millis(100),
        notification_retry_delay: Duration::from_millis(100),
        notification_retries: 5,
    };

    let _ = linera_rpc::grpc::GrpcClient::new(network_config, node_options)
        .unwrap()
        .get_version_info()
        .await
        .unwrap();
}
