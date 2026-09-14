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
    use linera_rpc::grpc::{
        transport::{create_channel, Options},
        GrpcClient,
    };

    let address = "http://127.0.0.1:9000".to_string();
    let options = Options {
        connect_timeout: Some(Duration::from_millis(100)),
        timeout: Some(Duration::from_millis(100)),
    };
    let channel = create_channel(address.clone(), &options).unwrap();
    GrpcClient::new(
        address,
        channel,
        linera_rpc::node_provider::NodeOptions {
            send_timeout: Duration::from_millis(100),
            recv_timeout: Duration::from_millis(100),
            retry_delay: Duration::from_millis(100),
            max_retries: 5,
            max_backoff: linera_rpc::node_provider::DEFAULT_MAX_BACKOFF,
        },
        std::sync::Arc::new(papaya::HashMap::new()),
    )
    .get_version_info()
    .await
    .unwrap();
}
