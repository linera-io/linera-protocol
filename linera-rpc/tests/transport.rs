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

    let retry_delay = Duration::from_millis(100);
    let max_retries = 5;
    let address = "127.0.0.1:9000".to_string();
    let options = Options {
        connect_timeout: Some(Duration::from_millis(100)),
        timeout: Some(Duration::from_millis(100)),
    };
    let channel = create_channel(address.clone(), &options).unwrap();
    let _ = GrpcClient::new(address, channel, retry_delay, max_retries)
        .get_version_info()
        .await
        .unwrap();
}
