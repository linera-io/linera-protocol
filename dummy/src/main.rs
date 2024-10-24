use std::time::Duration;
use linera_base::async_graphql::futures_util::StreamExt;
use linera_rpc::config::{NetworkProtocol, TlsConfig, ValidatorPublicNetworkConfig, ValidatorPublicNetworkPreConfig};
use linera_rpc::grpc::api::validator_node_client::ValidatorNodeClient;
use linera_rpc::grpc::{transport, GrpcClient, GRPC_MAX_MESSAGE_SIZE};
use linera_rpc::grpc::api::{ChainId, SubscriptionRequest};
// use linera_rpc::grpc::api::validator_node_server::ValidatorNode;
use linera_rpc::NodeOptions;
use linera_core::node::ValidatorNode;

#[tokio::main]
async fn main() {
    let network = ValidatorPublicNetworkConfig {
        host: "validator-5.testnet-boole.linera.net".to_string(),
        port: 443,
        protocol: NetworkProtocol::Grpc(TlsConfig::Tls)
    };
    let options = NodeOptions {
        send_timeout: Duration::from_millis(4000),
        recv_timeout: Duration::from_millis(4000),
        retry_delay: Duration::from_millis(4000),
        max_retries: 10,
    };
    let client = GrpcClient::new(network, options).unwrap();
    let my_chain: linera_base::identifiers::ChainId = "9eb222e81c6ca9ea7986b2b1266e8ccb89035d510888fbd0a3e28e1702cfcd7e".parse().unwrap();
    let mut notification_stream = client.subscribe(vec![my_chain]).await.unwrap();
    while let Some(notification) = notification_stream.next().await {
        println!("{:?}", notification);
    }
}

// #[tokio::main]
// async fn main() {
//     let options = transport::Options {
//         connect_timeout: Some(Duration::from_millis(4000)),
//         timeout: Some(Duration::from_millis(4000)),
//     };
//     let channel = transport::create_channel("https://validator-4.testnet-boole.linera.net:443".to_string(), &options).unwrap();
//     let mut client = ValidatorNodeClient::new(channel)
//         .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
//         .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);
//     let my_chain: linera_base::identifiers::ChainId = "887cbd0a1f88406a5f5661e067c6c215deb8df910c208e8774daf898bf480f1f".parse().unwrap();
//     let subscription_request = SubscriptionRequest {
//         chain_ids: vec![my_chain.into()],
//     };
//     let mut stream = client.subscribe(subscription_request).await.unwrap().into_inner();
//
//     while let Some(notification) = stream.next().await {
//         println!("{:?}", notification);
//     }
//
//
//     unimplemented!()
// }
