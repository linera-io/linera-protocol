//use aws_sdk_sqs::Region;
use aws_smithy_http::endpoint::Endpoint;
use http::Uri;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let shared_config = aws_config::load_from_env().await;
    let sqs_client = sqs_client(&shared_config);
    let s3_client = s3_client(&shared_config);

    let buckets = s3_client.list_buckets().send().await?;
    println!("s3 buckets: {:?}", buckets.buckets().unwrap_or_default());

    let queues = sqs_client.list_queues().send().await?;
    println!("SQS queues: {:?}", queues.queue_urls().unwrap_or_default());
    Ok(())
}

/// If LOCALSTACK environment variable is set, use LocalStack endpoints.
/// You can use your own method for determining whether to use LocalStack endpoints.
fn use_localstack() -> bool {
    std::env::var("LOCALSTACK").is_ok()
}

fn localstack_endpoint() -> Endpoint {
    Endpoint::immutable(Uri::from_static("http://localhost:4566/"))
}

fn sqs_client(conf: &aws_types::SdkConfig) -> aws_sdk_sqs::Client {
    let mut sqs_config_builder = aws_sdk_sqs::config::Builder::from(conf);
    if use_localstack() {
        sqs_config_builder = sqs_config_builder.endpoint_resolver(localstack_endpoint())
    }
    aws_sdk_sqs::Client::from_conf(sqs_config_builder.build())
}

fn s3_client(conf: &aws_types::SdkConfig) -> aws_sdk_s3::Client {
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(conf);
    if use_localstack() {
        s3_config_builder = s3_config_builder.endpoint_resolver(localstack_endpoint());
    }
    aws_sdk_s3::Client::from_conf(s3_config_builder.build())
}
