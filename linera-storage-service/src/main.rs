
use linera_storage_service::{
    key_value_store::store_processor_server::StoreProcessorServer,
    server::{ServiceStoreServer, ServiceStoreServerInternal, ServiceStoreServerOptions}
};
use linera_views::{memory::MemoryStore, store::CommonStoreConfig};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;


#[tokio::main]
async fn main() {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    let internal_event_filter = {
        match std::env::var_os("RUST_LOG_SPAN_EVENTS") {
            Some(mut value) => {
                value.make_ascii_lowercase();
                let value = value
                    .to_str()
                    .expect("test-log: RUST_LOG_SPAN_EVENTS must be valid UTF-8");
                value
                    .split(',')
                    .map(|filter| match filter.trim() {
                        "new" => FmtSpan::NEW,
                        "enter" => FmtSpan::ENTER,
                        "exit" => FmtSpan::EXIT,
                        "close" => FmtSpan::CLOSE,
                        "active" => FmtSpan::ACTIVE,
                        "full" => FmtSpan::FULL,
                        _ => panic!("test-log: RUST_LOG_SPAN_EVENTS must contain filters separated by `,`.\n\t\
                                     For example: `active` or `new,close`\n\t\
                                     Supported filters: new, enter, exit, close, active, full\n\t\
                                     Got: {}", value),
                    })
                    .fold(FmtSpan::NONE, |acc, filter| filter | acc)
            }
            None => FmtSpan::NONE,
        }
    };
    tracing_subscriber::fmt()
        .with_span_events(internal_event_filter)
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let options = <ServiceStoreServerOptions as clap::Parser>::parse();
    let common_config = CommonStoreConfig::default();
    let namespace = "linera_storage_service";
    let root_key = &[];
    let (store, endpoint) = match options {
        ServiceStoreServerOptions::Memory { endpoint } => {
            let store =
                MemoryStore::new(common_config.max_stream_queries, namespace, root_key).unwrap();
            let store = ServiceStoreServerInternal::Memory(store);
            (store, endpoint)
        }
        #[cfg(with_rocksdb)]
        ServiceStoreServerOptions::RocksDb { path, endpoint } => {
            let path_buf = path.into();
            let path_with_guard = PathWithGuard::new(path_buf);
            // The server is run in multi-threaded mode so we can use the block_in_place.
            let spawn_mode = RocksDbSpawnMode::get_spawn_mode_from_runtime();
            let config = RocksDbStoreConfig::new(spawn_mode, path_with_guard, common_config);
            let store = RocksDbStore::maybe_create_and_connect(&config, namespace, root_key)
                .await
                .expect("store");
            let store = ServiceStoreServerInternal::RocksDb(store);
            (store, endpoint)
        }
    };

    let heaped_store = ServiceStoreServer::new_from_store(store);

    let endpoint = endpoint.parse().unwrap();
    info!("Starting linera_storage_service on endpoint={}", endpoint);
    Server::builder()
        .add_service(StoreProcessorServer::from_arc(heaped_store))
        .serve(endpoint)
        .await
        .expect("a successful running of the server");
}
