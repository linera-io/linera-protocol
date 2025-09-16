id = {{ .exporterId }}

metrics_port = {{ .Values.blockExporter.metricsPort }}

[service_config]
host = "0.0.0.0"
port = {{ .Values.blockExporter.port }}

[destination_config]
committee_destination = true

[[destination_config.destinations]]
file_name = "/data/linera-exporter.log"
kind = "Logging"

[[destination_config.destinations]]
kind = "Indexer"
tls = "ClearText"
port = {{ .Values.indexer.port }}
endpoint = "linera-indexer"

[limits]
persistence_period_ms = 299000
work_queue_size = 256
blob_cache_weight_mb = 1024
blob_cache_items_capacity = 8192
block_cache_weight_mb = 1024
block_cache_items_capacity = 8192
auxiliary_cache_size_mb = 1024