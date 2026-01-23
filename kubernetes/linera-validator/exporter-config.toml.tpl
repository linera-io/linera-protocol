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
{{- if .Values.blockExporter.indexerEndpoint }}
tls = "Tls"
port = 443
endpoint = "{{ .Values.blockExporter.indexerEndpoint }}"
{{- else }}
tls = "ClearText"
port = {{ .Values.blockExporter.indexerPort }}
endpoint = "linera-indexer-{{ .Values.networkName }}.linera-indexer.svc.cluster.local"
{{- end }}

[limits]
persistence_period_ms = 5_000
work_queue_size = 256
blob_cache_weight_mb = 1024
blob_cache_items_capacity = 8192
block_cache_weight_mb = 1024
block_cache_items_capacity = 8192
auxiliary_cache_size_mb = 1024