// Grafana Alloy configuration for Linera validator observability
// Collects metrics, logs, and traces and forwards to central stack

// ==================== Prometheus Metrics Scraping ====================

// Discover Kubernetes pods for scraping
discovery.kubernetes "pods" {
  role = "pod"

  namespaces {
    names = [env("NAMESPACE")]
  }
}

// Relabel discovered pods to scrape linera-proxy and linera-shard
discovery.relabel "linera_metrics" {
  targets = discovery.kubernetes.pods.targets

  // Only scrape pods with app=linera-validator label
  rule {
    source_labels = ["__meta_kubernetes_pod_label_app"]
    regex         = "linera-validator"
    action        = "keep"
  }

  // Set job label based on container name
  rule {
    source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label  = "job"
    replacement   = "linera-${1}"
  }

  // Set instance label to pod name
  rule {
    source_labels = ["__meta_kubernetes_pod_name"]
    target_label  = "instance"
  }

  // Set namespace label
  rule {
    source_labels = ["__meta_kubernetes_namespace"]
    target_label  = "namespace"
  }

  // Keep pods with a port named "metrics" (covers shards 21100, proxy 21100, block-exporter 9091)
  rule {
    source_labels = ["__meta_kubernetes_pod_container_port_name"]
    regex         = "metrics"
    action        = "keep"
  }

  // Set __address__ to pod IP:port
  rule {
    source_labels = ["__meta_kubernetes_pod_ip", "__meta_kubernetes_pod_container_port_number"]
    separator     = ":"
    target_label  = "__address__"
  }
}

// Scrape metrics from discovered pods
prometheus.scrape "linera_metrics" {
  targets = discovery.relabel.linera_metrics.output

  forward_to = [otelcol.receiver.prometheus.default.receiver]

  scrape_interval = "15s"
  scrape_timeout  = "10s"
}

// Expose Alloy's own metrics
prometheus.exporter.self "alloy" {}

prometheus.scrape "alloy_metrics" {
  targets    = prometheus.exporter.self.alloy.targets
  forward_to = [otelcol.receiver.prometheus.default.receiver]
}

// ==================== Prometheus Metrics Export ====================

// Convert Prometheus metrics to OTLP and send to external Prometheus
// Requires: PROMETHEUS_OTLP_URL, PROMETHEUS_OTLP_USER, PROMETHEUS_OTLP_PASS

otelcol.exporter.otlphttp "prometheus" {
  client {
    endpoint = env("PROMETHEUS_OTLP_URL")

    auth = otelcol.auth.basic.prometheus_credentials.handler

    tls {
      insecure_skip_verify = false
    }

    compression = "gzip"

    headers = {
      "Content-Type" = "application/x-protobuf",
    }
  }

  encoding = "proto"
}

otelcol.auth.basic "prometheus_credentials" {
  username = env("PROMETHEUS_OTLP_USER")
  password = env("PROMETHEUS_OTLP_PASS")
}

// Convert Prometheus metrics to OTLP format
otelcol.receiver.prometheus "default" {
  output {
    metrics = [otelcol.exporter.otlphttp.prometheus.input]
  }
}

// ==================== Loki Logs Collection ====================

// Discover Kubernetes pods for log collection
discovery.kubernetes "pod_logs" {
  role = "pod"

  namespaces {
    names = [env("NAMESPACE")]
  }
}

// Relabel discovered pods for log collection
discovery.relabel "pod_logs" {
  targets = discovery.kubernetes.pod_logs.targets

  // Only collect logs from linera-validator pods
  rule {
    source_labels = ["__meta_kubernetes_pod_label_app"]
    regex         = "linera-validator"
    action        = "keep"
  }

  // Set pod label
  rule {
    source_labels = ["__meta_kubernetes_pod_name"]
    target_label  = "pod"
  }

  // Set container label
  rule {
    source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label  = "container"
  }

  // Set namespace label
  rule {
    source_labels = ["__meta_kubernetes_namespace"]
    target_label  = "namespace"
  }
}

// Read pod logs
loki.source.kubernetes "pods" {
  targets    = discovery.relabel.pod_logs.output
  forward_to = [loki.write.central.receiver]
}

// Write logs to external Loki
// Requires: LOKI_PUSH_URL, LOKI_PUSH_USER, LOKI_PUSH_PASS
loki.write "central" {
  endpoint {
    url = env("LOKI_PUSH_URL")

    basic_auth {
      username = env("LOKI_PUSH_USER")
      password = env("LOKI_PUSH_PASS")
    }

    tls_config {
      insecure_skip_verify = false
    }
  }

  external_labels = {
    cluster   = env("CLUSTER_NAME"),
    validator = env("VALIDATOR_NAME"),
  }
}

// ==================== Tempo Traces Collection ====================

// OTLP receiver for traces
otelcol.receiver.otlp "default" {
  grpc {
    endpoint = "0.0.0.0:4317"
  }

  http {
    endpoint = "0.0.0.0:4318"
  }

  output {
    traces = [otelcol.exporter.otlphttp.central.input]
  }
}

// Export traces to external Tempo
// Requires: TEMPO_OTLP_URL, TEMPO_OTLP_USER, TEMPO_OTLP_PASS
otelcol.exporter.otlphttp "central" {
  client {
    endpoint = env("TEMPO_OTLP_URL")

    auth = otelcol.auth.basic.credentials.handler

    tls {
      insecure_skip_verify = false
    }
  }
}

otelcol.auth.basic "credentials" {
  username = env("TEMPO_OTLP_USER")
  password = env("TEMPO_OTLP_PASS")
}
