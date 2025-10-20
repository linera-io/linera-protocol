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

  // Use metrics port (21100)
  rule {
    source_labels = ["__meta_kubernetes_pod_container_port_number"]
    regex         = "21100"
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

  // Forward to OTLP converter for remote push if configured
  forward_to = [otelcol.receiver.prometheus.default.receiver]

  scrape_interval = "15s"
  scrape_timeout  = "10s"
}

// Expose Alloy's own metrics
prometheus.exporter.self "alloy" {}

prometheus.scrape "alloy_metrics" {
  targets    = prometheus.exporter.self.alloy.targets
  // Forward to OTLP converter for remote push if configured
  forward_to = [otelcol.receiver.prometheus.default.receiver]
}

// ==================== Prometheus Metrics Export (Optional) ====================

// Convert Prometheus metrics to OTLP and send to central (Prometheus 3.x uses OTLP)
// To enable, set these environment variables:
//   PROMETHEUS_OTLP_URL: https://your-prometheus-endpoint/otlp
//   PROMETHEUS_OTLP_USER: your-username
//   PROMETHEUS_OTLP_PASS: your-password

// Export Prometheus metrics as OTLP
otelcol.exporter.otlphttp "prometheus" {
  client {
    endpoint = env("PROMETHEUS_OTLP_URL")

    auth = otelcol.auth.basic.prometheus_credentials.handler

    tls {
      insecure_skip_verify = false
    }
  }
}

// Basic auth for Prometheus OTLP
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

// Write logs to central Loki (optional - only if env vars are set)
// To enable, set these environment variables:
//   LOKI_PUSH_URL: https://your-loki-endpoint/loki/api/v1/push
//   LOKI_PUSH_USER: your-username
//   LOKI_PUSH_PASS: your-password
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
    traces  = [otelcol.exporter.otlphttp.central.input]
  }
}

// Export traces to central Tempo (optional - only if env vars are set)
// To enable, set these environment variables:
//   TEMPO_OTLP_URL: https://your-tempo-endpoint/tempo/otlp
//   TEMPO_OTLP_USER: your-username
//   TEMPO_OTLP_PASS: your-password
otelcol.exporter.otlphttp "central" {
  client {
    endpoint = env("TEMPO_OTLP_URL")

    auth = otelcol.auth.basic.credentials.handler

    tls {
      insecure_skip_verify = false
    }
  }
}

// Basic auth for OTLP
otelcol.auth.basic "credentials" {
  username = env("TEMPO_OTLP_USER")
  password = env("TEMPO_OTLP_PASS")
}

// ==================== Metrics Exposition ====================

// Expose Prometheus-compatible metrics endpoint for central Prometheus to scrape
// This runs on port 12345 and exposes all collected metrics
// Note: Alloy's own metrics are already exposed via prometheus.exporter.self
