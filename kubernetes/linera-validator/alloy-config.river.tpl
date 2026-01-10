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

// Relabel discovered pods to scrape all pods in namespace
discovery.relabel "linera_metrics" {
  targets = discovery.kubernetes.pods.targets

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

  // Set cluster label
  rule {
    target_label  = "cluster"
    replacement   = env("CLUSTER_NAME")
  }

  // Set validator label
  rule {
    target_label  = "validator"
    replacement   = env("VALIDATOR_NAME")
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

  // Conditional forwarding - only export if PROMETHEUS_ENABLED is set to "true"
  forward_to = env("PROMETHEUS_ENABLED") == "true" ? [otelcol.receiver.prometheus.default.receiver] : []

  scrape_interval = "15s"
  scrape_timeout  = "10s"
}

// Expose Alloy's own metrics
prometheus.exporter.self "alloy" {}

prometheus.scrape "alloy_metrics" {
  targets    = prometheus.exporter.self.alloy.targets
  // Conditional forwarding - only export if PROMETHEUS_ENABLED is set to "true"
  forward_to = env("PROMETHEUS_ENABLED") == "true" ? [otelcol.receiver.prometheus.default.receiver] : []
}

// ==================== Prometheus Metrics Export (Optional) ====================

// Convert Prometheus metrics to OTLP and send to external Prometheus
// Enabled via PROMETHEUS_ENABLED environment variable
// Requires: PROMETHEUS_OTLP_URL, PROMETHEUS_OTLP_USER, PROMETHEUS_OTLP_PASS

// Export Prometheus metrics as OTLP (only if enabled)
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

// Basic auth for Prometheus OTLP
otelcol.auth.basic "prometheus_credentials" {
  username = env("PROMETHEUS_OTLP_USER")
  password = env("PROMETHEUS_OTLP_PASS")
}

// Convert Prometheus metrics to OTLP format (only if enabled)
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

  // Set cluster label
  rule {
    target_label  = "cluster"
    replacement   = env("CLUSTER_NAME")
  }

  // Set validator label
  rule {
    target_label  = "validator"
    replacement   = env("VALIDATOR_NAME")
  }
}

// Read pod logs
loki.source.kubernetes "pods" {
  targets    = discovery.relabel.pod_logs.output
  // Conditional forwarding - only export if LOKI_ENABLED is set to "true"
  forward_to = env("LOKI_ENABLED") == "true" ? [loki.write.central.receiver] : []
}

// Write logs to external Loki (only if enabled)
// Enabled via LOKI_ENABLED environment variable
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
    // Conditional forwarding - only export if TEMPO_ENABLED is set to "true"
    traces  = env("TEMPO_ENABLED") == "true" ? [otelcol.exporter.otlphttp.central.input] : []
  }
}

// Export traces to external Tempo (only if enabled)
// Enabled via TEMPO_ENABLED environment variable
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

// Basic auth for OTLP
otelcol.auth.basic "credentials" {
  username = env("TEMPO_OTLP_USER")
  password = env("TEMPO_OTLP_PASS")
}

// ==================== Metrics Exposition ====================

// Expose Prometheus-compatible metrics endpoint for central Prometheus to scrape
// This runs on port 12345 and exposes all collected metrics
// Note: Alloy's own metrics are already exposed via prometheus.exporter.self
