# Linera Validator Helm Directory

This directory holds the requisite
Helm [Charts](https://helm.sh/docs/topics/charts/) and files to deploy a Linera
validator to Kubernetes using Helm.

## Outline

The directory is split into a few key parts:

- `Chart.yaml`: The validator's helm manifest. Defines versions, dependencies,
  etc.
- `Chart.lock`: A lock file for the chart's dependencies.
- `charts/`:  Holds the source for the validator chart's dependencies.
- `templates/`: The Kubernetes manifests templates which Helm uses to
  parameterise the validators.
- `working`: A directory which is in `.gitignore` (but not by Helm, i.e. it is
  *not* in the .helmignore) used to store intermediate artifacts for
  bootstrapping networks. For example genesis configuration, server
  configuration, etc.
- `values-*.yaml`: Files which are substituted into the validators' Kubernetes
  manifests.

## Usage

To deploy a Linera validator, use the Helm chart with `helmfile` and `kubectl`
directly against your target cluster.

