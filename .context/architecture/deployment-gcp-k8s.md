# Deployment (Docker / GCP / Kubernetes)

## Purpose
How validator artifacts are built and deployed. **Important:** the Kubernetes/Helm/
Terraform manifests do **not** live in this repo — they live in
`linera-io/linera-artifacts`, with operator docs at <https://docs.infra.linera.net/>.
This pack covers what's in-repo (image builds, Docker Compose) and points to the rest.

## Entry points (in this repo)
- `docker/Dockerfile`, `docker/Dockerfile.{exporter,bridge,indexer,explorer,foundry}` —
  image definitions.
- `docker/docker-compose.yml` — single-host stack: `proxy`, `shard-0..3`, `scylla`,
  `prometheus`, `grafana`, `watchtower`, `caddy`.
- `docker/docker-compose.alloy.yml`, `docker/alloy-config.river` — telemetry sidecar.
- `.github/workflows/build_image.yml`, `docker_image.yml`, `build_image.yaml` files in
  `docker/` — GCP image builds/pushes.
- `.github/workflows/remote-net-test.yml` — multi-validator integration test.
- `scripts/{deploy-validator,backup-validator-keys,wait-for-kubernetes-service,fix-validator-env}.sh`
  — operational helpers (the only k8s-aware scripts in-repo).
- `docs/operators.md` → redirects to docs.infra.linera.net.

## Entry points (external — not in this repo)
- `github.com/linera-io/linera-artifacts` — **Helm charts**, compose stack, deploy
  scripts, example cloud values. The `company-context` deployment backend and prod-state
  reads target this + the live cluster, not this repo. (See also #6067 DevSpace loop.)

## How it works
CI builds images and pushes to GCP Container Registry. Production runs on GCP/Kubernetes
via Helm charts in `linera-artifacts`. The in-repo Docker Compose stack is for local/dev
and CI, not production topology.

## Invariants & gotchas
- Don't look for `*.tf` / `Chart.yaml` here — they're in `linera-artifacts`. A task that
  needs them must reference the external repo.
- `watchtower` in the compose stack auto-updates images — fine for dev, **not** a prod
  deploy mechanism.
- Image version ↔ release is tracked via `docs/RELEASE_*` files and `scripts/bump_version.sh`.

## Related
- Packs: `observability.md`, `validator.md`.
- `company-context` `metrics_summary`/`logs_summary` + the read-only prod-state backend
  (DESIGN.md §8) surface live deploy state; this pack is the static map.

## How agents should use this
For "how is this deployed", read `docker/docker-compose.yml` for local shape, then defer
to `linera-artifacts` / docs.infra.linera.net for prod. For live state (images, replicas),
use the prod-state backend, never `kubectl` directly.

## Freshness
- Depends on: `docker/docker-compose.yml`, `docker/Dockerfile*`,
  `.github/workflows/build_image.yml`, `docs/operators.md`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
