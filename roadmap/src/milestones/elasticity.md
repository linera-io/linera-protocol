# Elastic validators

The goal of this milestone is to demontrate easy upscaling and downscaling of shards with each validator.

We also aim at fully supporting crashes and restarts of workers.

## Requirements

* Workers load existing accounts from remote storage the first time, then keep updating storage.
  *Optionally*, local storage is also supported for testing / comparing.

* Workers are crash-resistant:
    - Must write to storage before answering client queries.
    - Must ensure that cross-shard requests are never lost (or duplicated) while minimizing cost in hot storage.
    - However, crashes may cause unused data to temporarily persist in hot storage.

* Clients do not send their queries directly to the shard but instead to a load balancer (i.e. the service entry-point).
    - The load balancer can detect if a worker is off-line, then re-assign its logical shards (aka user accounts).
    - To avoid downtime (see lease below), the load balancer should signal to a worker that a shard is moved out.

* To ensure sequential execution within each account (despite LB rebalancing or possible
  bugs etc), when a worker loads an account to it must takes a "lease" first on the
  account.
    - TODO: prevent race conditions between in-flight cross-shard requests and expiration of a lease

* System can be easily rolled on at least one cloud provider (e.g. AWS + docker swarm)

* Worker metrics are collected for testing, debugging and auto-scaling purposes.

* Clients can be rolled out to generate arbitrary large traffic.
