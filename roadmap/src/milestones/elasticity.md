# Elastic validators

The goal of this milestone is to demonstrate easy upscaling and downscaling of shards with each validator.

We also aim at fully supporting crashes and restarts of workers.

## Requirements

* Workers load existing chains from remote storage the first time, then keep updating storage.
  *Optionally*, local storage is also supported for tests and benchmarks.

* Workers are crash-resistant:
    - They must write to storage before answering client queries.
    - They must ensure that cross-chain requests are never lost (or duplicated) while minimizing cost in hot storage.
    - However, crashes may cause unused data to temporarily persist in hot storage.

* Clients do not send their queries directly to the shard but instead to a load balancer (i.e. the service entry-point).
    - The load balancer can detect if a worker is off-line, then re-assign its logical shards (aka user chains).
    - To avoid downtime (see lease below), the load balancer should signal to a worker that a shard is moved out.
    - Cross-shard requests need to be routed using the latest shard assignment.
    - However, this is a best effort and occasionally cross-chain requests will be lost (then re-tried) when
      the shard assignment changes.

* To ensure sequential execution of transactions within each chain (despite LB rebalancing or possible
  bugs etc), every worker must takes a "lease" (i.e. a lock with a TTL) before it can read or modify a chain.
    - At all time, there can be no more than one worker in possession of the lease for a given chain.
    - A lease terminates when its time expires before renewal (crashed worker) or when it is explicitly invalidated (shard re-assignment).
    - Active workers should renew their leases regularly and ahead of time to avoid downtime due to network latency.
    - The validity of a lease should be considered with a safety margin to allow for small discrepancies between clocks in the system.

* Linera deployments can be easily rolled on at least one cloud provider (e.g. AWS + docker swarm)

* Worker metrics are collected for testing, debugging and auto-scaling purposes.

* Linera clients can be rolled out to generate arbitrary large traffic.
