+++
title = "Goals"
insert_anchor_links = "left"
weight = 10
+++

The reference Zefchain validator aims to provide a fully *elastic* implementation of
the Zefchain protocol.

## Primary design goals

* The full Zefchain protocol should be supported with excellent performance and cost
  efficiency in mind.

* Important variations in client traffic should lead to appropriate up-scaling and
  down-scaling of service workers.

* Operational costs charged by a cloud provider should be covered by the transaction fees
  (pre-)paid by Zefchain clients.

* The system should tolerate crashes in workers.

* Cryptographic secrets should be protected.

* Cloud storage should support large-scale read-only accesses by auditors and new validators.

## Secondary design goals

* Workers should be as simple as possible. They may be also hosted by a cloud provider.

* Service logs should be created by workers and easily aggregated for consumption by the
  account owner (aka the operator).

* (MAYBE?) Physical shards should use different signing keys (PKI).

* Several cloud providers should be eventually supported (storage + workers).

* New validators should be able to verify data consistency lazily.

