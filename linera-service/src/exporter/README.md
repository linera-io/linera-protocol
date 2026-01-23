# Linera Block Exporter

This module provides the executable to run a Linera block exporter. This binary is similar to `linera-proxy` and `linera-server`. Block exporter listens for the notifications about the new block from the chain workers in real-time just like the `linera-proxy`. The new blocks are resolved for their dependencies and are exported to the desired destinations in order.

## How to start

The block exporter requires a running validator. The chain workers from the validators will feed the exporter about the new incoming data and the exporter will then use the shared storage to read those data, hence a shared storage instance is also required.

Most of the CLI arguments required to run the block exporter are similar to that of linera-proxy.
From the root of Linera repository, this can be minimally started as follows:

```bash
cargo run --package linera-service --bin linera-exporter run --features metrics -- --config-path <CONFIG_PATH> --storage <STORAGE_CONFIG>
```

Care should be taken when passing the arguments <STORAGE_CONFIG> and <GENESIS_CONFIG_PATH>. They should be same that are already used for that validator to run its proxy and the chain workers.

Block exporter requires a startup configuration file. A minimal example of this file is provided with the required fields. Many other optional fields can also be used and their references can be taken from the `config.rs` in the `linera-client` crate.

```bash
# BlockExporterConfig

id = 42

[service_config]
host = "127.0.0.1"
port = 12000

```

This will start a block exporter without any export destinations. So now only sorting of the blocks will take place and destinations in future can utilize that for their exports.

A new destination can be added with a simple modification in the configuration file.

```bash
[[destination_config.destinations]]
endpoint = "export1.example.com"
port = 443
tls = "Tls"

[[destination_config.destinations]]
endpoint = "export2.internal"
port = 8080
tls = "ClearText"

```

Finally, the `URI` endpoint of the block exporter service must be provided in the configuration of the chain workers for it to receive notification about the new blocks.
In the configuration file of the chain worker:

```bash
[[block_exporters]]
host = "exporter"
port = 12000

```
