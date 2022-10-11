#!/bin/bash
cd "`dirname $0`"/..
cargo run --example generate-format > linera-rpc/tests/staged/formats.yaml
