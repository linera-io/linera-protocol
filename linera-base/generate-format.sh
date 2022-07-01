#!/bin/bash
cd "`dirname $0`"/..
cargo run --example generate-format > linera-base/tests/staged/formats.yaml
