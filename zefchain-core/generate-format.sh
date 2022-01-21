#!/bin/bash
cd "`dirname $0`"/..
cargo run --example generate-format > zefchain-core/tests/staged/formats.yaml
