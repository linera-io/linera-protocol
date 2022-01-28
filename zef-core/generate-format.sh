#!/bin/bash
cd "`dirname $0`"/..
cargo run --example generate-format > zef-core/tests/staged/formats.yaml
