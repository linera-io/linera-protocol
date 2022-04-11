#!/bin/bash
cd "`dirname $0`"/..
cargo run --example generate-format > zef-base/tests/staged/formats.yaml
