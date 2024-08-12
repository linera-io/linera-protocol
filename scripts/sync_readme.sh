#!/bin/bash

# Must run this from the root directory of the example that the README is being modified

README_FILE="README.md"
LIB_FILE="src/lib.rs"

# Extract the relevant portion from the README, excluding the cargo-rdme markers
README_CONTENT=$(sed -n '/<!-- cargo-rdme start -->/,/<!-- cargo-rdme end -->/p' $README_FILE | sed '1d;$d')

# Remove leading and trailing empty lines while preserving internal empty lines
ESCAPED_CONTENT=$(echo "$README_CONTENT" | sed '/./,$!d' | sed -e :a -e '/^\n*$/{$d;N;};/\n$/ba')

# Escape dollar signs and backticks to prevent issues in the lib.rs output
ESCAPED_CONTENT=$(echo "$ESCAPED_CONTENT" | sed 's/\\/\\\\/g; s/\$/\\\$/g; s/\`/\\\`/g')

# Replace the corresponding doc comment in lib.rs
perl -0777 -i -pe "s|/\*!.*?\*/|/\*!\n$ESCAPED_CONTENT\n*/|s" $LIB_FILE
