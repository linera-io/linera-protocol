#!/bin/bash

ROOT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )

if cat <<EOF > $ROOT_DIR/.git/hooks/pre-push
#!/bin/sh
#
# This pre-push hook runs a cargo-clippy and cargo +nightly fmt before pushing code to remote.
# This is to prevent commits being pushed which will fail CI.

cargo clippy --all-targets || { echo "Error: clippy did not pass - aborting push. Please run 'cargo clippy --all-targets'." ; exit 1 ; }

cargo +nightly fmt -- --check --config unstable_features=true --config imports_granularity=Crate || { echo "Error: format check failed - aborting push. Please run 'cargo +nightly fmt'." ; exit 1 ; }

cargo +nightly udeps --all-targets || { echo "Error: dependency check failed - aborting push." ; exit 1 ; }
EOF
then
	chmod +x $ROOT_DIR/.git/hooks/pre-push
else
	echo "Failed to write pre-push hook"
	exit 1
fi
