#!/bin/bash

# Check that the correctness-specification modules contain only marker traits.
#
# CI skips the test suite for pull requests that touch nothing but these paths (see the `code`
# filter in .github/workflows/rust.yml). That is safe precisely because the files hold no
# executable code: every item is a public trait with no members, no implementors and no call
# sites, carrying the specification in its doc comments. Compilation, rustdoc link checking and
# formatting still run, and they are the only things such an edit can break.
#
# If real code ever lands under one of these paths, that reasoning stops holding and the tests
# would silently stop covering it. This check turns that into a build failure instead.

set -euo pipefail

# Make sure we're at the root of the repo.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

SPEC_PATHS=(
    linera-spec/src
    linera-chain/src/proof
    linera-chain/src/manager/proof
    linera-chain/src/data_types/proof
    linera-chain/src/justification/proof.rs
    linera-core/src/proof
)

FOUND=0

for path in "${SPEC_PATHS[@]}"; do
    [ -e "$path" ] || continue
    # Executable items: an `impl` block, a function, a `const`/`static`, or a `struct`/`enum`.
    # Doc comments and `//` comments are stripped first so that prose mentioning these words
    # does not trip the check.
    while IFS= read -r hit; do
        echo "unexpected code in a specification module: $hit"
        FOUND=1
    done < <(
        grep -rn --include='*.rs' -E '^[[:space:]]*(pub[[:space:]]+)?(impl|fn|const|static|struct|enum|macro_rules!)[[:space:]!]' "$path" \
            | grep -v -E ':[[:space:]]*(///|//!|//)' \
            || true
    )
done

if [ "$FOUND" -ne 0 ]; then
    cat <<'EOF'

The specification modules are expected to contain only marker traits, `use` statements and
module declarations. CI relies on this: a pull request touching only these paths skips the
test suite, on the grounds that nothing there can change runtime behaviour.

Either move the code elsewhere, or remove the affected path from the `code` filter in
.github/workflows/rust.yml (and the matching exclusions in the other workflows) so that the
tests run for it again.
EOF
    exit 1
fi

echo "specification modules contain only marker traits"
