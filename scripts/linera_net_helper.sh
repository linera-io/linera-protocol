# Runs a command such as `linera net up` in the background.
# - Records stdout
# - Waits for the background process to print READY! on stderr
# - Then executes the bash command recorded from stdout
# - Returns without killing the process
function linera_spawn_and_read_wallet_variables() {
    DIR=$(mktemp -d "${TMPDIR:-.}tmp-XXXXX") || exit 1
    OUT="$DIR/out"
    ERR="$DIR/err"
    mkfifo "$ERR" || exit 1

    trap 'jobs -p | xargs -r kill && rm -rf "$DIR"' EXIT

    (
        # Ignoring SIGPIPE to keep `tee` alive after `sed` exits below, closing $ERR.
        trap '' PIPE
        "$@" 2> >(tee "$ERR" 2>/dev/null) 1>"$OUT" &
    )

    sed -n '/^READY!/q' <"$ERR" || exit 1

    source "$OUT"
}
