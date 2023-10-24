# Runs a command such as `linera net up` in the background.
# - Records stdout
# - Waits for the background process to print READY! on stderr
# - Then executes the bash command recorded from stdout
# - Returns without killing the process
function linera_spawn_and_read_wallet_variables() {
    LINERA_TMP_DIR=$(mktemp -d "${TMPDIR:-.}tmp-XXXXX") || exit 1

    # Clean up the temporary directory in case we exit.
    trap 'rm -rf "$LINERA_TMP_DIR"' EXIT

    LINERA_TMP_PID="$LINERA_TMP_DIR/pid"
    LINERA_TMP_OUT="$LINERA_TMP_DIR/out"
    LINERA_TMP_ERR="$LINERA_TMP_DIR/err"
    mkfifo "$LINERA_TMP_ERR" || exit 1

    (
        # Ignoring SIGPIPE to keep `tee` alive after `sed` exits below, closing
        # LINERA_TMP_ERR.
        trap '' PIPE

        # Start the main process.
        "$@" 2> >(tee "$LINERA_TMP_ERR" 2>/dev/null) 1>"$LINERA_TMP_OUT" &

        # Remember the PID of the service for later.
        jobs -p > "$LINERA_TMP_PID"
    )
    LINERA_NET_PID=$(cat "$LINERA_TMP_PID")

    # When the shell exits, we will clean up the top-level jobs (if any), the temporary
    # directory, and the main process. Handling future top-level jobs here is useful
    # because calling `trap` again will be replace this handler.
    trap 'jobs -p | xargs -r kill && rm -rf "$LINERA_TMP_DIR" && kill "$LINERA_NET_PID"' EXIT

    # Read from LINERA_TMP_ERR until the string "READY!" is found.
    sed -n '/^READY!/q' <"$LINERA_TMP_ERR" || exit 1

    # Source the Bash commands output by the server.
    source "$LINERA_TMP_OUT"
}
