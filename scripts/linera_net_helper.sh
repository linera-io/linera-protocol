# Runs a command such as `linera net up` in the background.
# - Export a temporary directory `$LINERA_TMP_DIR`.
# - Records stdout
# - Waits for the background process to print READY! on stderr
# - Then executes the bash command recorded from stdout
# - Returns without killing the process
function linera_spawn_and_read_wallet_variables() {
    LINERA_TMP_DIR=$(mktemp -d) || exit 1

    # When the shell exits, we will clean up the top-level jobs (if any), the temporary
    # directory, and the main process. Handling future top-level jobs here is useful
    # because calling `trap` again will be replace this handler.
    trap 'jobs -p | xargs -r kill; rm -rf "$LINERA_TMP_DIR"' EXIT

    LINERA_TMP_OUT="$LINERA_TMP_DIR/out"
    LINERA_TMP_ERR="$LINERA_TMP_DIR/err"
    mkfifo "$LINERA_TMP_ERR" || exit 1

    "$@" >"$LINERA_TMP_OUT" 2>"$LINERA_TMP_ERR" &

    # Read from LINERA_TMP_ERR until the string "READY!" is found.
    sed '/^READY!/q' <"$LINERA_TMP_ERR" || exit 1

    # Output the commands for reading.
    cat "$LINERA_TMP_OUT"

    # Source the Bash commands output by the server.
    source "$LINERA_TMP_OUT"

    # Continue to output the command's stderr onto stdout.
    cat "$LINERA_TMP_ERR" &
}

# Runs a command such as `linera net up` in the background.
# - Export a temporary directory `$LINERA_TMP_DIR`.
# - Waits for the background process to print READY! on stderr
# - Returns without killing the process
function linera_spawn() {
    LINERA_TMP_DIR=$(mktemp -d) || exit 1

    trap 'jobs -p | xargs -r kill; rm -rf "$LINERA_TMP_DIR"' EXIT

    LINERA_TMP_OUT="$LINERA_TMP_DIR/out"
    LINERA_TMP_ERR="$LINERA_TMP_DIR/err"
    mkfifo "$LINERA_TMP_ERR" || exit 1

    "$@" 2>"$LINERA_TMP_ERR" &

    # Read from LINERA_TMP_ERR until the string "READY!" is found.
    sed '/^READY!/q' <"$LINERA_TMP_ERR" || exit 1

    # Continue to output the command's stderr onto stdout.
    cat "$LINERA_TMP_ERR" &
}
