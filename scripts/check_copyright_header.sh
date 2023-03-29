#!/bin/bash

FILE="$1"

if [ -z "$FILE" ]; then
    echo "Usage: $0 FILE" >&2
    exit 1
fi

sed -n -e '
    # Enter loop on first line
    {
        : loop;

            # If Zefchain copyright is found, hold it in the auxiliary buffer
            /^\/\/ Copyright (c) Zefchain Labs, Inc\.$/ { h; b next };

            # Ignore any other copyright lines
            /^\/\/ Copyright (c) / { b next };

            # Last line should be the SPDX license identifier
            /^\/\/ SPDX-License-Identifier: Apache-2.0/ { b end };

            # Unexpected line reached, quit with error code
            q1;
        
        # Read next line and repeat
        : next;
        n;
        b loop;

        # Check the end of the header
        : end;

            # Expect an empty line to separate the header from the body of the file
            n;
            /^$/! q1;

            # Grab auxiliary buffer and check for Zefchain copyright line
            g;
            /^\/\/ Copyright (c) Zefchain Labs, Inc\.$/! q1;

        # Success
        q0;
    }
' "$FILE"

if [ "$?" != 0 ]; then
    echo "Incorrect copyright header: $FILE"
    exit 1
fi
