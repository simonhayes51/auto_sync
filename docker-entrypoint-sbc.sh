#!/usr/bin/env bash
set -euo pipefail

# Started Xvfb directly instead of using `xvfb-run -a`: a real Railway
# deploy sat at "Starting Container" indefinitely with zero further
# output - not even this worker's own first log line, which fires before
# anything else in futbin_sbc_sync.py - meaning Python never even started.
# xvfb-run -a auto-picks a display number by scanning /tmp/.X*-lock files,
# a mechanism known to hang on restricted/ephemeral container runtimes.
# Starting Xvfb ourselves on the fixed display already set via
# ENV DISPLAY=:99, with an explicit (bounded) readiness check instead of
# an unbounded implicit one, avoids that failure mode entirely - and
# fails loudly within ~2s instead of hanging forever if Xvfb itself is
# the thing that's broken in this environment.
DISPLAY_NUM="${DISPLAY#:}"
SOCKET="/tmp/.X11-unix/X${DISPLAY_NUM}"

echo "Starting Xvfb on display ${DISPLAY:-:99}..."
Xvfb "${DISPLAY:-:99}" -screen 0 1920x1080x24 -nolisten tcp &
XVFB_PID=$!

ready=0
for _ in $(seq 1 20); do
    if [ -e "$SOCKET" ]; then
        ready=1
        break
    fi
    if ! kill -0 "$XVFB_PID" 2>/dev/null; then
        echo "Xvfb process exited before creating ${SOCKET} - see any Xvfb output above for why"
        exit 1
    fi
    sleep 0.1
done

if [ "$ready" = "1" ]; then
    echo "Xvfb ready (socket ${SOCKET} present)"
else
    echo "WARNING: Xvfb socket ${SOCKET} not seen after 2s - continuing anyway; the worker's own browser-launch timeout will fail loudly if this is fatal"
fi

exec python futbin_sbc_sync.py
