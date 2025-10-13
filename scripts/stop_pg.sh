#!/bin/bash
set -e

# -----------------------------
# stop server if running
# -----------------------------
if [ -f "$PG_DATA/postmaster.pid" ]; then
    pid=$(head -n1 "$PG_DATA/postmaster.pid")
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "stop_pg stopping server (pid $pid)"
        pg_ctl -D "$PG_DATA" stop -m fast
    else
        echo "stop_pg stale postmaster.pid found, removing"
        rm "$PG_DATA/postmaster.pid"
    fi
else
    echo "stop_pg no server running"
fi
