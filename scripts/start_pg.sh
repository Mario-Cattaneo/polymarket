#!/bin/bash
set -e

# -----------------------------
# flags
# -----------------------------
force=false
reset=false

for arg in "$@"; do
    case $arg in
        -force) force=true ;;
        -reset) reset=true ;;
        *) echo "start_pg unknown option: $arg"; exit 1 ;;
    esac
done

# -----------------------------
# ensure directories exist
# -----------------------------
mkdir -p "$PG_DATA" "$PG_SOCKET"
chmod 700 "$PG_DATA"
chmod 777 "$PG_SOCKET"

# -----------------------------
# check for running server
# -----------------------------
if [ -f "$PG_DATA/postmaster.pid" ]; then
    pid=$(head -n1 "$PG_DATA/postmaster.pid")
    if ps -p "$pid" > /dev/null 2>&1; then
        if [ "$force" = true ]; then
            echo "start_pg stopping existing server (pid $pid)"
            pg_ctl -D "$PG_DATA" stop -m fast
        else
            echo "start_pg server is already running (pid $pid). use -force to stop it"
            exit 1
        fi
    else
        echo "start_pg stale postmaster.pid found, removing"
        rm "$PG_DATA/postmaster.pid"
    fi
fi

# -----------------------------
# reset database if requested
# -----------------------------
if [ "$reset" = true ]; then
    echo "start_pg resetting database cluster"
    rm -rf "$PG_DATA"/*
    initdb -D "$PG_DATA"
fi

# -----------------------------
# initialize cluster if missing
# -----------------------------
if [ ! -f "$PG_DATA/PG_VERSION" ]; then
    echo "start_pg initializing new database cluster"
    initdb -D "$PG_DATA"
fi

# -----------------------------
# start server persistently
# -----------------------------
echo "start_pg starting postgres server"
pg_ctl -D "$PG_DATA" -o "-k $PG_SOCKET -h ''" -l "$PG_DATA/logfile" start

# wait until server is ready
echo "start_pg waiting for server to accept connections"
until psql -h "$PG_SOCKET" -d postgres -U "$POLY_DB_CLI" -c '\q' > /dev/null 2>&1; do
    sleep 1
done
echo "start_pg postgres running at $PG_SOCKET"

# -----------------------------
# create client role if missing
# -----------------------------
if ! psql -h "$PG_SOCKET" -d postgres -U "$POLY_DB_CLI" -tc \
    "SELECT 1 FROM pg_roles WHERE rolname='$POLY_DB_CLI';" | grep -q 1; then
    echo "start_pg creating role $POLY_DB_CLI"
    psql -h "$PG_SOCKET" -d postgres -U "$POLY_DB_CLI" -c \
        "CREATE ROLE $POLY_DB_CLI LOGIN PASSWORD '$POLY_DB_CLI_PASS';"
fi

# -----------------------------
# create database if missing
# -----------------------------
if ! psql -h "$PG_SOCKET" -d postgres -U "$POLY_DB_CLI" -tc \
    "SELECT 1 FROM pg_database WHERE datname='$POLY_DB';" | grep -q 1; then
    echo "start_pg creating database $POLY_DB"
    createdb -h "$PG_SOCKET" -U "$POLY_DB_CLI" -O "$POLY_DB_CLI" "$POLY_DB"
fi

# -----------------------------
# temporary test table
# -----------------------------
psql -h "$PG_SOCKET" -d "$POLY_DB" -U "$POLY_DB_CLI" -c \
    "CREATE TABLE IF NOT EXISTS records_test(id SERIAL PRIMARY KEY, value TEXT);"
psql -h "$PG_SOCKET" -d "$POLY_DB" -U "$POLY_DB_CLI" -c \
    "DROP TABLE IF EXISTS records_test"

echo "start_pg postgres setup complete"
echo "start_pg socket: $PG_SOCKET"
echo "start_pg database: $POLY_DB"
echo "start_pg user: $POLY_DB_CLI / $POLY_DB_CLI_PASS"
echo "start_pg log: $PG_DATA/logfile"
