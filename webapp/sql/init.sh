#!/bin/bash

set -eu
cd -- "$(dirname -- "$0")"

if test -f /home/isucon/env.sh; then
	. /home/isucon/env.sh
fi

ISUREAL_DB_HOST=${ISUREAL_DB_HOST:-127.0.0.1}
ISUREAL_DB_PORT=${ISUREAL_DB_PORT:-5432}
ISUREAL_DB_USER=${ISUREAL_DB_USER:-isureal}
ISUREAL_DB_PASSWORD=${ISUREAL_DB_PASSWORD:-isureal}
ISUREAL_DB_NAME=${ISUREAL_DB_NAME:-isureal}

PGPASSWORD="$ISUREAL_DB_PASSWORD" psql \
	--no-psqlrc \
	--no-password \
	--host="$ISUREAL_DB_HOST" \
	--port="$ISUREAL_DB_PORT" \
	--username="$ISUREAL_DB_USER" \
	--dbname="$ISUREAL_DB_NAME" \
	--set=ON_ERROR_STOP=1 \
	--file=reset.sql
