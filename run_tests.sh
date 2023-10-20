#!/bin/bash

set -euo pipefail

REDIS_VERSION='7.2.2'

SCRIPT_PATH=$(readlink -f "$0")
ROOT_DIR=$(dirname "$SCRIPT_PATH")
TEST_DIR="$ROOT_DIR/tests"
REDIS_DIR="$TEST_DIR/redis-$REDIS_VERSION"

if [ ! -d "$REDIS_DIR" ]; then
    curl -L "https://github.com/redis/redis/archive/$REDIS_VERSION.tar.gz" | tar xz -C "$TEST_DIR"
fi

cd "$REDIS_DIR"

./runtest \
    --host 127.0.0.1 \
    --singledb \
    --ignore-encoding \
    --tags '-needs:repl -needs:debug -needs:pfdebug -needs:config-maxmemory -needs:reset -needs:save -resp3' \
    --skipfile "$TEST_DIR/skipfile" \
    --single unit/protocol \
    --single unit/keyspace \
    --single unit/type/string \
    --single unit/type/incr \
    --single unit/type/list \
    --single unit/type/list-2 \
    --single unit/type/list-3\
    --single unit/type/set \
    --single unit/type/hash \
    --single unit/other \
    --single unit/multi \
    --single unit/bitops \
    --single unit/hyperloglog \
	"$@"
