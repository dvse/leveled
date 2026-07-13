#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
SOURCE=${SOURCE:-/tmp/recoll_bench/corpus/samples/hpmor_complete.txt}
WORK_DIR=${WORK_DIR:-/tmp/leveled_compression_bench}
WORKLOAD=${WORKLOAD:-$WORK_DIR/workload.bin}
STORE=${STORE:-$WORK_DIR/store}
RESULTS=${RESULTS:-$WORK_DIR/results.tsv}
EBIN=$WORK_DIR/ebin

mkdir -p "$EBIN"
cd "$ROOT_DIR"
./rebar3 compile
erlc -Werror -I include -pa _build/default/lib/leveled/ebin \
    -o "$EBIN" bench/leveled_compression_bench.erl

if [[ ! -s "$WORKLOAD" ]]; then
    erl -noshell -pa "$EBIN" -pa _build/default/lib/*/ebin \
        -eval "leveled_compression_bench:main([\"prepare\", \"$SOURCE\", \"$WORKLOAD\"]), halt()."
fi

: > "$RESULTS"

run_case() {
    local method=$1
    local receipt=$2
    local ledger=$3
    local load=$4
    erl -noshell -pa "$EBIN" -pa _build/default/lib/*/ebin \
        -eval "leveled_compression_bench:main([\"run\", \"$WORKLOAD\", \"$STORE\", \"$method\", \"$receipt\", \"$ledger\", \"$load\"]), halt()." \
        | tee -a "$RESULTS"
}

for load in text fts; do
    for method in none lz4 native zstd; do
        run_case "$method" true "$method" "$load"
    done
done

for receipt in true false; do
    for method in none lz4 native zstd; do
        run_case "$method" "$receipt" "$method" mixed
    done
done

# Hold journal compression at lz4 to isolate the ledger press method.
for receipt in true false; do
    for ledger in none native zstd; do
        run_case lz4 "$receipt" "$ledger" mixed
    done
done
