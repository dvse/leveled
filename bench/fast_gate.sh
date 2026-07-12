#!/bin/bash
# Fast FTS iteration loop (goal: leveled within 3x of SQLite, <30s/cycle).
# Usage: bench/fast_gate.sh [rank]   (rank: none|bm25, default none)
set -e
export PATH="$HOME/.local/share/mise/shims:$PATH"
RANK=${1:-none}
W=/tmp/leveled_fts_bench
FAST=${FTS_FAST_DIR:-$W/fast}
Q=bench/fts_gate_queries.txt
SQL_R=$W/results/fast/sqlite-s10m-$RANK.tsv   # shared baseline (read-only reuse)
LEV_R=$FAST/leveled-s10m-$RANK.tsv
mkdir -p $W/results/fast $FAST $FAST/ebin
# SQLite side: run once, cache (delete the TSV to re-measure)
if [ ! -f "$SQL_R" ]; then
  $W/gate/sqlite_fts_bench --tsv $W/docs-s10m.tsv --db $W/fast-sqlite.db \
    --queries $Q --result $SQL_R --rank $RANK --runs 9 --warmup 3 2>&1 | tail -1
fi
# Leveled side: recompile bench + library, fresh store, no settle sleep
erlc -o $FAST/ebin -I include -pa _build/default/lib/leveled/ebin bench/leveled_fts_bench.erl 2>/dev/null
rm -rf $FAST/fast-store
erl -noshell -pa $FAST/ebin -pa _build/default/lib/leveled/ebin -pa _build/default/lib/lz4/ebin -pa _build/default/lib/zstd/ebin -eval "
Args = [\"--tsv\",\"$W/docs-s10m.tsv\",\"--root\",\"$FAST/fast-store\",
        \"--queries\",\"$Q\",\"--result\",\"$LEV_R\",
        \"--rank\",\"$RANK\",\"--runs\",\"9\",\"--warmup\",\"3\",
        \"--amortized\",\"--compact\",\"--settle-ms\",\"0\"],
R = leveled_fts_bench:main(Args), io:format(\"leveled=~p~n\",[R]), init:stop()." 
python3 - "$LEV_R" "$SQL_R" <<'PYEOF'
import csv, statistics, sys
def med(path):
    out = {}
    for r in csv.reader(open(path), delimiter="\t"):
        if len(r) > 6 and r[1] == "query_us" and r[6]:
            runs = [int(x) for x in r[6].split(",") if x]
            out[r[3][:34]] = statistics.median(runs)
    return out
lm, sm = med(sys.argv[1]), med(sys.argv[2])
print(f"{'query':36} {'leveled_us':>10} {'sqlite_us':>10} {'ratio':>8}")
worst = ("", 0)
for q, v in sorted(lm.items()):
    s = sm.get(q)
    r = v / s if s else float("nan")
    if s and r > worst[1]: worst = (q, r)
    flag = " <== BREACH" if s and r > 3.0 else ""
    print(f"{q:36} {v:10.0f} {s or 0:10.0f} {r:8.2f}{flag}")
print(f"\nworst: {worst[0]} at {worst[1]:.2f}x   (goal: 3.0x all shapes)")
PYEOF
