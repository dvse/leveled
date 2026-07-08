#!/usr/bin/env python3
"""Scale-ladder driver for the Leveled-vs-SQLite-FTS5 equivalence + 5x gate.

Builds and measures both engines at a ladder of text-size slices of a shared
Wikipedia TSV, for both rank modes (rank=none, rank=bm25), writing result TSVs
named ``{sqlite,leveled}-{scale}-{rank}.tsv`` that ``check_gate.py`` consumes.

Methodology (matches the repo's single-run discipline, applied per rung):
  * deterministic slices: the first N documents of the shared TSV whose
    cumulative decoded text reaches the rung target (doc keys are ordinal, so a
    prefix slice is deterministic and identical across engines/ranks);
  * each (scale, rank, engine) run is an independent FRESH build -- SQLite DB /
    Leveled store are created from scratch, closed, and reopened before timed
    queries; no store is reused across ranks (so both rank modes pay a genuine
    cold build + reopen);
  * warmup passes then measured runs; check_gate.py takes medians;
  * per rung, per engine, the store/db is deleted after both ranks unless
    FTS_KEEP=1, so full-scale indexes never coexist beyond one rung.

Prereqs: ``fts_bench.py prepare`` has produced the shared TSV; the SQLite
amalgamation (sqlite3.c/.h) and CLI exist under --sqlite-source.
"""
import argparse
import os
import pathlib
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import fts_bench as fb  # noqa: E402


def decoded_len(b64_field: bytes) -> int:
    n = len(b64_field)
    if n == 0:
        return 0
    pad = b64_field.count(b"=")
    return (n // 4) * 3 - pad


def slice_tsv(src: pathlib.Path, dst: pathlib.Path, target_text_bytes: int):
    """Write the first K docs of src whose cumulative decoded (title+body) text
    reaches target_text_bytes. Returns (docs, text_bytes)."""
    docs = 0
    text_bytes = 0
    with src.open("rb") as inp, dst.open("wb") as out:
        for line in inp:
            stripped = line.rstrip(b"\r\n")
            if not stripped:
                continue
            parts = stripped.split(b"\t")
            if len(parts) != 3:
                raise ValueError("bad TSV line during slicing")
            text_bytes += decoded_len(parts[1]) + decoded_len(parts[2])
            out.write(line if line.endswith(b"\n") else line + b"\n")
            docs += 1
            if text_bytes >= target_text_bytes:
                break
    return docs, text_bytes


def run_sqlite(helper, tsv, db, queries, result, rank, limit, runs, warmup, batch,
               sqlite_source, log):
    cmd = [
        str(helper), "--tsv", str(tsv), "--db", str(db), "--queries", str(queries),
        "--result", str(result), "--batch", str(batch), "--limit", str(limit),
        "--runs", str(runs), "--warmup", str(warmup), "--rank", rank,
        "--source-checkout", str(sqlite_source),
        "--source-version", fb.sqlite_source_version(sqlite_source),
        "--cli-version", fb.sqlite_cli_version(sqlite_source / "sqlite3"),
        "--force",
    ]
    with open(log, "ab") as lg:
        lg.write(f"\n$ {' '.join(cmd)}\n".encode())
        lg.flush()
        return subprocess.call(cmd, stdout=lg, stderr=lg)


def run_leveled(ebin, tsv, root, queries, result, rank, limit, runs, warmup, batch,
                log, regime="warm"):
    eval_src = (
        "Args=init:get_plain_arguments(), "
        "case leveled_fts_bench:main(Args) of "
        "ok -> halt(0); "
        "{error,R} -> io:format(standard_error, \"~p~n\", [R]), halt(1); "
        "Other -> io:format(standard_error, \"~p~n\", [Other]), halt(1) end."
    )
    cmd = [
        "erl", "-noshell", "-pa", str(ebin), "-eval", eval_src, "-extra",
        "--tsv", str(tsv), "--root", str(root), "--queries", str(queries),
        "--result", str(result), "--batch", str(batch),
        "--compression-method", "native", "--ledger-compression", "as_store",
        "--limit", str(limit), "--runs", str(runs), "--warmup", str(warmup),
        "--rank", rank,
    ]
    if regime == "uncached":
        cmd.append("--uncached")
    elif regime == "amortized":
        cmd.append("--amortized")
    with open(log, "ab") as lg:
        lg.write(f"\n$ {' '.join(cmd)}\n".encode())
        lg.flush()
        return subprocess.call(cmd, stdout=lg, stderr=lg, cwd=str(fb.repo_root()))


def parse_rungs(specs):
    rungs = []
    for spec in specs:
        parts = spec.split(":")
        if len(parts) != 4:
            raise ValueError(f"bad rung {spec!r}; expected label:text_bytes:runs:warmup")
        label, tb, runs, warmup = parts
        rungs.append({"label": label, "text_bytes": int(tb),
                      "runs": int(runs), "warmup": int(warmup)})
    return rungs


DEFAULT_RUNGS = [
    "s100m:104857600:25:5",
    "s1g:1073741824:15:3",
    "full:0:7:2",
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
    ap.add_argument("--queries", default=str(pathlib.Path(__file__).with_name("fts_gate_queries.txt")))
    ap.add_argument("--work-dir", default="/tmp/leveled_fts_bench/gate")
    ap.add_argument("--results-dir", default="/tmp/leveled_fts_bench/results/gate")
    ap.add_argument("--sqlite-source", default="/Users/dvse/repos/sqlite")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--insert-batch", type=int, default=200)
    ap.add_argument("--ranks", default="none,bm25")
    ap.add_argument("--rung", action="append", default=[],
                    help="label:text_bytes:runs:warmup (repeatable; 0 text_bytes = full corpus)")
    ap.add_argument("--min-free-gb", type=float, default=40.0)
    ap.add_argument("--uncached", action="store_true",
                    help="alias for --regime uncached")
    ap.add_argument("--regime", choices=["warm", "uncached", "amortized"], default=None,
                    help="leveled measurement regime: warm (result-cache hits), "
                         "uncached (write-seq bump per timed run; write-per-query worst "
                         "case), amortized (bump once + untimed absorber per run; "
                         "steady-state novel-query cost)")
    ap.add_argument("--reuse-sqlite", action="store_true",
                    help="skip the SQLite run when its result TSV already exists")
    args = ap.parse_args()

    tsv = pathlib.Path(args.tsv).resolve()
    if not tsv.exists():
        print(f"missing TSV {tsv}; run fts_bench.py prepare first", file=sys.stderr)
        return 2
    work = pathlib.Path(args.work_dir)
    results = pathlib.Path(args.results_dir)
    work.mkdir(parents=True, exist_ok=True)
    results.mkdir(parents=True, exist_ok=True)
    logs = work / "logs"
    logs.mkdir(exist_ok=True)
    sqlite_source = pathlib.Path(args.sqlite_source)
    ranks = [r for r in args.ranks.split(",") if r]
    rungs = parse_rungs(args.rung) if args.rung else parse_rungs(DEFAULT_RUNGS)
    keep = os.environ.get("FTS_KEEP") == "1"

    print(f"== ladder: tsv={tsv} rungs={[r['label'] for r in rungs]} ranks={ranks} "
          f"limit={args.limit} ==", flush=True)

    # Compile helpers once.
    print("compiling sqlite source runner + leveled ebin ...", flush=True)
    helper = work / "sqlite_fts_bench"
    fb.compile_sqlite_source_runner(sqlite_source, helper)
    ebin = work / "ebin"
    fb.compile_leveled(fb.repo_root(), ebin)
    print("compiled.", flush=True)

    for rung in rungs:
        label = rung["label"]
        target = rung["text_bytes"]
        print(f"\n===== rung {label} (target_text_bytes={target or 'FULL'}) =====", flush=True)
        free_gb = shutil.disk_usage(str(work)).free / 1e9
        print(f"disk free: {free_gb:.1f} GB", flush=True)
        if free_gb < args.min_free_gb:
            print(f"SKIP {label}: free {free_gb:.1f} GB < min {args.min_free_gb} GB", flush=True)
            continue

        if target and target > 0:
            slice_path = work / f"docs-{label}.tsv"
            t0 = time.time()
            docs, tb = slice_tsv(tsv, slice_path, target)
            print(f"sliced {label}: {docs} docs, {tb/1e6:.1f} MB text "
                  f"({time.time()-t0:.0f}s)", flush=True)
            rung_tsv = slice_path
        else:
            rung_tsv = tsv
            slice_path = None
            print(f"rung {label}: using full corpus TSV", flush=True)

        regime = args.regime or ("uncached" if args.uncached else "warm")
        for rank in ranks:
            db = work / f"sqlite-{label}-{rank}.db"
            sres = results / f"sqlite-{label}-{rank}.tsv"
            slog = logs / f"sqlite-{label}-{rank}.log"
            if args.reuse_sqlite and sres.exists():
                print(f"  sqlite {label} {rank}: reused {sres.name}", flush=True)
            else:
                t0 = time.time()
                rc = run_sqlite(helper, rung_tsv, db, args.queries, sres, rank, args.limit,
                                rung["runs"], rung["warmup"], args.insert_batch, sqlite_source, slog)
                print(f"  sqlite {label} {rank}: rc={rc} ({time.time()-t0:.0f}s) -> {sres.name}",
                      flush=True)
                if rc != 0:
                    print(f"  !! sqlite failed; see {slog}", flush=True)

            root = work / f"leveled-{label}-{rank}-{regime}"
            lres = results / f"leveled-{label}-{rank}-{regime}.tsv"
            llog = logs / f"leveled-{label}-{rank}-{regime}.log"
            t0 = time.time()
            rc = run_leveled(ebin, rung_tsv, root, args.queries, lres, rank, args.limit,
                             rung["runs"], rung["warmup"], args.batch, llog, regime=regime)
            print(f"  leveled {label} {rank} [{regime}]: rc={rc} ({time.time()-t0:.0f}s) -> {lres.name}",
                  flush=True)
            if rc != 0:
                print(f"  !! leveled failed; see {llog}", flush=True)

            if not keep:
                if db.exists():
                    db.unlink()
                shutil.rmtree(root, ignore_errors=True)

        if slice_path is not None and not keep:
            slice_path.unlink(missing_ok=True)

    print("\n== ladder complete ==", flush=True)
    print(f"results in {results}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
