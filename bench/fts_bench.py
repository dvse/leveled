#!/usr/bin/env python3
import argparse
import base64
import bz2
import csv
import hashlib
import json
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from statistics import median


DEFAULT_QUERIES = [
    "london",
    "history",
    '"new york"',
    "title:history",
    "science AND research",
    "war OR peace",
    "comput*",
    "NEAR(history culture, 10)",
]

DEFAULT_INDEX_QUERIES = [
    "mod100:0",
    "mod100:1",
    "mod10:0",
    "mod10:7",
    "mod2:0",
    "mod2:1",
]

DEFAULT_OP_QUERIES = [
    "equivbaseline",
    "equivupdate",
    "equivupdateold",
    "equivdeleteold",
    "equivreinsert",
    "equivreinsertold",
    "equivlateupdate",
    "equivoldpre*",
    "title:equivtitleupdate",
    "title:equivtitlebaseline",
    "title:equivtitlereinsert",
    "body:equivbodyupdate",
    "-title:equivbodyupdate",
    "-{title}:equivbodyupdate",
    "-body:equivtitleupdate",
    "-{title body}:equivbaseline",
    "equivprefix*",
    "equivupdate AND equivbeta",
    "equivbaseline OR equivreinsert",
    "equivprefix* NOT equivbodyupdate",
    '"new york"',
    "NEAR(equivnearone equivneartwo, 5)",
    "^equivupdate",
    '"equivnearone ga"*',
    "equivnearone + gap",
    'NEAR("equivnearone ga"* equivneartwo, 5)',
]

FTS_REPRESENTATION_CONTRACT = "secondary_index_payload_postings"
RANK_NONE_SNAPSHOT_CONTRACT = "rank_none_payload_key_order"

SQLITE_INDEX_DDL = (
    "CREATE TABLE docs(key TEXT PRIMARY KEY, title BLOB NOT NULL, body BLOB NOT NULL, "
    "mod100 INTEGER NOT NULL, mod10 INTEGER NOT NULL, mod2 INTEGER NOT NULL); "
    "CREATE INDEX docs_mod100_key_idx ON docs(mod100, key); "
    "CREATE INDEX docs_mod10_key_idx ON docs(mod10, key); "
    "CREATE INDEX docs_mod2_key_idx ON docs(mod2, key)"
)
SQLITE_INDEX_CONTRACT = "mod100,mod10,mod2 from document ordinal"
LEVELED_INDEX_CONTRACT = "mod100_int,mod10_int,mod2_int from document ordinal"
SQLITE_FTS_DDL = (
    "CREATE VIRTUAL TABLE docs USING fts5(key UNINDEXED, title, body, "
    "tokenize='unicode61 remove_diacritics 2', prefix='5 11')"
)
HARNESS_SCHEMA_VERSION = 3

RESULT_HEADER = ["engine", "metric", "value", "query", "count", "total_count", "runs_us", "error", "keys"]

try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare a compressed corpus and benchmark Leveled FTS against SQLite FTS5."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("prepare")
    add_prepare_args(p)
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser("sqlite")
    add_sqlite_args(p, include_tsv=True)
    p.set_defaults(func=cmd_sqlite)

    p = sub.add_parser("leveled")
    add_leveled_args(p, include_common=True)
    p.set_defaults(func=cmd_leveled)

    p = sub.add_parser("compare")
    add_compare_args(p)
    p.set_defaults(func=cmd_compare)

    p = sub.add_parser("generate-queries")
    add_generate_queries_args(p)
    p.set_defaults(func=cmd_generate_queries)

    p = sub.add_parser("sqlite-index")
    add_sqlite_args(
        p,
        include_tsv=True,
        queries_default="/tmp/leveled_fts_bench/index_queries.txt",
        sqlite_db_default="/tmp/leveled_fts_bench/sqlite/index.sqlite3",
        sqlite_result_default="/tmp/leveled_fts_bench/results/sqlite-index.tsv",
        sqlite_helper_default="/tmp/leveled_fts_bench/sqlite_index_bench",
    )
    p.set_defaults(func=cmd_sqlite_index)

    p = sub.add_parser("leveled-index")
    add_leveled_args(
        p,
        include_common=True,
        queries_default="/tmp/leveled_fts_bench/index_queries.txt",
        leveled_root_default="/tmp/leveled_fts_bench/leveled-index",
        leveled_ebin_default="/tmp/leveled_fts_bench/ebin-index",
        leveled_result_default="/tmp/leveled_fts_bench/results/leveled-index.tsv",
    )
    p.set_defaults(func=cmd_leveled_index)

    p = sub.add_parser("compare-index")
    add_compare_args(
        p,
        sqlite_result_default="/tmp/leveled_fts_bench/results/sqlite-index.tsv",
        leveled_result_default="/tmp/leveled_fts_bench/results/leveled-index.tsv",
        summary_default="/tmp/leveled_fts_bench/results/summary-index.json",
    )
    p.set_defaults(func=cmd_compare_index)

    p = sub.add_parser("ops-equivalence")
    add_ops_equivalence_args(p)
    p.set_defaults(func=cmd_ops_equivalence)

    p = sub.add_parser("run")
    add_prepare_args(p)
    add_sqlite_args(p, include_tsv=False)
    add_leveled_args(p, include_common=False)
    p.add_argument("--summary", default="/tmp/leveled_fts_bench/results/summary.json")
    p.add_argument("--threshold", type=float, default=2.0)
    p.add_argument("--load-threshold", type=float, default=None)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("sweep")
    add_sweep_args(p)
    p.set_defaults(func=cmd_sweep)

    args = parser.parse_args()
    return args.func(args)


def add_prepare_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--corpus-dir", default="/tmp/leveled_fts_bench/corpus")
    parser.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
    parser.add_argument("--meta", default="/tmp/leveled_fts_bench/corpus_meta.json")
    parser.add_argument("--max-docs", type=int, default=0)
    parser.add_argument("--min-compressed-bytes", type=int, default=1024 * 1024 * 1024)
    parser.add_argument("--allow-small-corpus", action="store_true")


def add_sqlite_args(
    parser: argparse.ArgumentParser,
    include_tsv: bool,
    queries_default: str = "/tmp/leveled_fts_bench/queries.txt",
    sqlite_db_default: str = "/tmp/leveled_fts_bench/sqlite/fts.sqlite3",
    sqlite_result_default: str = "/tmp/leveled_fts_bench/results/sqlite.tsv",
    sqlite_helper_default: str = "/tmp/leveled_fts_bench/sqlite_fts_bench",
) -> None:
    if include_tsv:
        parser.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
    parser.add_argument("--sqlite-db", default=sqlite_db_default)
    parser.add_argument("--sqlite-result", default=sqlite_result_default)
    parser.add_argument("--sqlite-runner", choices=["source", "python"], default="source")
    parser.add_argument("--sqlite-source", default="/Users/dvse/repos/sqlite")
    parser.add_argument("--sqlite-helper", default=sqlite_helper_default)
    parser.add_argument("--queries", default=queries_default)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--insert-batch", type=int, default=200)
    parser.add_argument("--force-sqlite", action="store_true")
    parser.add_argument(
        "--reuse-sqlite",
        action="store_true",
        help="Reuse an existing SQLite database instead of rebuilding it from the TSV.",
    )


def add_leveled_args(
    parser: argparse.ArgumentParser,
    include_common: bool,
    queries_default: str = "/tmp/leveled_fts_bench/queries.txt",
    leveled_root_default: str = "/tmp/leveled_fts_bench/leveled",
    leveled_ebin_default: str = "/tmp/leveled_fts_bench/ebin",
    leveled_result_default: str = "/tmp/leveled_fts_bench/results/leveled.tsv",
) -> None:
    if include_common:
        parser.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
        parser.add_argument("--queries", default=queries_default)
        parser.add_argument("--limit", type=int, default=20)
        parser.add_argument("--runs", type=int, default=5)
        parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repo", default=str(repo_root()))
    parser.add_argument("--leveled-root", default=leveled_root_default)
    parser.add_argument("--leveled-ebin", default=leveled_ebin_default)
    parser.add_argument("--leveled-result", default=leveled_result_default)
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument(
        "--compression-method",
        choices=["native", "lz4", "zstd", "none"],
        default="native",
    )
    parser.add_argument(
        "--ledger-compression",
        choices=["as_store", "native", "lz4", "zstd", "none"],
        default="as_store",
    )
    parser.add_argument("--cache-size", default="default")
    parser.add_argument("--cache-multiple", default="default")
    parser.add_argument("--max-pencillercachesize", default="default")


def add_compare_args(
    parser: argparse.ArgumentParser,
    sqlite_result_default: str = "/tmp/leveled_fts_bench/results/sqlite.tsv",
    leveled_result_default: str = "/tmp/leveled_fts_bench/results/leveled.tsv",
    summary_default: str = "/tmp/leveled_fts_bench/results/summary.json",
) -> None:
    parser.add_argument("--sqlite-result", default=sqlite_result_default)
    parser.add_argument("--leveled-result", default=leveled_result_default)
    parser.add_argument("--summary", default=summary_default)
    parser.add_argument("--corpus-meta", default="")
    parser.add_argument("--threshold", type=float, default=2.0)
    parser.add_argument("--load-threshold", type=float, default=None)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--quiet", action="store_true")


def add_generate_queries_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
    parser.add_argument("--queries", default="/tmp/leveled_fts_bench/query_matrix.txt")
    parser.add_argument("--max-docs", type=int, default=20000)
    parser.add_argument("--min-docs", type=int, default=20000)
    parser.add_argument("--target-count", type=int, default=96)
    parser.add_argument("--allow-small-corpus", action="store_true")


def add_sweep_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
    parser.add_argument("--meta", default="/tmp/leveled_fts_bench/corpus_meta.json")
    add_sqlite_args(parser, include_tsv=False)
    parser.add_argument("--repo", default=str(repo_root()))
    parser.add_argument("--results-dir", default="/tmp/leveled_fts_bench/results/sweep")
    parser.add_argument("--leveled-root-prefix", default="/tmp/leveled_fts_bench/leveled-sweep")
    parser.add_argument("--leveled-ebin-prefix", default="/tmp/leveled_fts_bench/ebin-sweep")
    parser.add_argument(
        "--variant",
        action="append",
        default=[],
        help="label:batch:compression_method:ledger_compression",
    )
    parser.add_argument("--summary", default="/tmp/leveled_fts_bench/results/sweep/summary.json")
    parser.add_argument("--threshold", type=float, default=2.0)
    parser.add_argument("--load-threshold", type=float, default=None)


def add_ops_equivalence_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tsv", default="/tmp/leveled_fts_bench/docs.tsv")
    parser.add_argument("--ops", default="/tmp/leveled_fts_bench/ops/fts_ops.tsv")
    parser.add_argument("--queries", default="/tmp/leveled_fts_bench/ops/queries.txt")
    parser.add_argument("--sqlite-db", default="/tmp/leveled_fts_bench/ops/sqlite-ops.sqlite3")
    parser.add_argument("--sqlite-result", default="/tmp/leveled_fts_bench/results/sqlite-ops.tsv")
    parser.add_argument("--sqlite-source", default="/Users/dvse/repos/sqlite")
    parser.add_argument("--sqlite-helper", default="/tmp/leveled_fts_bench/sqlite_fts_ops_bench")
    parser.add_argument("--repo", default=str(repo_root()))
    parser.add_argument("--leveled-root", default="/tmp/leveled_fts_bench/leveled-ops")
    parser.add_argument("--leveled-ebin", default="/tmp/leveled_fts_bench/ebin-ops")
    parser.add_argument("--leveled-result", default="/tmp/leveled_fts_bench/results/leveled-ops.tsv")
    parser.add_argument("--summary", default="/tmp/leveled_fts_bench/results/summary-ops.json")
    parser.add_argument("--max-docs", type=int, default=20000)
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--threshold", type=float, default=2.0)
    parser.add_argument("--load-threshold", type=float, default=None)
    parser.add_argument("--min-docs", type=int, default=20000)
    parser.add_argument("--allow-small-corpus", action="store_true")
    parser.add_argument(
        "--compression-method",
        choices=["native", "lz4", "zstd", "none"],
        default="native",
    )
    parser.add_argument(
        "--ledger-compression",
        choices=["as_store", "native", "lz4", "zstd", "none"],
        default="as_store",
    )


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def cmd_generate_queries(args: argparse.Namespace) -> int:
    docs = []
    for key, title, body in read_tsv(pathlib.Path(args.tsv)):
        docs.append((key, title, body))
        if args.max_docs > 0 and len(docs) >= args.max_docs:
            break
    if len(docs) < args.min_docs and not args.allow_small_corpus:
        raise ValueError(
            f"query matrix requires at least {args.min_docs} docs; found {len(docs)}"
        )

    term_counts = {}
    title_terms_by_doc = []
    body_terms_by_doc = []
    for _key, title, body in docs:
        title_terms = query_matrix_terms(title)
        body_terms = query_matrix_terms(body)
        title_terms_by_doc.append(title_terms)
        body_terms_by_doc.append(body_terms)
        for term in set(title_terms + body_terms):
            term_counts[term] = term_counts.get(term, 0) + 1

    common_terms = [
        term
        for term, _count in sorted(term_counts.items(), key=lambda item: (-item[1], item[0]))
        if len(term) >= 3
    ]
    rare_terms = [
        term
        for term, _count in sorted(term_counts.items(), key=lambda item: (item[1], item[0]))
        if len(term) >= 5
    ]
    queries = []

    def add(query: str) -> None:
        if query and query not in queries:
            queries.append(query)

    for term in common_terms[:12]:
        add(term)
    for term in rare_terms[:8]:
        add(term)
    for i in range(4):
        add(f"zzmissingfts{i}")
    for term in common_terms:
        if len(term) >= 5:
            add(f"{term[:5]}*")
        if len([q for q in queries if q.endswith("*")]) >= 8:
            break
    for term in common_terms[:8]:
        add(f"title:{term}")
        add(f"body:{term}")
        add(f"-title:{term}")
        add(f"-{{title}}:{term}")
    for terms_by_doc in (title_terms_by_doc, body_terms_by_doc):
        for width in (2, 3, 4):
            for terms in terms_by_doc:
                phrase = first_phrase_terms(terms, width)
                if phrase:
                    add('"' + " ".join(phrase) + '"')
                    break
    for terms in body_terms_by_doc:
        near_terms = first_phrase_terms(terms, 2)
        if near_terms:
            add(f"NEAR({near_terms[0]} {near_terms[1]}, 5)")
            add(f"NEAR({near_terms[0]} {near_terms[1]}, 10)")
            break
    if len(common_terms) >= 4:
        add(f"{common_terms[0]} AND {common_terms[1]}")
        add(f"{common_terms[0]} OR {common_terms[1]}")
        add(f"{common_terms[0]} NOT {common_terms[1]}")
        add(f"({common_terms[0]} OR {common_terms[1]}) AND {common_terms[2]}")
        add(f"{common_terms[0]} OR {common_terms[1]} {common_terms[2]}")
        add(f"{common_terms[0]} NOT {common_terms[1]} NOT {common_terms[2]}")
    add("istanbul")
    add("a")

    for term in common_terms[12:]:
        add(term)
        if len(queries) >= args.target_count:
            break

    output = pathlib.Path(args.queries)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(queries[: args.target_count]) + "\n")
    count, sha = query_contract(queries[: args.target_count])
    print(json.dumps({"queries": str(output), "query_count": count, "query_sha256": sha}))
    return 0


def query_matrix_terms(text: bytes) -> list[str]:
    decoded = text.decode("utf-8", "ignore").lower()
    return re.findall(r"[a-z0-9]+", decoded)


def first_phrase_terms(terms: list[str], width: int):
    for i in range(0, max(0, len(terms) - width + 1)):
        phrase = terms[i : i + width]
        if all(len(term) >= 2 for term in phrase):
            return phrase
    return None


def cmd_run(args: argparse.Namespace) -> int:
    ensure_queries(pathlib.Path(args.queries))
    rc = cmd_prepare(args)
    if rc != 0:
        return rc
    rc = cmd_sqlite(args)
    if rc != 0:
        return rc
    rc = cmd_leveled(args)
    if rc != 0:
        return rc
    args.corpus_meta = args.meta
    return cmd_compare(args)


def cmd_sweep(args: argparse.Namespace) -> int:
    ensure_queries(pathlib.Path(args.queries))
    results_dir = pathlib.Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    sqlite_result = pathlib.Path(args.sqlite_result)

    if not args.reuse_sqlite or not sqlite_result.exists():
        sqlite_args = namespace_with(args, sqlite_result=str(sqlite_result))
        rc = cmd_sqlite(sqlite_args)
        if rc != 0:
            return rc

    variants = parse_sweep_variants(args.variant)
    sweep_results = []
    for variant in variants:
        label = variant["label"]
        leveled_result = results_dir / f"leveled-{label}.tsv"
        compare_summary = results_dir / f"summary-{label}.json"
        leveled_args = namespace_with(
            args,
            leveled_root=f"{args.leveled_root_prefix}-{label}",
            leveled_ebin=f"{args.leveled_ebin_prefix}-{label}",
            leveled_result=str(leveled_result),
            batch=variant["batch"],
            compression_method=variant["compression_method"],
            ledger_compression=variant["ledger_compression"],
        )
        leveled_rc = cmd_leveled(leveled_args)
        compare_rc = None
        comparison = None
        if leveled_rc == 0:
            compare_args = namespace_with(
                args,
                sqlite_result=str(sqlite_result),
                leveled_result=str(leveled_result),
                summary=str(compare_summary),
                corpus_meta=args.meta,
                quiet=True,
            )
            compare_rc = cmd_compare(compare_args)
            comparison = read_optional_json(str(compare_summary))

        sweep_results.append(
            {
                **variant,
                "leveled_result": str(leveled_result),
                "compare_summary": str(compare_summary),
                "leveled_rc": leveled_rc,
                "compare_rc": compare_rc,
                "benchmark_passed": bool(
                    isinstance(comparison, dict) and comparison.get("benchmark_passed")
                ),
                "equivalent_results_all_queries": bool(
                    isinstance(comparison, dict)
                    and comparison.get("equivalent_results_all_queries")
                ),
                "within_provenance_contract": bool(
                    isinstance(comparison, dict) and comparison.get("within_provenance_contract")
                ),
                "within_threshold_all_queries": bool(
                    isinstance(comparison, dict) and comparison.get("within_threshold_all_queries")
                ),
                "within_threshold_load": bool(
                    isinstance(comparison, dict) and comparison.get("within_threshold_load")
                ),
                "leveled_load_to_sqlite_ratio": (
                    comparison.get("leveled_load_to_sqlite_ratio")
                    if isinstance(comparison, dict)
                    else None
                ),
                "leveled_store_bytes": (
                    comparison.get("leveled_metrics", {}).get("store_bytes")
                    if isinstance(comparison, dict)
                    else None
                ),
            }
        )

    sweep = {
        "benchmark_command": " ".join(sys.argv),
        "sqlite_result": str(sqlite_result),
        "query_threshold": args.threshold,
        "load_threshold": args.load_threshold if args.load_threshold is not None else args.threshold,
        "variants": sweep_results,
    }
    summary_path = pathlib.Path(args.summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(sweep, indent=2, sort_keys=True) + "\n")
    print(json.dumps(sweep, indent=2, sort_keys=True))
    return 0 if all(v["benchmark_passed"] for v in sweep_results) else 1


def namespace_with(args: argparse.Namespace, **updates) -> argparse.Namespace:
    values = vars(args).copy()
    values.update(updates)
    return argparse.Namespace(**values)


def parse_sweep_variants(raw_variants):
    variants = raw_variants or [
        "b1000-native-as_store:1000:native:as_store",
        "b200-native-as_store:200:native:as_store",
    ]
    parsed = []
    for raw in variants:
        parts = raw.split(":")
        if len(parts) != 4:
            raise ValueError(
                f"invalid sweep variant {raw!r}; expected label:batch:compression:ledger"
            )
        label, batch, compression_method, ledger_compression = parts
        if not label or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for c in label):
            raise ValueError(f"invalid sweep label {label!r}")
        if compression_method not in {"native", "lz4", "zstd", "none"}:
            raise ValueError(f"invalid compression method {compression_method!r}")
        if ledger_compression not in {"as_store", "native", "lz4", "zstd", "none"}:
            raise ValueError(f"invalid ledger compression {ledger_compression!r}")
        parsed.append(
            {
                "label": label,
                "batch": int(batch),
                "compression_method": compression_method,
                "ledger_compression": ledger_compression,
            }
        )
    return parsed


def cmd_prepare(args: argparse.Namespace) -> int:
    corpus_dir = pathlib.Path(args.corpus_dir)
    files = sorted(corpus_dir.glob("*.bz2"))
    if not files:
        print(f"no .bz2 corpus files found in {corpus_dir}", file=sys.stderr)
        return 2

    compressed_bytes = sum(p.stat().st_size for p in files)
    if compressed_bytes < args.min_compressed_bytes and not args.allow_small_corpus:
        print(
            f"compressed corpus is {compressed_bytes} bytes, below {args.min_compressed_bytes}",
            file=sys.stderr,
        )
        return 2

    tsv = pathlib.Path(args.tsv)
    tsv.parent.mkdir(parents=True, exist_ok=True)
    meta_path = pathlib.Path(args.meta)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    docs = 0
    text_bytes = 0
    started = time.perf_counter_ns()
    with tsv.open("wb") as out:
        for source in files:
            print(f"extracting {source}", file=sys.stderr)
            for title, text in iter_wiki_pages(source):
                if not text.strip():
                    continue
                docs += 1
                key = f"doc-{docs:012d}".encode("ascii")
                title_bytes = title.encode("utf-8", "replace")
                title_b64 = base64.b64encode(title_bytes)
                body = text.encode("utf-8", "replace")
                body_b64 = base64.b64encode(body)
                out.write(key + b"\t" + title_b64 + b"\t" + body_b64 + b"\n")
                text_bytes += len(title_bytes) + len(body)
                if docs % 10000 == 0:
                    print(f"prepared {docs} docs", file=sys.stderr)
                if args.max_docs and docs >= args.max_docs:
                    break
            if args.max_docs and docs >= args.max_docs:
                break

    elapsed_ns = time.perf_counter_ns() - started
    tsv_sha256 = file_sha256(tsv)
    meta = {
        "source_files": [str(p) for p in files],
        "compressed_bytes": compressed_bytes,
        "min_compressed_bytes": args.min_compressed_bytes,
        "docs": docs,
        "text_bytes": text_bytes,
        "tsv": str(tsv),
        "tsv_sha256": tsv_sha256,
        "elapsed_seconds": elapsed_ns / 1_000_000_000,
        "max_docs": args.max_docs or None,
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    print(json.dumps(meta, indent=2, sort_keys=True))
    return 0


def iter_wiki_pages(path: pathlib.Path):
    with bz2.open(path, "rb") as fh:
        for event, elem in ET.iterparse(fh, events=("end",)):
            if local_name(elem.tag) != "page":
                continue
            title = direct_child_text(elem, "title") or ""
            namespace = direct_child_text(elem, "ns") or ""
            redirect = any(local_name(child.tag) == "redirect" for child in list(elem))
            text = first_descendant_text(elem, "text") or ""
            elem.clear()
            if namespace != "0" or redirect:
                continue
            yield title, text


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def direct_child_text(elem, name: str):
    for child in list(elem):
        if local_name(child.tag) == name:
            return child.text
    return None


def first_descendant_text(elem, name: str):
    for child in elem.iter():
        if local_name(child.tag) == name:
            return child.text
    return None


def cmd_sqlite(args: argparse.Namespace) -> int:
    if args.sqlite_runner == "source":
        return cmd_sqlite_source(args)
    return cmd_sqlite_python(args)


def cmd_sqlite_index(args: argparse.Namespace) -> int:
    if args.sqlite_runner != "source":
        print("sqlite-index requires --sqlite-runner source", file=sys.stderr)
        return 2
    return cmd_sqlite_index_source(args)


def cmd_sqlite_source(args: argparse.Namespace) -> int:
    ensure_queries(pathlib.Path(args.queries))
    _query_count, query_sha256 = query_file_contract(pathlib.Path(args.queries))
    tsv = pathlib.Path(args.tsv).resolve()
    tsv_sha256 = file_sha256(tsv)
    sqlite_source = pathlib.Path(args.sqlite_source)
    sqlite_bin = sqlite_source / "sqlite3"
    check_sqlite_cli_fts5(sqlite_bin)
    helper = pathlib.Path(args.sqlite_helper)
    build_meta = compile_sqlite_source_runner(sqlite_source, helper)

    db = pathlib.Path(args.sqlite_db)
    result = pathlib.Path(args.sqlite_result)
    db.parent.mkdir(parents=True, exist_ok=True)
    result.parent.mkdir(parents=True, exist_ok=True)
    helper.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(helper),
        "--tsv",
        str(tsv),
        "--db",
        str(db),
        "--queries",
        str(pathlib.Path(args.queries)),
        "--result",
        str(result),
        "--batch",
        str(args.insert_batch),
        "--limit",
        str(args.limit),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
        "--source-checkout",
        str(sqlite_source),
        "--source-version",
        sqlite_source_version(sqlite_source),
        "--cli-version",
        sqlite_cli_version(sqlite_bin),
        "--queries-sha256",
        query_sha256,
        "--tsv-sha256",
        tsv_sha256,
    ]
    if args.force_sqlite or not args.reuse_sqlite:
        cmd.append("--force")
    rc = subprocess.call(cmd, cwd=str(repo_root()))
    if rc == 0:
        append_result_metrics(result, "sqlite", build_meta)
    return rc


def cmd_sqlite_index_source(args: argparse.Namespace) -> int:
    ensure_index_queries(pathlib.Path(args.queries))
    query_count, query_sha256 = query_file_contract(pathlib.Path(args.queries))
    tsv = pathlib.Path(args.tsv).resolve()
    tsv_sha256 = file_sha256(tsv)
    sqlite_source = pathlib.Path(args.sqlite_source)
    sqlite_bin = sqlite_source / "sqlite3"
    if not sqlite_bin.exists():
        raise FileNotFoundError(f"SQLite CLI not found: {sqlite_bin}")
    helper = pathlib.Path(args.sqlite_helper)
    build_meta = compile_sqlite_index_runner(sqlite_source, helper)

    db = pathlib.Path(args.sqlite_db)
    result = pathlib.Path(args.sqlite_result)
    db.parent.mkdir(parents=True, exist_ok=True)
    result.parent.mkdir(parents=True, exist_ok=True)
    helper.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(helper),
        "--tsv",
        str(tsv),
        "--db",
        str(db),
        "--queries",
        str(pathlib.Path(args.queries)),
        "--result",
        str(result),
        "--batch",
        str(args.insert_batch),
        "--limit",
        str(args.limit),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
        "--source-checkout",
        str(sqlite_source),
        "--source-version",
        sqlite_source_version(sqlite_source),
        "--cli-version",
        sqlite_cli_version(sqlite_bin),
        "--tsv-sha256",
        tsv_sha256,
    ]
    if args.force_sqlite or not args.reuse_sqlite:
        cmd.append("--force")
    rc = subprocess.call(cmd, cwd=str(repo_root()))
    if rc == 0:
        append_result_metrics(result, "sqlite", build_meta)
        append_result_metrics(result, "sqlite", query_run_meta(args, query_count, query_sha256))
    return rc


def cmd_sqlite_python(args: argparse.Namespace) -> int:
    ensure_queries(pathlib.Path(args.queries))
    queries = read_queries(pathlib.Path(args.queries))
    query_count, query_sha256 = query_contract(queries)
    check_sqlite_fts5()
    tsv = pathlib.Path(args.tsv).resolve()
    tsv_sha256 = file_sha256(tsv)
    db = pathlib.Path(args.sqlite_db)
    result = pathlib.Path(args.sqlite_result)
    db.parent.mkdir(parents=True, exist_ok=True)
    result.parent.mkdir(parents=True, exist_ok=True)
    if (args.force_sqlite or not args.reuse_sqlite) and db.exists():
        db.unlink()

    created = not db.exists()
    conn = sqlite3.connect(str(db))
    try:
        configure_sqlite(conn)
        if created:
            load_us, docs, text_bytes = sqlite_load(conn, tsv, args.insert_batch)
        else:
            load_us = 0
            docs = conn.execute("SELECT count(*) FROM docs").fetchone()[0]
            text_bytes = 0
        query_results = sqlite_queries(
            conn,
            queries,
            args.limit,
            args.warmup,
            args.runs,
        )
        store_bytes = db.stat().st_size if db.exists() else 0
    finally:
        conn.close()

    write_results(
        result,
        "sqlite",
        docs,
        text_bytes,
        load_us,
        query_results,
        store_bytes,
        {
            "tsv": str(tsv),
            "tsv_sha256": tsv_sha256,
            "queries": str(pathlib.Path(args.queries)),
            "query_count": query_count,
            "query_sha256": query_sha256,
            "limit": args.limit,
            "runs": args.runs,
            "warmup": args.warmup,
            "sync_strategy": "PRAGMA synchronous=OFF",
            "sqlite_journal_mode": "OFF",
            "sqlite_synchronous": "OFF",
            "sqlite_temp_store": "MEMORY",
            "sqlite_cache_size": "-200000",
            "sqlite_runtime_version": sqlite3.sqlite_version,
            "sqlite_runner": "python_sqlite3_module",
            "sqlite_source_checkout": args.sqlite_source,
            "sqlite_source_version": sqlite_source_version(pathlib.Path(args.sqlite_source)),
            "sqlite_cli_version": sqlite_cli_version(pathlib.Path(args.sqlite_source) / "sqlite3"),
            "sqlite_query_order": "ORDER BY key",
            "sqlite_ddl": SQLITE_FTS_DDL,
        },
    )
    return 0


def check_sqlite_fts5() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(x)")
    finally:
        conn.close()


def check_sqlite_cli_fts5(sqlite_bin: pathlib.Path) -> None:
    if not sqlite_bin.exists():
        raise FileNotFoundError(f"SQLite CLI not found: {sqlite_bin}")
    subprocess.check_call(
        [
            str(sqlite_bin),
            ":memory:",
            "CREATE VIRTUAL TABLE t USING fts5(x); "
            "INSERT INTO t VALUES('hello world'); "
            "SELECT count(*) FROM t WHERE t MATCH 'hello';",
        ],
        stdout=subprocess.DEVNULL,
    )


def sqlite_source_version(sqlite_source: pathlib.Path) -> str:
    version_path = sqlite_source / "VERSION"
    try:
        return version_path.read_text().strip()
    except OSError:
        return "missing"


def sqlite_cli_version(sqlite_bin: pathlib.Path) -> str:
    if not sqlite_bin.exists():
        return "missing"
    try:
        return subprocess.check_output([str(sqlite_bin), "-version"], text=True).strip()
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def compile_sqlite_source_runner(sqlite_source: pathlib.Path, helper: pathlib.Path) -> dict:
    sqlite3_c = sqlite_source / "sqlite3.c"
    sqlite3_h = sqlite_source / "sqlite3.h"
    runner_c = repo_root() / "bench" / "sqlite_fts_bench.c"
    if not sqlite3_c.exists() or not sqlite3_h.exists():
        raise FileNotFoundError(f"SQLite amalgamation not found under {sqlite_source}")
    helper.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "cc",
        "-O2",
        "-DSQLITE_ENABLE_FTS5",
        "-DSQLITE_THREADSAFE=1",
        "-DSQLITE_ENABLE_MATH_FUNCTIONS",
        "-DSQLITE_HAVE_ZLIB=1",
        "-I",
        str(sqlite_source),
        str(runner_c),
        str(sqlite3_c),
        "-lz",
        "-lpthread",
        "-lm",
        "-o",
        str(helper),
    ]
    subprocess.check_call(cmd, cwd=str(repo_root()))
    return sqlite_build_metadata(sqlite_source, helper, runner_c, sqlite3_c, cmd)


def compile_sqlite_index_runner(sqlite_source: pathlib.Path, helper: pathlib.Path) -> dict:
    sqlite3_c = sqlite_source / "sqlite3.c"
    sqlite3_h = sqlite_source / "sqlite3.h"
    runner_c = repo_root() / "bench" / "sqlite_index_bench.c"
    if not sqlite3_c.exists() or not sqlite3_h.exists():
        raise FileNotFoundError(f"SQLite amalgamation not found under {sqlite_source}")
    helper.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "cc",
        "-O2",
        "-DSQLITE_THREADSAFE=1",
        "-DSQLITE_ENABLE_MATH_FUNCTIONS",
        "-DSQLITE_HAVE_ZLIB=1",
        "-I",
        str(sqlite_source),
        str(runner_c),
        str(sqlite3_c),
        "-lz",
        "-lpthread",
        "-lm",
        "-o",
        str(helper),
    ]
    subprocess.check_call(cmd, cwd=str(repo_root()))
    return sqlite_build_metadata(sqlite_source, helper, runner_c, sqlite3_c, cmd)


def sqlite_build_metadata(
    sqlite_source: pathlib.Path,
    helper: pathlib.Path,
    runner_c: pathlib.Path,
    sqlite3_c: pathlib.Path,
    cmd: list[str],
) -> dict:
    return {
        "sqlite_build_source_sha256": files_sha256([runner_c, sqlite3_c], repo_root()),
        "sqlite_helper_sha256": file_sha256(helper),
        "sqlite_compile_command_sha256": hashlib.sha256(
            "\0".join(cmd).encode("utf-8")
        ).hexdigest(),
        "sqlite_compile_command": " ".join(cmd),
        "sqlite_source_git_commit": git_output(sqlite_source, ["rev-parse", "HEAD"]),
        "sqlite_source_git_dirty": git_dirty(sqlite_source),
    }


def configure_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-200000")


def sqlite_load(conn: sqlite3.Connection, tsv: pathlib.Path, batch_size: int):
    conn.execute(SQLITE_FTS_DDL)
    docs = 0
    text_bytes = 0
    started = time.perf_counter_ns()
    conn.execute("BEGIN")
    rows = []
    for key, title, body in read_tsv(tsv):
        rows.append(
            (
                key.decode("utf-8", "replace"),
                title.decode("utf-8", "replace"),
                body.decode("utf-8", "replace"),
            )
        )
        docs += 1
        text_bytes += len(title) + len(body)
        if len(rows) >= batch_size:
            conn.executemany("INSERT INTO docs(key, title, body) VALUES (?, ?, ?)", rows)
            rows.clear()
            if docs % 10000 == 0:
                print(f"sqlite loaded {docs} docs", file=sys.stderr)
    if rows:
        conn.executemany("INSERT INTO docs(key, title, body) VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.execute("PRAGMA optimize")
    load_us = (time.perf_counter_ns() - started) // 1000
    return load_us, docs, text_bytes


def read_tsv(tsv: pathlib.Path):
    with tsv.open("rb") as fh:
        for line in fh:
            line = line.rstrip(b"\r\n")
            if not line:
                continue
            parts = line.split(b"\t")
            if len(parts) != 3:
                raise ValueError(f"invalid TSV line with {len(parts)} columns")
            key, title64, body64 = parts
            yield key, base64.b64decode(title64), base64.b64decode(body64)


def sqlite_queries(conn, queries, limit: int, warmup: int, runs: int):
    results = []
    for query in queries:
        for _ in range(warmup):
            sqlite_query_once(conn, query, limit)
        times = []
        count = 0
        error = "none"
        keys = []
        first_signature = None
        for _ in range(runs):
            started = time.perf_counter_ns()
            try:
                rows = sqlite_query_once(conn, query, limit)
                count = len(rows)
                keys = [row[0] for row in rows]
            except Exception as exc:
                rows = []
                keys = []
                error = repr(exc)
            elapsed_us = (time.perf_counter_ns() - started) // 1000
            times.append(elapsed_us)
            signature = (error, tuple(keys))
            if first_signature is None:
                first_signature = signature
            elif signature != first_signature:
                error = "inconsistent_timed_results"
        started = time.perf_counter_ns()
        try:
            full_keys = [row[0] for row in sqlite_query_full_result(conn, query)]
            total_count = len(full_keys)
            total_error = "none"
        except Exception as exc:
            full_keys = []
            total_count = 0
            total_error = repr(exc)
        full_result_us = (time.perf_counter_ns() - started) // 1000
        if total_error != "none" and error == "none":
            error = total_error
        results.append(
            {
                "query": query,
                "count": count,
                "total_count": total_count,
                "full_result_us": full_result_us,
                "runs_us": times,
                "error": error,
                "keys": keys,
                "full_keys": full_keys,
            }
        )
    return results


def sqlite_query_once(conn, query: str, limit: int):
    return conn.execute(
        "SELECT key FROM docs WHERE docs MATCH ? ORDER BY key LIMIT ?", (query, limit)
    ).fetchall()


def sqlite_query_full_result(conn, query: str):
    return conn.execute(
        "SELECT key FROM docs WHERE docs MATCH ? ORDER BY key", (query,)
    ).fetchall()


def cmd_leveled(args: argparse.Namespace) -> int:
    ensure_queries(pathlib.Path(args.queries))
    tsv = pathlib.Path(args.tsv).resolve()
    repo = pathlib.Path(args.repo).resolve()
    ebin = pathlib.Path(args.leveled_ebin)
    build_meta = compile_leveled(repo, ebin)
    result = pathlib.Path(args.leveled_result)
    result.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "erl",
        "-noshell",
        "-pa",
        str(ebin),
        "-eval",
        "Args=init:get_plain_arguments(), "
        "case leveled_fts_bench:main(Args) of "
        "ok -> halt(0); "
        "{error,R} -> io:format(standard_error, \"~p~n\", [R]), halt(1); "
        "Other -> io:format(standard_error, \"~p~n\", [Other]), halt(1) "
        "end.",
        "-extra",
        "--tsv",
        str(tsv),
        "--root",
        str(args.leveled_root),
        "--queries",
        str(args.queries),
        "--result",
        str(result),
        "--batch",
        str(args.batch),
        "--compression-method",
        str(args.compression_method),
        "--ledger-compression",
        str(args.ledger_compression),
        "--limit",
        str(args.limit),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
    ]
    append_optional_int_arg(cmd, "--cache-size", getattr(args, "cache_size", "default"))
    append_optional_int_arg(cmd, "--cache-multiple", getattr(args, "cache_multiple", "default"))
    append_optional_int_arg(
        cmd, "--max-pencillercachesize", getattr(args, "max_pencillercachesize", "default")
    )
    rc = subprocess.call(cmd, cwd=str(repo))
    if rc == 0:
        append_result_metrics(result, "leveled", build_meta)
    return rc


def cmd_leveled_index(args: argparse.Namespace) -> int:
    ensure_index_queries(pathlib.Path(args.queries))
    query_count, query_sha256 = query_file_contract(pathlib.Path(args.queries))
    tsv = pathlib.Path(args.tsv).resolve()
    repo = pathlib.Path(args.repo).resolve()
    ebin = pathlib.Path(args.leveled_ebin)
    build_meta = compile_leveled(repo, ebin)
    result = pathlib.Path(args.leveled_result)
    result.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "erl",
        "-noshell",
        "-pa",
        str(ebin),
        "-eval",
        "Args=init:get_plain_arguments(), "
        "case leveled_index_bench:main(Args) of "
        "ok -> halt(0); "
        "{error,R} -> io:format(standard_error, \"~p~n\", [R]), halt(1); "
        "Other -> io:format(standard_error, \"~p~n\", [Other]), halt(1) "
        "end.",
        "-extra",
        "--tsv",
        str(tsv),
        "--root",
        str(args.leveled_root),
        "--queries",
        str(args.queries),
        "--result",
        str(result),
        "--batch",
        str(args.batch),
        "--compression-method",
        str(args.compression_method),
        "--ledger-compression",
        str(args.ledger_compression),
        "--limit",
        str(args.limit),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
    ]
    rc = subprocess.call(cmd, cwd=str(repo))
    if rc == 0:
        append_result_metrics(result, "leveled", build_meta)
        append_result_metrics(result, "leveled", query_run_meta(args, query_count, query_sha256))
    return rc


def query_run_meta(args: argparse.Namespace, query_count: int, query_sha256: str) -> dict:
    return {
        "queries": str(pathlib.Path(args.queries)),
        "query_count": query_count,
        "query_sha256": query_sha256,
        "limit": args.limit,
        "runs": args.runs,
        "warmup": args.warmup,
    }


def append_optional_int_arg(cmd: list[str], flag: str, value) -> None:
    if value in (None, "", "default"):
        return
    number = int(value)
    if number <= 0:
        raise ValueError(f"{flag} must be a positive integer or default")
    cmd.extend([flag, str(number)])


def compile_leveled(repo: pathlib.Path, ebin: pathlib.Path) -> dict:
    if ebin.exists():
        shutil.rmtree(ebin)
    ebin.mkdir(parents=True, exist_ok=True)
    sources = sorted(str(p) for p in (repo / "src").glob("*.erl"))
    for bench_module in [
        "leveled_fts_bench.erl",
        "leveled_fts_ops_bench.erl",
        "leveled_index_bench.erl",
    ]:
        path = repo / "bench" / bench_module
        if path.exists():
            sources.append(str(path))
    provenance_sources = [pathlib.Path(p) for p in sources]
    provenance_sources.extend(sorted((repo / "include").glob("*.hrl")))
    coordinator = repo / "bench" / "fts_bench.py"
    if coordinator.exists():
        provenance_sources.append(coordinator)
    cmd = ["erlc", "-I", str(repo / "include"), "-o", str(ebin)] + sources
    subprocess.check_call(cmd, cwd=str(repo))
    ebin_files = sorted(p for p in ebin.rglob("*") if p.is_file())
    return {
        "leveled_repo": str(repo),
        "leveled_git_commit": git_output(repo, ["rev-parse", "HEAD"]),
        "leveled_git_dirty": git_dirty(repo),
        "leveled_compile_source_sha256": files_sha256([pathlib.Path(p) for p in sources], repo),
        "leveled_source_sha256": files_sha256(provenance_sources, repo),
        "leveled_source_file_count": len(provenance_sources),
        "leveled_ebin_sha256": files_sha256(ebin_files, ebin),
        "leveled_compile_command_sha256": hashlib.sha256(
            "\0".join(cmd).encode("utf-8")
        ).hexdigest(),
        "leveled_compile_command": " ".join(cmd),
        "leveled_erlang_version": erlang_version(),
    }


def append_result_metrics(path: pathlib.Path, engine: str, metrics: dict) -> None:
    with path.open("a", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        for key, value in sorted(metrics.items()):
            writer.writerow([engine, key, value, "", "", "", "", "", ""])


def files_sha256(paths, root: pathlib.Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(pathlib.Path(p) for p in paths):
        rel = path.resolve()
        try:
            rel_text = rel.relative_to(root.resolve()).as_posix()
        except ValueError:
            rel_text = str(rel)
        digest.update(rel_text.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def git_output(repo: pathlib.Path, args: list[str]) -> str:
    try:
        return subprocess.check_output(["git"] + args, cwd=str(repo), text=True).strip()
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def git_dirty(repo: pathlib.Path) -> str:
    try:
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=str(repo),
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return "unavailable"
    return "true" if status.strip() else "false"


def erlang_version() -> str:
    try:
        return subprocess.check_output(
            [
                "erl",
                "-noshell",
                "-eval",
                'io:format("~s", [erlang:system_info(system_version)]), halt().',
            ],
            text=True,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def cmd_compare(args: argparse.Namespace) -> int:
    return cmd_compare_with(args, compare_provenance_checks)


def cmd_compare_index(args: argparse.Namespace) -> int:
    return cmd_compare_with(args, compare_index_provenance_checks)


def cmd_ops_equivalence(args: argparse.Namespace) -> int:
    tsv = pathlib.Path(args.tsv).resolve()
    ops = pathlib.Path(args.ops)
    queries = pathlib.Path(args.queries)
    sqlite_source = pathlib.Path(args.sqlite_source)
    sqlite_bin = sqlite_source / "sqlite3"
    sqlite_helper = pathlib.Path(args.sqlite_helper)
    sqlite_db = pathlib.Path(args.sqlite_db)
    sqlite_result = pathlib.Path(args.sqlite_result)
    repo = pathlib.Path(args.repo).resolve()
    leveled_ebin = pathlib.Path(args.leveled_ebin)
    leveled_result = pathlib.Path(args.leveled_result)

    ensure_ops_queries(queries)
    ops_meta = write_ops_fixture(tsv, ops, args.max_docs)
    ops_meta["min_docs"] = args.min_docs
    ops_meta["allow_small_corpus"] = bool(args.allow_small_corpus)
    ops_meta["within_min_docs"] = ops_meta["source_docs"] >= args.min_docs
    if not ops_meta["within_min_docs"] and not args.allow_small_corpus:
        print(
            f"ops fixture has {ops_meta['source_docs']} docs, below --min-docs {args.min_docs}",
            file=sys.stderr,
        )
        return 2
    ops_meta["queries"] = str(queries.resolve())
    _ops_query_count, ops_query_sha256 = query_file_contract(queries)

    check_sqlite_cli_fts5(sqlite_bin)
    sqlite_build_meta = compile_sqlite_source_runner(sqlite_source, sqlite_helper)
    leveled_build_meta = compile_leveled(repo, leveled_ebin)

    sqlite_db.parent.mkdir(parents=True, exist_ok=True)
    sqlite_result.parent.mkdir(parents=True, exist_ok=True)
    sqlite_cmd = [
        str(sqlite_helper),
        "--ops",
        str(ops.resolve()),
        "--db",
        str(sqlite_db),
        "--queries",
        str(queries),
        "--result",
        str(sqlite_result),
        "--batch",
        str(args.batch),
        "--limit",
        str(args.limit),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
        "--source-checkout",
        str(sqlite_source),
        "--source-version",
        sqlite_source_version(sqlite_source),
        "--cli-version",
        sqlite_cli_version(sqlite_bin),
        "--queries-sha256",
        ops_query_sha256,
        "--force",
    ]
    sqlite_rc = subprocess.call(sqlite_cmd, cwd=str(repo_root()))
    if sqlite_rc != 0:
        return sqlite_rc
    append_result_metrics(sqlite_result, "sqlite", sqlite_build_meta)

    leveled_result.parent.mkdir(parents=True, exist_ok=True)
    leveled_cmd = [
        "erl",
        "-noshell",
        "-pa",
        str(leveled_ebin),
        "-eval",
        "Args=init:get_plain_arguments(), "
        "case leveled_fts_ops_bench:main(Args) of "
        "ok -> halt(0); "
        "{error,R} -> io:format(standard_error, \"~p~n\", [R]), halt(1); "
        "Other -> io:format(standard_error, \"~p~n\", [Other]), halt(1) "
        "end.",
        "-extra",
        "--ops",
        str(ops.resolve()),
        "--root",
        str(args.leveled_root),
        "--queries",
        str(queries),
        "--result",
        str(leveled_result),
        "--batch",
        str(args.batch),
        "--compression-method",
        str(args.compression_method),
        "--ledger-compression",
        str(args.ledger_compression),
        "--limit",
        str(args.limit),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
    ]
    leveled_rc = subprocess.call(leveled_cmd, cwd=str(repo))
    if leveled_rc != 0:
        return leveled_rc
    append_result_metrics(leveled_result, "leveled", leveled_build_meta)

    return compare_ops_results(
        sqlite_result,
        leveled_result,
        pathlib.Path(args.summary),
        ops_meta,
        args.limit,
        args.threshold,
        args.load_threshold if args.load_threshold is not None else args.threshold,
    )


def write_ops_fixture(tsv: pathlib.Path, ops: pathlib.Path, max_docs: int) -> dict:
    if max_docs < 1:
        raise ValueError("--max-docs must be positive")
    docs = []
    for source_id, (key, title, body) in enumerate(read_tsv(tsv), start=1):
        docs.append((source_id, key, title, body))
        if source_id >= max_docs:
            break
    if not docs:
        raise ValueError(f"no documents available in {tsv}")

    operations = []
    for source_id, key, title, body in docs:
        title1 = append_ascii_terms(title, ["equivtitlebaseline"])
        terms = ["equivbaseline", "equivprefixbase", f"equivdoc{source_id}"]
        if source_id % 10 == 0:
            terms.extend(["equivupdateold", "equivoldprefixupdate"])
        if source_id % 17 == 0:
            terms.extend(["equivdeleteold", "equivoldprefixdelete"])
        if source_id % 34 == 0:
            terms.append("equivreinsertold")
        if source_id % 7 == 0:
            terms.extend(["new", "york"])
        if source_id % 13 == 0:
            terms.extend(["equivnearone", "middle", "equivneartwo"])
        operations.append(("put", source_id, key, title1, append_ascii_terms(body, terms)))

    for source_id, key, title, _body in docs:
        if source_id % 10 == 0:
            operations.append(
                (
                    "put",
                    source_id,
                    key,
                    append_ascii_terms(title, ["equivtitleupdate"]),
                    ascii_doc(
                        [
                            "equivupdate",
                            "equivbeta",
                            "equivbodyupdate",
                            "equivprefixlive",
                            "new",
                            "york",
                            "equivnearone",
                            "gap",
                            "equivneartwo",
                        ]
                    ),
                )
            )

    for source_id, key, _title, _body in docs:
        if source_id % 17 == 0:
            operations.append(("delete", source_id, key, b"", b""))

    for source_id, key, title, _body in docs:
        if source_id % 34 == 0:
            operations.append(
                (
                    "put",
                    source_id,
                    key,
                    append_ascii_terms(title, ["equivtitlereinsert"]),
                    ascii_doc(
                        [
                            "equivreinsert",
                            "equivprefixlive",
                            "new",
                            "york",
                            "equivnearone",
                            "gap",
                            "equivneartwo",
                        ]
                    ),
                )
            )

    for source_id, key, title, _body in docs:
        if source_id % 68 == 0:
            operations.append(
                (
                    "put",
                    source_id,
                    key,
                    append_ascii_terms(title, ["equivtitlelateupdate"]),
                    ascii_doc(
                        [
                            "equivlateupdate",
                            "equivprefixlate",
                            "equivbodylateupdate",
                        ]
                    ),
                )
            )

    ops.parent.mkdir(parents=True, exist_ok=True)
    active = {}
    puts = 0
    deletes = 0
    with ops.open("wb") as fh:
        for op, source_id, key, title, body in operations:
            if op == "put":
                puts += 1
                active[key] = len(title) + len(body)
                fh.write(
                    b"\t".join(
                        [
                            b"put",
                            str(source_id).encode("ascii"),
                            key,
                            base64.b64encode(title),
                            base64.b64encode(body),
                        ]
                    )
                    + b"\n"
                )
            else:
                deletes += 1
                active.pop(key, None)
                fh.write(
                    b"\t".join(
                        [b"delete", str(source_id).encode("ascii"), key, b"", b""]
                    )
                    + b"\n"
                )

    return {
        "tsv": str(tsv),
        "tsv_sha256": file_sha256(tsv),
        "ops": str(ops.resolve()),
        "ops_sha256": file_sha256(ops),
        "source_docs": len(docs),
        "ops_count": len(operations),
        "puts": puts,
        "deletes": deletes,
        "active_docs": len(active),
        "active_text_bytes": sum(active.values()),
        "operation_contract": (
            "initial TSV puts, deterministic replacement puts, deletes, reinserts, "
            "and late replacement puts keyed by source id/key"
        ),
        "document_identity_contract": (
            "stable key per source id; Leveled document identity is key"
        ),
        "fts_batch_contract": (
            "one FTS mutation per {Bucket,Key,Index} per Leveled batch; "
            "harness flushes repeated identities before batching"
        ),
    }


def append_ascii_terms(text: bytes, terms) -> bytes:
    return text + b"\n" + " ".join(terms).encode("ascii")


def ascii_doc(terms) -> bytes:
    return " ".join(terms).encode("ascii")


def compare_ops_results(
    sqlite_result: pathlib.Path,
    leveled_result: pathlib.Path,
    summary_path: pathlib.Path,
    ops_meta: dict,
    expected_limit: int,
    query_threshold: float,
    load_threshold: float,
) -> int:
    sqlite_rows = read_result_rows(sqlite_result)
    leveled_rows = read_result_rows(leveled_result)
    sqlite_metrics = metric_rows(sqlite_rows)
    leveled_metrics = metric_rows(leveled_rows)
    sqlite_queries_by_text = query_rows(sqlite_rows)
    leveled_queries_by_text = query_rows(leveled_rows)
    if set(sqlite_queries_by_text) != set(leveled_queries_by_text):
        raise ValueError(
            "query sets differ: "
            f"missing_leveled={sorted(set(sqlite_queries_by_text) - set(leveled_queries_by_text))!r} "
            f"extra_leveled={sorted(set(leveled_queries_by_text) - set(sqlite_queries_by_text))!r}"
        )

    comparisons = []
    all_equivalent = sqlite_metrics.get("docs") == leveled_metrics.get("docs")
    for query, sqlite_row in sqlite_queries_by_text.items():
        leveled_row = leveled_queries_by_text[query]
        sqlite_med = median(sqlite_row["runs_us"])
        leveled_med = median(leveled_row["runs_us"])
        ratio = None if sqlite_med == 0 else leveled_med / sqlite_med
        within = ratio is not None and ratio <= query_threshold
        sqlite_full_result_us = sqlite_row["full_result_us"]
        leveled_full_result_us = leveled_row["full_result_us"]
        full_result_ratio = (
            None
            if not sqlite_full_result_us
            else leveled_full_result_us / sqlite_full_result_us
        )
        full_result_within = (
            full_result_ratio is not None and full_result_ratio <= query_threshold
        )
        equivalent = (
            sqlite_row["count"] == leveled_row["count"]
            and sqlite_row["total_count"] == leveled_row["total_count"]
            and sqlite_row["keys"] == leveled_row["keys"]
            and sqlite_row["full_keys"] == leveled_row["full_keys"]
            and sqlite_row["error"] == "none"
            and leveled_row["error"] == "none"
        )
        all_equivalent = all_equivalent and equivalent
        comparisons.append(
            {
                "query": query,
                "sqlite_median_us": sqlite_med,
                "leveled_median_us": leveled_med,
                "leveled_to_sqlite_ratio": ratio,
                "within_threshold": within,
                "sqlite_full_result_us": sqlite_full_result_us,
                "leveled_full_result_us": leveled_full_result_us,
                "leveled_full_result_to_sqlite_ratio": full_result_ratio,
                "within_full_result_threshold": full_result_within,
                "equivalent_result": equivalent,
                "sqlite_count": sqlite_row["count"],
                "leveled_count": leveled_row["count"],
                "sqlite_total_count": sqlite_row["total_count"],
                "leveled_total_count": leveled_row["total_count"],
                "full_keys_equal": sqlite_row["full_keys"] == leveled_row["full_keys"],
                "sqlite_full_keys_sha256": keys_sha256(sqlite_row["full_keys"]),
                "leveled_full_keys_sha256": keys_sha256(leveled_row["full_keys"]),
                "sqlite_error": sqlite_row["error"],
                "leveled_error": leveled_row["error"],
                "sqlite_keys": sqlite_row["keys"],
                "leveled_keys": leveled_row["keys"],
            }
        )

    provenance_checks = compare_ops_provenance_checks(
        sqlite_metrics, leveled_metrics, ops_meta, expected_limit
    ) + compare_query_contract_checks(
        sqlite_metrics,
        leveled_metrics,
        sqlite_queries_by_text,
        leveled_queries_by_text,
        require_full_result_us=True,
    )
    provenance_ok = all(check["passed"] for check in provenance_checks)
    sqlite_load_us = load_metric(sqlite_metrics)
    leveled_load_us = load_metric(leveled_metrics)
    load_ratio = (
        None
        if not isinstance(sqlite_load_us, int)
        or not isinstance(leveled_load_us, int)
        or sqlite_load_us == 0
            else leveled_load_us / sqlite_load_us
    )
    load_within = load_ratio is not None and load_ratio <= load_threshold
    within_threshold_all_queries = all(c.get("within_threshold") for c in comparisons)
    within_full_result_threshold_all_queries = all(
        c.get("within_full_result_threshold") for c in comparisons
    )
    summary = {
        "harness_schema_version": HARNESS_SCHEMA_VERSION,
        "query_threshold": query_threshold,
        "load_threshold": load_threshold,
        "performance_scope": "correctness_and_timing_report",
        "benchmark_command": " ".join(sys.argv),
        "ops_meta": ops_meta,
        "result_paths": {
            "sqlite": str(sqlite_result),
            "leveled": str(leveled_result),
        },
        "sqlite_metrics": sqlite_metrics,
        "leveled_metrics": leveled_metrics,
        "load_time_metric": "load_reopened_us",
        "sqlite_load_reopened_us": sqlite_load_us,
        "leveled_load_reopened_us": leveled_load_us,
        "leveled_load_to_sqlite_ratio": load_ratio,
        "same_doc_count": sqlite_metrics.get("docs") == leveled_metrics.get("docs"),
        "same_active_text_bytes": sqlite_metrics.get("text_bytes")
        == leveled_metrics.get("text_bytes"),
        "equivalent_results_all_queries": all_equivalent,
        "equivalent_results_supplied_queries": all_equivalent,
        "query_scope": "supplied_queries",
        "supplied_query_count": len(sqlite_queries_by_text),
        "supplied_query_sha256": query_texts_sha256(list(sqlite_queries_by_text)),
        "within_provenance_contract": provenance_ok,
        "ops_equivalence_passed": all_equivalent and provenance_ok,
        "within_threshold_all_queries": within_threshold_all_queries,
        "within_full_result_threshold_all_queries": within_full_result_threshold_all_queries,
        "within_threshold_load": load_within,
        "within_threshold_all": within_threshold_all_queries and load_within,
        "benchmark_passed": (
            all_equivalent
            and provenance_ok
            and within_threshold_all_queries
            and within_full_result_threshold_all_queries
            and load_within
        ),
        "provenance_checks": provenance_checks,
        "comparisons": comparisons,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["benchmark_passed"] else 1


def compare_query_contract_checks(
    sqlite_metrics: dict,
    leveled_metrics: dict,
    sqlite_queries_by_text: dict,
    leveled_queries_by_text: dict,
    require_full_result_us: bool = True,
):
    checks = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    sqlite_query_order = list(sqlite_queries_by_text)
    leveled_query_order = list(leveled_queries_by_text)
    query_count = len(sqlite_query_order)
    query_sha256 = query_texts_sha256(sqlite_query_order)
    query_file = sqlite_metrics.get("queries")
    file_queries = []
    file_query_sha256 = None
    file_query_error = None
    try:
        if query_file:
            file_queries = read_existing_queries(pathlib.Path(str(query_file)))
            _file_count, file_query_sha256 = query_contract(file_queries)
        else:
            file_query_error = "missing query file metric"
    except (OSError, ValueError) as exc:
        file_query_error = repr(exc)
    add(
        "same_query_file",
        bool(sqlite_metrics.get("queries"))
        and sqlite_metrics.get("queries") == leveled_metrics.get("queries"),
        f"sqlite={sqlite_metrics.get('queries')!r} leveled={leveled_metrics.get('queries')!r}",
    )
    add(
        "query_file_readable",
        file_query_error is None,
        f"queries={query_file!r} error={file_query_error!r}",
    )
    add(
        "sqlite_query_rows_match_file",
        file_query_error is None and sqlite_query_order == file_queries,
        f"file_count={len(file_queries)!r} sqlite_row_count={query_count!r}",
    )
    add(
        "leveled_query_rows_match_file",
        file_query_error is None and leveled_query_order == file_queries,
        f"file_count={len(file_queries)!r} leveled_row_count={len(leveled_query_order)!r}",
    )
    add(
        "same_query_row_order",
        sqlite_query_order == leveled_query_order,
        f"sqlite={sqlite_query_order!r} leveled={leveled_query_order!r}",
    )
    add(
        "same_query_count",
        sqlite_metrics.get("query_count")
        == leveled_metrics.get("query_count")
        == query_count
        == len(file_queries),
        (
            f"rows={query_count!r} file={len(file_queries)!r} "
            f"sqlite={sqlite_metrics.get('query_count')!r} "
            f"leveled={leveled_metrics.get('query_count')!r}"
        ),
    )
    add(
        "same_query_sha256",
        sqlite_metrics.get("query_sha256")
        == leveled_metrics.get("query_sha256")
        == query_sha256
        == file_query_sha256,
        (
            f"rows={query_sha256!r} file={file_query_sha256!r} "
            f"sqlite={sqlite_metrics.get('query_sha256')!r} "
            f"leveled={leveled_metrics.get('query_sha256')!r}"
        ),
    )
    add(
        "nonempty_query_suite",
        query_count > 0 and len(leveled_queries_by_text) == query_count,
        f"sqlite_rows={query_count!r} leveled_rows={len(leveled_queries_by_text)!r}",
    )
    for metric in ["limit", "runs", "warmup"]:
        add(
            f"same_{metric}",
            sqlite_metrics.get(metric) == leveled_metrics.get(metric),
            f"sqlite={sqlite_metrics.get(metric)!r} leveled={leveled_metrics.get(metric)!r}",
        )
    expected_runs = sqlite_metrics.get("runs")
    add(
        "timed_run_count_matches_config",
        isinstance(expected_runs, int)
        and expected_runs > 0
        and all(len(row["runs_us"]) == expected_runs for row in sqlite_queries_by_text.values())
        and all(len(row["runs_us"]) == expected_runs for row in leveled_queries_by_text.values()),
        f"runs={expected_runs!r}",
    )
    add(
        "full_result_us_rows_present",
        (not require_full_result_us)
        or (
            all(isinstance(row.get("full_result_us"), int) for row in sqlite_queries_by_text.values())
            and all(
                isinstance(row.get("full_result_us"), int)
                for row in leveled_queries_by_text.values()
            )
        ),
        f"required={require_full_result_us!r}",
    )
    add(
        "full_result_keys_present",
        (not require_full_result_us)
        or (
            all(len(row.get("full_keys", [])) == row.get("total_count") for row in sqlite_queries_by_text.values())
            and all(
                len(row.get("full_keys", [])) == row.get("total_count")
                for row in leveled_queries_by_text.values()
            )
        ),
        f"required={require_full_result_us!r}",
    )
    return checks


def query_texts_sha256(queries):
    metadata = "".join(f"{query}\n" for query in queries).encode("utf-8")
    return hashlib.sha256(metadata).hexdigest()


def compare_ops_provenance_checks(
    sqlite_metrics: dict,
    leveled_metrics: dict,
    ops_meta: dict,
    expected_limit: int,
):
    checks = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    def same_path(*paths) -> bool:
        if not paths or any(not p for p in paths):
            return False
        try:
            resolved = [pathlib.Path(str(p)).resolve() for p in paths]
        except OSError:
            return False
        return all(p == resolved[0] for p in resolved)

    sqlite_compile_options = str(sqlite_metrics.get("sqlite_compile_options", ""))
    sqlite_source_version_value = sqlite_metrics.get("sqlite_source_version")
    sqlite_cli_version_value = str(sqlite_metrics.get("sqlite_cli_version", ""))
    sqlite_ddl = SQLITE_FTS_DDL
    expected_ops = ops_meta.get("ops_count")
    expected_puts = ops_meta.get("puts")
    expected_deletes = ops_meta.get("deletes")

    ops_path = pathlib.Path(str(ops_meta.get("ops", "")))
    tsv_path = pathlib.Path(str(ops_meta.get("tsv", "")))
    try:
        actual_ops_sha256 = file_sha256(ops_path)
    except OSError:
        actual_ops_sha256 = None
    try:
        actual_tsv_sha256 = file_sha256(tsv_path)
    except OSError:
        actual_tsv_sha256 = None

    add(
        "same_ops",
        same_path(ops_meta.get("ops"), sqlite_metrics.get("ops"), leveled_metrics.get("ops")),
        (
            f"meta={ops_meta.get('ops')!r} sqlite={sqlite_metrics.get('ops')!r} "
            f"leveled={leveled_metrics.get('ops')!r}"
        ),
    )
    add(
        "ops_sha256_recorded",
        bool(ops_meta.get("ops_sha256")) and ops_meta.get("ops_sha256") == actual_ops_sha256,
        f"recorded={ops_meta.get('ops_sha256')!r} actual={actual_ops_sha256!r}",
    )
    add(
        "tsv_sha256_recorded",
        bool(ops_meta.get("tsv_sha256")) and ops_meta.get("tsv_sha256") == actual_tsv_sha256,
        f"recorded={ops_meta.get('tsv_sha256')!r} actual={actual_tsv_sha256!r}",
    )
    add(
        "representative_source_docs",
        bool(ops_meta.get("within_min_docs")) or bool(ops_meta.get("allow_small_corpus")),
        (
            f"source_docs={ops_meta.get('source_docs')!r} "
            f"min_docs={ops_meta.get('min_docs')!r} "
            f"allow_small_corpus={ops_meta.get('allow_small_corpus')!r}"
        ),
    )
    add(
        "same_queries",
        same_path(
            ops_meta.get("queries"),
            sqlite_metrics.get("queries"),
            leveled_metrics.get("queries"),
        ),
        (
            f"meta={ops_meta.get('queries')!r} sqlite={sqlite_metrics.get('queries')!r} "
            f"leveled={leveled_metrics.get('queries')!r}"
        ),
    )
    add(
        "same_active_docs",
        sqlite_metrics.get("docs") == leveled_metrics.get("docs") == ops_meta.get("active_docs"),
        (
            f"meta={ops_meta.get('active_docs')!r} sqlite={sqlite_metrics.get('docs')!r} "
            f"leveled={leveled_metrics.get('docs')!r}"
        ),
    )
    add(
        "same_active_text_bytes",
        sqlite_metrics.get("text_bytes")
        == leveled_metrics.get("text_bytes")
        == ops_meta.get("active_text_bytes"),
        (
            f"meta={ops_meta.get('active_text_bytes')!r} "
            f"sqlite={sqlite_metrics.get('text_bytes')!r} "
            f"leveled={leveled_metrics.get('text_bytes')!r}"
        ),
    )
    add(
        "same_operation_counts",
        sqlite_metrics.get("ops_count") == leveled_metrics.get("ops_count") == expected_ops
        and sqlite_metrics.get("puts") == leveled_metrics.get("puts") == expected_puts
        and sqlite_metrics.get("deletes") == leveled_metrics.get("deletes") == expected_deletes,
        (
            f"expected ops/puts/deletes={expected_ops!r}/{expected_puts!r}/{expected_deletes!r} "
            f"sqlite={sqlite_metrics.get('ops_count')!r}/{sqlite_metrics.get('puts')!r}/"
            f"{sqlite_metrics.get('deletes')!r} leveled={leveled_metrics.get('ops_count')!r}/"
            f"{leveled_metrics.get('puts')!r}/{leveled_metrics.get('deletes')!r}"
        ),
    )
    add(
        "finalized_load_recorded",
        isinstance(sqlite_metrics.get("load_finalized_us"), int)
        and isinstance(leveled_metrics.get("load_finalized_us"), int)
        and isinstance(sqlite_metrics.get("load_reopened_us"), int)
        and isinstance(leveled_metrics.get("load_reopened_us"), int)
        and isinstance(sqlite_metrics.get("close_us"), int)
        and isinstance(sqlite_metrics.get("query_reopen_us"), int)
        and isinstance(sqlite_metrics.get("query_close_us"), int)
        and isinstance(leveled_metrics.get("close_us"), int)
        and isinstance(leveled_metrics.get("query_reopen_us"), int)
        and isinstance(leveled_metrics.get("query_close_us"), int),
        (
            f"sqlite_load_finalized={sqlite_metrics.get('load_finalized_us')!r} "
            f"leveled_load_finalized={leveled_metrics.get('load_finalized_us')!r} "
            f"sqlite_load_reopened={sqlite_metrics.get('load_reopened_us')!r} "
            f"leveled_load_reopened={leveled_metrics.get('load_reopened_us')!r} "
            f"sqlite_close={sqlite_metrics.get('close_us')!r} "
            f"sqlite_reopen={sqlite_metrics.get('query_reopen_us')!r} "
            f"sqlite_query_close={sqlite_metrics.get('query_close_us')!r} "
            f"leveled_close={leveled_metrics.get('close_us')!r} "
            f"leveled_reopen={leveled_metrics.get('query_reopen_us')!r} "
            f"leveled_query_close={leveled_metrics.get('query_close_us')!r}"
        ),
    )
    add(
        "same_query_connection_contract",
        sqlite_metrics.get("query_connection_contract")
        == leveled_metrics.get("query_connection_contract")
        == "close after load, reopen before timed queries",
        (
            f"sqlite={sqlite_metrics.get('query_connection_contract')!r} "
            f"leveled={leveled_metrics.get('query_connection_contract')!r}"
        ),
    )
    add(
        "same_load_batch_size",
        isinstance(sqlite_metrics.get("load_batch_size"), int)
        and sqlite_metrics.get("load_batch_size") == leveled_metrics.get("load_batch_size"),
        (
            f"sqlite={sqlite_metrics.get('load_batch_size')!r} "
            f"leveled={leveled_metrics.get('load_batch_size')!r}"
        ),
    )
    add(
        "load_batches_recorded",
        isinstance(sqlite_metrics.get("load_batches"), int)
        and sqlite_metrics.get("load_batches") > 0
        and isinstance(leveled_metrics.get("load_batches"), int)
        and leveled_metrics.get("load_batches") > 0,
        (
            f"sqlite={sqlite_metrics.get('load_batches')!r} "
            f"leveled={leveled_metrics.get('load_batches')!r}"
        ),
    )
    add(
        "load_transaction_contract_recorded",
        bool(sqlite_metrics.get("load_transaction_contract"))
        and bool(leveled_metrics.get("load_transaction_contract")),
        (
            f"sqlite={sqlite_metrics.get('load_transaction_contract')!r} "
            f"leveled={leveled_metrics.get('load_transaction_contract')!r}"
        ),
    )
    add(
        "stable_key_identity_contract",
        ops_meta.get("document_identity_contract")
        == "stable key per source id; Leveled document identity is key",
        f"contract={ops_meta.get('document_identity_contract')!r}",
    )
    add(
        "unique_fts_batch_contract",
        ops_meta.get("fts_batch_contract")
        == (
            "one FTS mutation per {Bucket,Key,Index} per Leveled batch; "
            "harness flushes repeated identities before batching"
        ),
        f"contract={ops_meta.get('fts_batch_contract')!r}",
    )
    add(
        "leveled_collapsed_ops_count_reported",
        isinstance(leveled_metrics.get("collapsed_ops_count"), int),
        f"collapsed_ops_count={leveled_metrics.get('collapsed_ops_count')!r}",
    )
    add(
        "leveled_no_collapsed_ops_for_load_comparison",
        leveled_metrics.get("collapsed_ops_count") == 0,
        f"collapsed_ops_count={leveled_metrics.get('collapsed_ops_count')!r}",
    )
    add(
        "sqlite_source_runner",
        sqlite_metrics.get("sqlite_runner") == "source_amalgamation_c",
        f"sqlite_runner={sqlite_metrics.get('sqlite_runner')!r}",
    )
    add(
        "sqlite_runtime_matches_source",
        sqlite_metrics.get("sqlite_runtime_version") == sqlite_source_version_value,
        (
            f"runtime={sqlite_metrics.get('sqlite_runtime_version')!r} "
            f"source={sqlite_source_version_value!r}"
        ),
    )
    add(
        "sqlite_cli_matches_source",
        bool(sqlite_source_version_value)
        and sqlite_cli_version_value.startswith(str(sqlite_source_version_value)),
        f"cli={sqlite_cli_version_value!r} source={sqlite_source_version_value!r}",
    )
    add(
        "sqlite_fts5_enabled",
        "ENABLE_FTS5" in sqlite_compile_options.split(","),
        "ENABLE_FTS5 in sqlite_compile_options",
    )
    add(
        "sqlite_build_provenance",
        bool(sqlite_metrics.get("sqlite_build_source_sha256"))
        and bool(sqlite_metrics.get("sqlite_helper_sha256"))
        and bool(sqlite_metrics.get("sqlite_compile_command_sha256"))
        and sqlite_metrics.get("sqlite_source_git_dirty") in ("true", "false"),
        (
            f"source_sha={sqlite_metrics.get('sqlite_build_source_sha256')!r} "
            f"helper_sha={sqlite_metrics.get('sqlite_helper_sha256')!r} "
            f"cmd_sha={sqlite_metrics.get('sqlite_compile_command_sha256')!r} "
            f"dirty={sqlite_metrics.get('sqlite_source_git_dirty')!r}"
        ),
    )
    add(
        "sqlite_ddl_contract",
        sqlite_metrics.get("sqlite_ddl") == sqlite_ddl,
        f"sqlite_ddl={sqlite_metrics.get('sqlite_ddl')!r}",
    )
    add(
        "sqlite_runtime_pragmas_recorded",
        sqlite_runtime_pragmas_match(sqlite_metrics),
        sqlite_runtime_pragmas_detail(sqlite_metrics),
    )
    add(
        "sqlite_mutation_contract",
        sqlite_metrics.get("mutation_contract")
        == "SQLite FTS5 docid INSERT OR REPLACE put; DELETE by docid",
        f"mutation_contract={sqlite_metrics.get('mutation_contract')!r}",
    )
    add(
        "leveled_mutation_contract",
        leveled_metrics.get("mutation_contract")
        == "book_batchput put/delete with automatic fts_indexes; book_ftssearch rank none",
        f"mutation_contract={leveled_metrics.get('mutation_contract')!r}",
    )
    add(
        "leveled_query_order_contract",
        leveled_metrics.get("query_order_contract") == "rank_none_order_by_key",
        f"query_order_contract={leveled_metrics.get('query_order_contract')!r}",
    )
    add(
        "leveled_build_provenance",
        bool(leveled_metrics.get("leveled_source_sha256"))
        and bool(leveled_metrics.get("leveled_compile_source_sha256"))
        and int(leveled_metrics.get("leveled_source_file_count") or 0) > 0
        and bool(leveled_metrics.get("leveled_ebin_sha256"))
        and bool(leveled_metrics.get("leveled_compile_command_sha256"))
        and bool(leveled_metrics.get("leveled_erlang_version"))
        and leveled_metrics.get("leveled_git_dirty") in ("true", "false"),
        (
            f"source_sha={leveled_metrics.get('leveled_source_sha256')!r} "
            f"compile_source_sha={leveled_metrics.get('leveled_compile_source_sha256')!r} "
            f"source_file_count={leveled_metrics.get('leveled_source_file_count')!r} "
            f"ebin_sha={leveled_metrics.get('leveled_ebin_sha256')!r} "
            f"cmd_sha={leveled_metrics.get('leveled_compile_command_sha256')!r} "
            f"erl={leveled_metrics.get('leveled_erlang_version')!r} "
            f"dirty={leveled_metrics.get('leveled_git_dirty')!r}"
        ),
    )
    leveled_search_opts = str(leveled_metrics.get("search_opts", ""))
    leveled_representation = str(leveled_metrics.get("fts_representation_contract", ""))
    add(
        "leveled_search_contract",
        "rank => none" in leveled_search_opts
        and "columns => [title,body]" in leveled_search_opts
        and "prefixes => [5,11]" in leveled_search_opts
        and f"limit => {expected_limit}" in leveled_search_opts
        and leveled_representation == FTS_REPRESENTATION_CONTRACT,
        (
            f"expected_representation={FTS_REPRESENTATION_CONTRACT!r} "
            f"reported_representation={leveled_representation!r} "
            f"search_opts={leveled_search_opts!r}"
        ),
    )
    expected_prefix_contract = "secondary_index_token_range_scan"
    add(
        "leveled_prefix_execution_contract",
        leveled_metrics.get("prefix_execution_contract") == expected_prefix_contract,
        (
            f"expected={expected_prefix_contract!r} "
            f"actual={leveled_metrics.get('prefix_execution_contract')!r}"
        ),
    )
    add(
        "leveled_payload_visibility_contract",
        leveled_metrics.get("payload_visibility_contract")
        == "no_hidden_objects_payload_in_index_metadata",
        f"payload_visibility_contract={leveled_metrics.get('payload_visibility_contract')!r}",
    )
    add(
        "leveled_rank_none_snapshot_contract",
        leveled_metrics.get("rank_none_snapshot_contract") == RANK_NONE_SNAPSHOT_CONTRACT,
        (
            f"expected={RANK_NONE_SNAPSHOT_CONTRACT!r} "
            f"actual={leveled_metrics.get('rank_none_snapshot_contract')!r}"
        ),
    )
    add(
        "store_bytes_reported",
        isinstance(sqlite_metrics.get("store_bytes"), int)
        and sqlite_metrics.get("store_bytes") > 0
        and isinstance(leveled_metrics.get("store_bytes"), int)
        and leveled_metrics.get("store_bytes") > 0,
        f"sqlite={sqlite_metrics.get('store_bytes')!r} leveled={leveled_metrics.get('store_bytes')!r}",
    )
    leveled_root = leveled_metrics.get("root")
    if leveled_root:
        try:
            root_store_bytes = directory_size(pathlib.Path(str(leveled_root)))
        except OSError as exc:
            add(
                "leveled_store_bytes_match_root",
                False,
                f"root={leveled_root!r} error={exc!r}",
            )
        else:
            add(
                "leveled_store_bytes_match_root",
                root_store_bytes == leveled_metrics.get("store_bytes"),
                (
                    f"root={leveled_root!r} computed={root_store_bytes!r} "
                    f"reported={leveled_metrics.get('store_bytes')!r}"
                ),
            )
    return checks


def cmd_compare_with(args: argparse.Namespace, provenance_fun) -> int:
    query_threshold = args.threshold
    load_threshold = args.load_threshold if args.load_threshold is not None else args.threshold
    require_full_result_us = provenance_fun is compare_provenance_checks
    sqlite_rows = read_result_rows(pathlib.Path(args.sqlite_result))
    leveled_rows = read_result_rows(pathlib.Path(args.leveled_result))
    sqlite_metrics = metric_rows(sqlite_rows)
    leveled_metrics = metric_rows(leveled_rows)
    sqlite_queries_by_text = query_rows(sqlite_rows)
    leveled_queries_by_text = query_rows(leveled_rows)
    if set(sqlite_queries_by_text) != set(leveled_queries_by_text):
        raise ValueError(
            "query sets differ: "
            f"missing_leveled={sorted(set(sqlite_queries_by_text) - set(leveled_queries_by_text))!r} "
            f"extra_leveled={sorted(set(leveled_queries_by_text) - set(sqlite_queries_by_text))!r}"
    )
    comparisons = []
    all_within = True
    all_full_result_within = True
    all_equivalent = sqlite_metrics.get("docs") == leveled_metrics.get("docs")
    for query, sqlite_row in sqlite_queries_by_text.items():
        leveled_row = leveled_queries_by_text.get(query)
        if not leveled_row:
            all_within = False
            all_equivalent = False
            comparisons.append({"query": query, "error": "missing_leveled_result"})
            continue
        sqlite_med = median(sqlite_row["runs_us"])
        leveled_med = median(leveled_row["runs_us"])
        ratio = None if sqlite_med == 0 else leveled_med / sqlite_med
        within = ratio is not None and ratio <= query_threshold
        sqlite_full_result_us = sqlite_row.get("full_result_us")
        leveled_full_result_us = leveled_row.get("full_result_us")
        full_result_ratio = (
            None
            if not isinstance(sqlite_full_result_us, int)
            or not isinstance(leveled_full_result_us, int)
            or sqlite_full_result_us == 0
            else leveled_full_result_us / sqlite_full_result_us
        )
        full_result_within = full_result_ratio is not None and full_result_ratio <= query_threshold
        equivalent = (
            sqlite_row["count"] == leveled_row["count"]
            and sqlite_row["total_count"] == leveled_row["total_count"]
            and sqlite_row["keys"] == leveled_row["keys"]
            and sqlite_row["full_keys"] == leveled_row["full_keys"]
            and sqlite_row["error"] == "none"
            and leveled_row["error"] == "none"
        )
        all_within = all_within and within
        all_full_result_within = all_full_result_within and (
            (not require_full_result_us) or full_result_within
        )
        all_equivalent = all_equivalent and equivalent
        comparisons.append(
            {
                "query": query,
                "sqlite_median_us": sqlite_med,
                "leveled_median_us": leveled_med,
                "leveled_to_sqlite_ratio": ratio,
                "within_threshold": within,
                "sqlite_full_result_us": sqlite_full_result_us,
                "leveled_full_result_us": leveled_full_result_us,
                "leveled_full_result_to_sqlite_ratio": full_result_ratio,
                "within_full_result_threshold": full_result_within,
                "equivalent_result": equivalent,
                "sqlite_count": sqlite_row["count"],
                "leveled_count": leveled_row["count"],
                "sqlite_total_count": sqlite_row["total_count"],
                "leveled_total_count": leveled_row["total_count"],
                "full_keys_equal": sqlite_row["full_keys"] == leveled_row["full_keys"],
                "sqlite_full_keys_sha256": keys_sha256(sqlite_row["full_keys"]),
                "leveled_full_keys_sha256": keys_sha256(leveled_row["full_keys"]),
                "sqlite_error": sqlite_row["error"],
                "leveled_error": leveled_row["error"],
                "sqlite_keys": sqlite_row["keys"],
                "leveled_keys": leveled_row["keys"],
            }
        )
    sqlite_load_us = load_metric(sqlite_metrics)
    leveled_load_us = load_metric(leveled_metrics)
    load_ratio = (
        None
        if not isinstance(sqlite_load_us, int)
        or not isinstance(leveled_load_us, int)
        or sqlite_load_us == 0
        else leveled_load_us / sqlite_load_us
    )
    load_within = load_ratio is not None and load_ratio <= load_threshold
    corpus_meta = read_optional_json(args.corpus_meta)
    provenance_checks = provenance_fun(
        sqlite_metrics,
        leveled_metrics,
        corpus_meta,
        args.limit,
    ) + compare_query_contract_checks(
        sqlite_metrics,
        leveled_metrics,
        sqlite_queries_by_text,
        leveled_queries_by_text,
        require_full_result_us=require_full_result_us,
    )
    provenance_ok = all(check["passed"] for check in provenance_checks)
    summary = {
        "harness_schema_version": HARNESS_SCHEMA_VERSION,
        "query_threshold": query_threshold,
        "load_threshold": load_threshold,
        "benchmark_command": " ".join(sys.argv),
        "corpus_meta": corpus_meta,
        "result_paths": {
            "sqlite": str(pathlib.Path(args.sqlite_result)),
            "leveled": str(pathlib.Path(args.leveled_result)),
        },
        "sqlite_metrics": sqlite_metrics,
        "leveled_metrics": leveled_metrics,
        "load_time_metric": "load_reopened_us",
        "sqlite_load_reopened_us": sqlite_load_us,
        "leveled_load_reopened_us": leveled_load_us,
        "leveled_load_to_sqlite_ratio": load_ratio,
        "query_scope": "supplied_queries",
        "supplied_query_count": len(sqlite_queries_by_text),
        "supplied_query_sha256": query_texts_sha256(list(sqlite_queries_by_text)),
        "same_doc_count": sqlite_metrics.get("docs") == leveled_metrics.get("docs"),
        "within_threshold_all_queries": all_within,
        "within_full_result_threshold_all_queries": all_full_result_within,
        "within_threshold_load": load_within,
        "within_provenance_contract": provenance_ok,
        "provenance_checks": provenance_checks,
        "within_threshold_all": all_within and load_within,
        "equivalent_results_all_queries": all_equivalent,
        "equivalent_results_supplied_queries": all_equivalent,
        "benchmark_passed": (
            all_within
            and all_full_result_within
            and load_within
            and all_equivalent
            and provenance_ok
        ),
        "comparisons": comparisons,
    }
    summary_path = pathlib.Path(args.summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    if not getattr(args, "quiet", False):
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["benchmark_passed"] else 1


def compare_provenance_checks(
    sqlite_metrics: dict,
    leveled_metrics: dict,
    corpus_meta=None,
    expected_limit: int = 20,
):
    checks = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    def same_path(*paths) -> bool:
        if not paths or any(not p for p in paths):
            return False
        try:
            resolved = [pathlib.Path(str(p)).resolve() for p in paths]
        except OSError:
            return False
        return all(p == resolved[0] for p in resolved)

    sqlite_source_version_value = sqlite_metrics.get("sqlite_source_version")
    sqlite_cli_version_value = str(sqlite_metrics.get("sqlite_cli_version", ""))
    sqlite_compile_options = str(sqlite_metrics.get("sqlite_compile_options", ""))
    sqlite_ddl = SQLITE_FTS_DDL
    corpus_meta = corpus_meta if isinstance(corpus_meta, dict) else {}
    try:
        actual_tsv_sha256 = file_sha256(pathlib.Path(str(sqlite_metrics.get("tsv", ""))))
    except (OSError, ValueError):
        actual_tsv_sha256 = None
    corpus_tsv_sha256 = corpus_meta.get("tsv_sha256")

    add(
        "same_docs",
        sqlite_metrics.get("docs") == leveled_metrics.get("docs"),
        f"sqlite={sqlite_metrics.get('docs')!r} leveled={leveled_metrics.get('docs')!r}",
    )
    add(
        "same_text_bytes",
        sqlite_metrics.get("text_bytes") == leveled_metrics.get("text_bytes"),
        f"sqlite={sqlite_metrics.get('text_bytes')!r} leveled={leveled_metrics.get('text_bytes')!r}",
    )
    add(
        "same_tsv",
        bool(sqlite_metrics.get("tsv"))
        and sqlite_metrics.get("tsv") == leveled_metrics.get("tsv"),
        f"sqlite={sqlite_metrics.get('tsv')!r} leveled={leveled_metrics.get('tsv')!r}",
    )
    add(
        "same_tsv_sha256",
        bool(sqlite_metrics.get("tsv_sha256"))
        and sqlite_metrics.get("tsv_sha256") == leveled_metrics.get("tsv_sha256")
        and sqlite_metrics.get("tsv_sha256") == actual_tsv_sha256
        and (not corpus_tsv_sha256 or corpus_tsv_sha256 == actual_tsv_sha256),
        (
            f"sqlite={sqlite_metrics.get('tsv_sha256')!r} "
            f"leveled={leveled_metrics.get('tsv_sha256')!r} "
            f"actual={actual_tsv_sha256!r} meta={corpus_tsv_sha256!r}"
        ),
    )
    add(
        "corpus_meta_docs",
        corpus_meta.get("docs") == sqlite_metrics.get("docs") == leveled_metrics.get("docs"),
        (
            f"meta={corpus_meta.get('docs')!r} sqlite={sqlite_metrics.get('docs')!r} "
            f"leveled={leveled_metrics.get('docs')!r}"
        ),
    )
    add(
        "corpus_meta_text_bytes",
        corpus_meta.get("text_bytes")
        == sqlite_metrics.get("text_bytes")
        == leveled_metrics.get("text_bytes"),
        (
            f"meta={corpus_meta.get('text_bytes')!r} "
            f"sqlite={sqlite_metrics.get('text_bytes')!r} "
            f"leveled={leveled_metrics.get('text_bytes')!r}"
        ),
    )
    add(
        "corpus_meta_tsv",
        same_path(corpus_meta.get("tsv"), sqlite_metrics.get("tsv"), leveled_metrics.get("tsv")),
        (
            f"meta={corpus_meta.get('tsv')!r} sqlite={sqlite_metrics.get('tsv')!r} "
            f"leveled={leveled_metrics.get('tsv')!r}"
        ),
    )
    add(
        "sqlite_source_runner",
        sqlite_metrics.get("sqlite_runner") == "source_amalgamation_c",
        f"sqlite_runner={sqlite_metrics.get('sqlite_runner')!r}",
    )
    add(
        "sqlite_runtime_matches_source",
        sqlite_metrics.get("sqlite_runtime_version") == sqlite_source_version_value,
        (
            f"runtime={sqlite_metrics.get('sqlite_runtime_version')!r} "
            f"source={sqlite_source_version_value!r}"
        ),
    )
    add(
        "sqlite_cli_matches_source",
        bool(sqlite_source_version_value)
        and sqlite_cli_version_value.startswith(str(sqlite_source_version_value)),
        f"cli={sqlite_cli_version_value!r} source={sqlite_source_version_value!r}",
    )
    add(
        "sqlite_has_sourceid",
        bool(sqlite_metrics.get("sqlite_runtime_sourceid")),
        f"sourceid={sqlite_metrics.get('sqlite_runtime_sourceid')!r}",
    )
    add(
        "sqlite_fts5_enabled",
        "ENABLE_FTS5" in sqlite_compile_options.split(","),
        "ENABLE_FTS5 in sqlite_compile_options",
    )
    add(
        "sqlite_threadsafe_recorded",
        any(opt.startswith("THREADSAFE=") for opt in sqlite_compile_options.split(",")),
        "THREADSAFE option in sqlite_compile_options",
    )
    add(
        "sqlite_build_provenance",
        bool(sqlite_metrics.get("sqlite_build_source_sha256"))
        and bool(sqlite_metrics.get("sqlite_helper_sha256"))
        and bool(sqlite_metrics.get("sqlite_compile_command_sha256"))
        and sqlite_metrics.get("sqlite_source_git_dirty") in ("true", "false"),
        (
            f"source_sha={sqlite_metrics.get('sqlite_build_source_sha256')!r} "
            f"helper_sha={sqlite_metrics.get('sqlite_helper_sha256')!r} "
            f"cmd_sha={sqlite_metrics.get('sqlite_compile_command_sha256')!r} "
            f"dirty={sqlite_metrics.get('sqlite_source_git_dirty')!r}"
        ),
    )
    add(
        "sqlite_order_contract",
        sqlite_metrics.get("sqlite_query_order") == "ORDER BY key",
        f"sqlite_query_order={sqlite_metrics.get('sqlite_query_order')!r}",
    )
    add(
        "sqlite_ddl_contract",
        sqlite_metrics.get("sqlite_ddl") == sqlite_ddl,
        f"sqlite_ddl={sqlite_metrics.get('sqlite_ddl')!r}",
    )
    add(
        "sqlite_runtime_pragmas_recorded",
        sqlite_runtime_pragmas_match(sqlite_metrics),
        sqlite_runtime_pragmas_detail(sqlite_metrics),
    )
    add(
        "leveled_source_object_shape_contract",
        leveled_metrics.get("source_object_shape") == "{Title, Body}",
        f"source_object_shape={leveled_metrics.get('source_object_shape')!r}",
    )
    add(
        "leveled_query_order_contract",
        leveled_metrics.get("query_order_contract") == "rank_none_order_by_key",
        f"query_order_contract={leveled_metrics.get('query_order_contract')!r}",
    )
    add(
        "leveled_build_provenance",
        bool(leveled_metrics.get("leveled_source_sha256"))
        and bool(leveled_metrics.get("leveled_ebin_sha256"))
        and bool(leveled_metrics.get("leveled_compile_command_sha256"))
        and bool(leveled_metrics.get("leveled_erlang_version"))
        and leveled_metrics.get("leveled_git_dirty") in ("true", "false"),
        (
            f"source_sha={leveled_metrics.get('leveled_source_sha256')!r} "
            f"ebin_sha={leveled_metrics.get('leveled_ebin_sha256')!r} "
            f"cmd_sha={leveled_metrics.get('leveled_compile_command_sha256')!r} "
            f"erl={leveled_metrics.get('leveled_erlang_version')!r} "
            f"dirty={leveled_metrics.get('leveled_git_dirty')!r}"
        ),
    )
    add(
        "leveled_runtime_config_recorded",
        bool(leveled_metrics.get("sync_strategy"))
        and bool(leveled_metrics.get("compression_method"))
        and bool(leveled_metrics.get("ledger_compression"))
        and bool(leveled_metrics.get("fts_representation_contract")),
        (
            f"sync={leveled_metrics.get('sync_strategy')!r} "
            f"compression={leveled_metrics.get('compression_method')!r} "
            f"ledger={leveled_metrics.get('ledger_compression')!r} "
            f"representation={leveled_metrics.get('fts_representation_contract')!r}"
        ),
    )
    add(
        "finalized_load_recorded",
        isinstance(sqlite_metrics.get("load_finalized_us"), int)
        and isinstance(leveled_metrics.get("load_finalized_us"), int)
        and isinstance(sqlite_metrics.get("load_reopened_us"), int)
        and isinstance(leveled_metrics.get("load_reopened_us"), int)
        and isinstance(sqlite_metrics.get("close_us"), int)
        and isinstance(sqlite_metrics.get("query_reopen_us"), int)
        and isinstance(sqlite_metrics.get("query_close_us"), int)
        and isinstance(leveled_metrics.get("close_us"), int)
        and isinstance(leveled_metrics.get("query_reopen_us"), int)
        and isinstance(leveled_metrics.get("query_close_us"), int),
        (
            f"sqlite_load_finalized={sqlite_metrics.get('load_finalized_us')!r} "
            f"leveled_load_finalized={leveled_metrics.get('load_finalized_us')!r} "
            f"sqlite_load_reopened={sqlite_metrics.get('load_reopened_us')!r} "
            f"leveled_load_reopened={leveled_metrics.get('load_reopened_us')!r} "
            f"sqlite_close={sqlite_metrics.get('close_us')!r} "
            f"sqlite_reopen={sqlite_metrics.get('query_reopen_us')!r} "
            f"sqlite_query_close={sqlite_metrics.get('query_close_us')!r} "
            f"leveled_close={leveled_metrics.get('close_us')!r} "
            f"leveled_reopen={leveled_metrics.get('query_reopen_us')!r} "
            f"leveled_query_close={leveled_metrics.get('query_close_us')!r}"
        ),
    )
    add(
        "same_query_connection_contract",
        sqlite_metrics.get("query_connection_contract")
        == leveled_metrics.get("query_connection_contract")
        == "close after load, reopen before timed queries",
        (
            f"sqlite={sqlite_metrics.get('query_connection_contract')!r} "
            f"leveled={leveled_metrics.get('query_connection_contract')!r}"
        ),
    )
    add(
        "same_load_batch_size",
        isinstance(sqlite_metrics.get("load_batch_size"), int)
        and sqlite_metrics.get("load_batch_size") == leveled_metrics.get("load_batch_size"),
        (
            f"sqlite={sqlite_metrics.get('load_batch_size')!r} "
            f"leveled={leveled_metrics.get('load_batch_size')!r}"
        ),
    )
    add(
        "load_batches_recorded",
        isinstance(sqlite_metrics.get("load_batches"), int)
        and sqlite_metrics.get("load_batches") > 0
        and isinstance(leveled_metrics.get("load_batches"), int)
        and leveled_metrics.get("load_batches") > 0,
        (
            f"sqlite={sqlite_metrics.get('load_batches')!r} "
            f"leveled={leveled_metrics.get('load_batches')!r}"
        ),
    )
    add(
        "load_transaction_contract_recorded",
        bool(sqlite_metrics.get("load_transaction_contract"))
        and bool(leveled_metrics.get("load_transaction_contract")),
        (
            f"sqlite={sqlite_metrics.get('load_transaction_contract')!r} "
            f"leveled={leveled_metrics.get('load_transaction_contract')!r}"
        ),
    )
    leveled_search_opts = str(leveled_metrics.get("search_opts", ""))
    leveled_representation = str(leveled_metrics.get("fts_representation_contract", ""))
    add(
        "leveled_search_contract",
        "rank => none" in leveled_search_opts
        and "columns => [title,body]" in leveled_search_opts
        and "prefixes => [5,11]" in leveled_search_opts
        and f"limit => {expected_limit}" in leveled_search_opts
        and leveled_representation == FTS_REPRESENTATION_CONTRACT,
        (
            f"expected_representation={FTS_REPRESENTATION_CONTRACT!r} "
            f"reported_representation={leveled_representation!r} "
            f"search_opts={leveled_search_opts!r}"
        ),
    )
    expected_prefix_contract = "secondary_index_token_range_scan"
    add(
        "leveled_prefix_execution_contract",
        leveled_metrics.get("prefix_execution_contract") == expected_prefix_contract,
        (
            f"expected={expected_prefix_contract!r} "
            f"actual={leveled_metrics.get('prefix_execution_contract')!r}"
        ),
    )
    add(
        "leveled_payload_visibility_contract",
        leveled_metrics.get("payload_visibility_contract")
        == "no_hidden_objects_payload_in_index_metadata",
        f"payload_visibility_contract={leveled_metrics.get('payload_visibility_contract')!r}",
    )
    add(
        "leveled_rank_none_snapshot_contract",
        leveled_metrics.get("rank_none_snapshot_contract") == RANK_NONE_SNAPSHOT_CONTRACT,
        (
            f"expected={RANK_NONE_SNAPSHOT_CONTRACT!r} "
            f"actual={leveled_metrics.get('rank_none_snapshot_contract')!r}"
        ),
    )
    add(
        "leveled_collapsed_ops_count_reported",
        isinstance(leveled_metrics.get("collapsed_ops_count"), int),
        f"collapsed_ops_count={leveled_metrics.get('collapsed_ops_count')!r}",
    )
    add(
        "store_bytes_reported",
        isinstance(sqlite_metrics.get("store_bytes"), int)
        and sqlite_metrics.get("store_bytes") > 0
        and isinstance(leveled_metrics.get("store_bytes"), int)
        and leveled_metrics.get("store_bytes") > 0,
        f"sqlite={sqlite_metrics.get('store_bytes')!r} leveled={leveled_metrics.get('store_bytes')!r}",
    )
    leveled_root = leveled_metrics.get("root")
    if leveled_root:
        try:
            root_store_bytes = directory_size(pathlib.Path(str(leveled_root)))
        except OSError as exc:
            add(
                "leveled_store_bytes_match_root",
                False,
                f"root={leveled_root!r} error={exc!r}",
            )
        else:
            add(
                "leveled_store_bytes_match_root",
                root_store_bytes == leveled_metrics.get("store_bytes"),
                (
                    f"root={leveled_root!r} computed={root_store_bytes!r} "
                    f"reported={leveled_metrics.get('store_bytes')!r}"
                ),
            )
    return checks


def compare_index_provenance_checks(
    sqlite_metrics: dict,
    leveled_metrics: dict,
    corpus_meta=None,
    expected_limit: int = 20,
):
    checks = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    def same_path(*paths) -> bool:
        if not paths or any(not p for p in paths):
            return False
        try:
            resolved = [pathlib.Path(str(p)).resolve() for p in paths]
        except OSError:
            return False
        return all(p == resolved[0] for p in resolved)

    sqlite_source_version_value = sqlite_metrics.get("sqlite_source_version")
    sqlite_cli_version_value = str(sqlite_metrics.get("sqlite_cli_version", ""))
    sqlite_compile_options = str(sqlite_metrics.get("sqlite_compile_options", ""))
    corpus_meta = corpus_meta if isinstance(corpus_meta, dict) else {}
    try:
        actual_tsv_sha256 = file_sha256(pathlib.Path(str(sqlite_metrics.get("tsv", ""))))
    except (OSError, ValueError):
        actual_tsv_sha256 = None
    corpus_tsv_sha256 = corpus_meta.get("tsv_sha256")

    add(
        "same_docs",
        sqlite_metrics.get("docs") == leveled_metrics.get("docs"),
        f"sqlite={sqlite_metrics.get('docs')!r} leveled={leveled_metrics.get('docs')!r}",
    )
    add(
        "same_text_bytes",
        sqlite_metrics.get("text_bytes") == leveled_metrics.get("text_bytes"),
        f"sqlite={sqlite_metrics.get('text_bytes')!r} leveled={leveled_metrics.get('text_bytes')!r}",
    )
    add(
        "same_tsv",
        bool(sqlite_metrics.get("tsv"))
        and sqlite_metrics.get("tsv") == leveled_metrics.get("tsv"),
        f"sqlite={sqlite_metrics.get('tsv')!r} leveled={leveled_metrics.get('tsv')!r}",
    )
    add(
        "same_tsv_sha256",
        bool(sqlite_metrics.get("tsv_sha256"))
        and sqlite_metrics.get("tsv_sha256") == leveled_metrics.get("tsv_sha256")
        and sqlite_metrics.get("tsv_sha256") == actual_tsv_sha256
        and (not corpus_tsv_sha256 or corpus_tsv_sha256 == actual_tsv_sha256),
        (
            f"sqlite={sqlite_metrics.get('tsv_sha256')!r} "
            f"leveled={leveled_metrics.get('tsv_sha256')!r} "
            f"actual={actual_tsv_sha256!r} meta={corpus_tsv_sha256!r}"
        ),
    )
    add(
        "corpus_meta_docs",
        corpus_meta.get("docs") == sqlite_metrics.get("docs") == leveled_metrics.get("docs"),
        (
            f"meta={corpus_meta.get('docs')!r} sqlite={sqlite_metrics.get('docs')!r} "
            f"leveled={leveled_metrics.get('docs')!r}"
        ),
    )
    add(
        "corpus_meta_text_bytes",
        corpus_meta.get("text_bytes")
        == sqlite_metrics.get("text_bytes")
        == leveled_metrics.get("text_bytes"),
        (
            f"meta={corpus_meta.get('text_bytes')!r} "
            f"sqlite={sqlite_metrics.get('text_bytes')!r} "
            f"leveled={leveled_metrics.get('text_bytes')!r}"
        ),
    )
    add(
        "corpus_meta_tsv",
        same_path(corpus_meta.get("tsv"), sqlite_metrics.get("tsv"), leveled_metrics.get("tsv")),
        (
            f"meta={corpus_meta.get('tsv')!r} sqlite={sqlite_metrics.get('tsv')!r} "
            f"leveled={leveled_metrics.get('tsv')!r}"
        ),
    )
    add(
        "sqlite_source_runner",
        sqlite_metrics.get("sqlite_runner") == "source_amalgamation_c",
        f"sqlite_runner={sqlite_metrics.get('sqlite_runner')!r}",
    )
    add(
        "sqlite_runtime_matches_source",
        sqlite_metrics.get("sqlite_runtime_version") == sqlite_source_version_value,
        (
            f"runtime={sqlite_metrics.get('sqlite_runtime_version')!r} "
            f"source={sqlite_source_version_value!r}"
        ),
    )
    add(
        "sqlite_cli_matches_source",
        bool(sqlite_source_version_value)
        and sqlite_cli_version_value.startswith(str(sqlite_source_version_value)),
        f"cli={sqlite_cli_version_value!r} source={sqlite_source_version_value!r}",
    )
    add(
        "sqlite_has_sourceid",
        bool(sqlite_metrics.get("sqlite_runtime_sourceid")),
        f"sourceid={sqlite_metrics.get('sqlite_runtime_sourceid')!r}",
    )
    add(
        "sqlite_threadsafe_recorded",
        any(opt.startswith("THREADSAFE=") for opt in sqlite_compile_options.split(",")),
        "THREADSAFE option in sqlite_compile_options",
    )
    add(
        "sqlite_build_provenance",
        bool(sqlite_metrics.get("sqlite_build_source_sha256"))
        and bool(sqlite_metrics.get("sqlite_helper_sha256"))
        and bool(sqlite_metrics.get("sqlite_compile_command_sha256"))
        and sqlite_metrics.get("sqlite_source_git_dirty") in ("true", "false"),
        (
            f"source_sha={sqlite_metrics.get('sqlite_build_source_sha256')!r} "
            f"helper_sha={sqlite_metrics.get('sqlite_helper_sha256')!r} "
            f"cmd_sha={sqlite_metrics.get('sqlite_compile_command_sha256')!r} "
            f"dirty={sqlite_metrics.get('sqlite_source_git_dirty')!r}"
        ),
    )
    add(
        "sqlite_order_contract",
        sqlite_metrics.get("sqlite_query_order") == "ORDER BY key",
        f"sqlite_query_order={sqlite_metrics.get('sqlite_query_order')!r}",
    )
    add(
        "sqlite_ddl_contract",
        sqlite_metrics.get("sqlite_ddl") == SQLITE_INDEX_DDL,
        f"sqlite_ddl={sqlite_metrics.get('sqlite_ddl')!r}",
    )
    add(
        "sqlite_index_contract",
        sqlite_metrics.get("benchmark_mode") == "secondary_index"
        and sqlite_metrics.get("index_contract") == SQLITE_INDEX_CONTRACT,
        (
            f"mode={sqlite_metrics.get('benchmark_mode')!r} "
            f"contract={sqlite_metrics.get('index_contract')!r}"
        ),
    )
    add(
        "leveled_index_contract",
        leveled_metrics.get("benchmark_mode") == "secondary_index"
        and leveled_metrics.get("content_object") == "{Title, Body}"
        and leveled_metrics.get("index_contract") == LEVELED_INDEX_CONTRACT,
        (
            f"mode={leveled_metrics.get('benchmark_mode')!r} "
            f"content={leveled_metrics.get('content_object')!r} "
            f"contract={leveled_metrics.get('index_contract')!r}"
        ),
    )
    add(
        "leveled_build_provenance",
        bool(leveled_metrics.get("leveled_source_sha256"))
        and bool(leveled_metrics.get("leveled_ebin_sha256"))
        and bool(leveled_metrics.get("leveled_compile_command_sha256"))
        and bool(leveled_metrics.get("leveled_erlang_version"))
        and leveled_metrics.get("leveled_git_dirty") in ("true", "false"),
        (
            f"source_sha={leveled_metrics.get('leveled_source_sha256')!r} "
            f"ebin_sha={leveled_metrics.get('leveled_ebin_sha256')!r} "
            f"cmd_sha={leveled_metrics.get('leveled_compile_command_sha256')!r} "
            f"erl={leveled_metrics.get('leveled_erlang_version')!r} "
            f"dirty={leveled_metrics.get('leveled_git_dirty')!r}"
        ),
    )
    add(
        "query_limit_recorded",
        isinstance(expected_limit, int) and expected_limit >= 0,
        f"limit={expected_limit!r}",
    )
    add(
        "store_bytes_reported",
        isinstance(sqlite_metrics.get("store_bytes"), int)
        and sqlite_metrics.get("store_bytes") > 0
        and isinstance(leveled_metrics.get("store_bytes"), int)
        and leveled_metrics.get("store_bytes") > 0,
        f"sqlite={sqlite_metrics.get('store_bytes')!r} leveled={leveled_metrics.get('store_bytes')!r}",
    )
    leveled_root = leveled_metrics.get("root")
    if leveled_root:
        try:
            root_store_bytes = directory_size(pathlib.Path(str(leveled_root)))
        except OSError as exc:
            add(
                "leveled_store_bytes_match_root",
                False,
                f"root={leveled_root!r} error={exc!r}",
            )
        else:
            add(
                "leveled_store_bytes_match_root",
                root_store_bytes == leveled_metrics.get("store_bytes"),
                (
                    f"root={leveled_root!r} computed={root_store_bytes!r} "
                    f"reported={leveled_metrics.get('store_bytes')!r}"
                ),
            )
    return checks


def directory_size(path: pathlib.Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            total += (pathlib.Path(root) / name).stat().st_size
    return total


def read_optional_json(path: str):
    if not path:
        return None
    try:
        return json.loads(pathlib.Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return {"error": "unavailable", "path": path}


def write_results(
    path: pathlib.Path,
    engine: str,
    docs: int,
    text_bytes: int,
    load_us: int,
    query_results,
    store_bytes=None,
    metadata=None,
):
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(RESULT_HEADER)
        writer.writerow([engine, "docs", docs, "", "", "", "", "", ""])
        writer.writerow([engine, "text_bytes", text_bytes, "", "", "", "", "", ""])
        writer.writerow([engine, "load_us", load_us, "", "", "", "", "", ""])
        writer.writerow([engine, "load_finalized_us", load_us, "", "", "", "", "", ""])
        writer.writerow([engine, "load_reopened_us", load_us, "", "", "", "", "", ""])
        if store_bytes is not None:
            writer.writerow([engine, "store_bytes", store_bytes, "", "", "", "", "", ""])
        for key, value in sorted((metadata or {}).items()):
            writer.writerow([engine, key, value, "", "", "", "", "", ""])
        for row in query_results:
            writer.writerow(
                [
                    engine,
                    "full_result_us",
                    row.get("full_result_us", ""),
                    row["query"],
                    "",
                    row["total_count"],
                    "",
                    row["error"],
                    json.dumps(row.get("full_keys", row["keys"]), ensure_ascii=False),
                ]
            )
            writer.writerow(
                [
                    engine,
                    "query_us",
                    "",
                    row["query"],
                    row["count"],
                    row["total_count"],
                    ",".join(str(v) for v in row["runs_us"]),
                    row["error"],
                    json.dumps(row["keys"], ensure_ascii=False),
                ]
            )


def read_result_rows(path: pathlib.Path):
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t", restkey="__extra__", restval=None)
        if reader.fieldnames != RESULT_HEADER:
            raise ValueError(f"{path} has invalid result header {reader.fieldnames!r}")
        rows = list(reader)
        for row in rows:
            if row.get("__extra__"):
                raise ValueError(f"{path} has a row with extra fields")
            if any(row.get(field) is None for field in RESULT_HEADER):
                raise ValueError(f"{path} has a row with missing fields")
        return rows


def query_rows(rows):
    full_result_rows = {}
    for row in rows:
        if row["metric"] != "full_result_us":
            continue
        query = row["query"]
        if not query:
            raise ValueError("full_result_us row is missing query text")
        if query in full_result_rows:
            raise ValueError(f"duplicate full_result_us row for {query!r}")
        full_result_rows[query] = {
            "full_result_us": int(row["value"]),
            "total_count": int(row["total_count"] or 0),
            "error": row["error"],
            "full_keys": parse_keys_cell(row.get("keys", "")),
        }
    out = {}
    for row in rows:
        if row["metric"] != "query_us":
            continue
        if "total_count" not in row or row["total_count"] == "":
            raise ValueError(f"query row for {row['query']!r} is missing total_count")
        if row["query"] in out:
            raise ValueError(f"duplicate query row for {row['query']!r}")
        full_result_row = full_result_rows.get(row["query"])
        if full_result_row and full_result_row["total_count"] != int(row["total_count"]):
            raise ValueError(f"total_count mismatch for {row['query']!r}")
        out[row["query"]] = {
            "count": int(row["count"] or 0),
            "total_count": int(row["total_count"]),
            "full_result_us": None if full_result_row is None else full_result_row["full_result_us"],
            "full_keys": [] if full_result_row is None else full_result_row["full_keys"],
            "runs_us": [int(v) for v in row["runs_us"].split(",") if v],
            "error": row["error"],
            "keys": parse_keys_cell(row.get("keys", "")),
        }
    return out


def keys_sha256(keys) -> str:
    metadata = json.dumps(keys, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(metadata).hexdigest()


def load_metric(metrics: dict):
    return metrics.get("load_reopened_us", metrics.get("load_finalized_us", metrics.get("load_us")))


def sqlite_runtime_pragmas_match(metrics: dict) -> bool:
    return (
        metrics.get("sqlite_journal_mode") == "OFF"
        and metrics.get("sqlite_synchronous") == "OFF"
        and metrics.get("sqlite_temp_store") == "MEMORY"
        and metrics.get("sqlite_cache_size") == -200000
    )


def sqlite_runtime_pragmas_detail(metrics: dict) -> str:
    return (
        f"journal={metrics.get('sqlite_journal_mode')!r} "
        f"sync={metrics.get('sqlite_synchronous')!r} "
        f"temp_store={metrics.get('sqlite_temp_store')!r} "
        f"cache_size={metrics.get('sqlite_cache_size')!r}"
    )


def parse_keys_cell(cell: str):
    if not cell:
        return []
    try:
        parsed = json.loads(cell)
    except json.JSONDecodeError as exc:
        raise ValueError(f"keys cell is not a JSON array: {cell!r}") from exc
    if not isinstance(parsed, list) or not all(isinstance(v, str) for v in parsed):
        raise ValueError(f"keys cell is not a JSON string array: {cell!r}")
    return parsed


def metric_rows(rows):
    out = {}
    for row in rows:
        if row["metric"] == "query_us" or row["query"]:
            continue
        if row["metric"] in out:
            raise ValueError(f"duplicate metric row for {row['metric']!r}")
        try:
            out[row["metric"]] = int(row["value"])
        except ValueError:
            out[row["metric"]] = row["value"]
    return out


def ensure_queries(path: pathlib.Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(DEFAULT_QUERIES) + "\n")


def ensure_index_queries(path: pathlib.Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(DEFAULT_INDEX_QUERIES) + "\n")


def ensure_ops_queries(path: pathlib.Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(DEFAULT_OP_QUERIES) + "\n")


def read_queries(path: pathlib.Path):
    ensure_queries(path)
    return read_existing_queries(path)


def read_existing_queries(path: pathlib.Path):
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def query_file_contract(path: pathlib.Path):
    return query_contract(read_queries(path))


def query_contract(queries):
    metadata = "".join(f"{query}\n" for query in queries).encode("utf-8")
    return len(queries), hashlib.sha256(metadata).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
