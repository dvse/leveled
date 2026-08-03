#!/usr/bin/env python3
"""One entrypoint for Leveled versus SQLite benchmark measurements."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import pathlib
import subprocess
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
BENCH = ROOT / "bench"
SCHEMA_VERSION = 1

# A comparison target is data: adding a driver means adding one entry whose
# binary implements the contract documented in README.md. The orchestrator's
# rung, normalization, comparison, report, and gate paths remain shared.
COMPARISON_DRIVERS = {
    "sqlite": {
        "source": "sqlite_bench.c",
        "artifact": "sqlite_bench",
        "compile": [
            "cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror",
            "{source}", "-lsqlite3", "-lcrypto", "-lm", "-o", "{artifact}",
        ],
        "store_flag": "--db",
        "store_suffix": ".db",
    },
}

STANDING = [
    ("pandemic", "pandemic", "pandemic"),
    ("influenza", "influenza", "influenza"),
    ("hermione", "hermione", "hermione"),
    ("harry_potter", '"harry potter"', '"harry potter"'),
    ("invoice", "invoice", "invoice"),
    ("meeting", "meeting", "meeting"),
    ("theosophy", "theosophy", "theosophy"),
    ("agreement", "agreement", "agreement"),
    ("pandemi_prefix", "pandemi*", "pandemi*"),
    ("purchase_order", '"purchase order"', '"purchase order"'),
    ("thank_you", '"thank you"', '"thank you"'),
    ("invoice_agreement", "invoice agreement", "invoice agreement"),
    ("invoice_or_insurance", "invoice OR insurance", "invoice OR insurance"),
    ("invoice_not_agreement", "invoice NOT agreement", "invoice NOT agreement"),
    ("invoice_prefix", "invoic*", "invoic*"),
    ("invoice_near_payment", "invoice NEAR,10 payment", "NEAR(invoice payment, 10)"),
    ("miss", "xzqnonexistentzzz", "xzqnonexistentzzz"),
]

# The 29 accepted public grammar productions. The engine-only implicit-distance
# NEAR(A B) form is deliberately absent because the public wrapper rejects it.
GRAMMAR = [
    ("g_plain_term", "agreement", "agreement"),
    ("g_implicit_and", "invoice agreement", "invoice agreement"),
    ("g_explicit_and", "invoice AND agreement", "invoice AND agreement"),
    ("g_or", "invoice OR insurance", "invoice OR insurance"),
    ("g_binary_not", "invoice NOT agreement", "invoice NOT agreement"),
    ("g_grouping", "(invoice OR insurance) AND agreement", "(invoice OR insurance) AND agreement"),
    ("g_phrase", '"purchase order"', '"purchase order"'),
    ("g_plus_concat", "purchase + order", "purchase + order"),
    ("g_separator_concat", "purchase-order", "purchase + order"),
    ("g_term_prefix", "invoic*", "invoic*"),
    ("g_phrase_last_prefix", '"purchase ord"*', '"purchase ord" *'),
    ("g_near_function_distance", "NEAR(invoice payment, 3)", "NEAR(invoice payment, 3)"),
    ("g_near_function_item_commas", "NEAR(invoice, payment, 3)", "NEAR(invoice payment, 3)"),
    ("g_near_multi_member", "NEAR(invoice payment agreement, 10)", "NEAR(invoice payment agreement, 10)"),
    ("g_near_infix_default", "invoice NEAR payment", "NEAR(invoice payment, 10)"),
    ("g_near_infix_comma", "invoice NEAR,3 payment", "NEAR(invoice payment, 3)"),
    ("g_near_infix_slash", "invoice NEAR/3 payment", "NEAR(invoice payment, 3)"),
    ("g_lower_near_distance", "invoice near,3 payment", "NEAR(invoice payment, 3)"),
    ("g_mixed_case_near_distance", "invoice Near/3 payment", "NEAR(invoice payment, 3)"),
    ("g_column", "content:invoice", "content:invoice"),
    ("g_column_group", "content:(invoice OR insurance)", "content:(invoice OR insurance)"),
    ("g_column_set", "{content}:invoice", "{content}:invoice"),
    ("g_negative_column", "-content:invoice", "-content:invoice"),
    ("g_negative_column_set", "-{content}:invoice", "-{content}:invoice"),
    ("g_anchor", "^from", "^from"),
    ("g_quoted_column_name", '{"content"}:invoice', '{"content"}:invoice'),
    ("g_quoted_reserved", '"AND"', '"AND"'),
    ("g_lowercase_keyword_term", "and", "and"),
    ("g_precedence", "invoice OR insurance AND agreement", "invoice OR insurance AND agreement"),
]

# The standing 34-query Xapian verification grid is retained verbatim as a
# separately labelled group, even where it overlaps the broader catalog.
GRID = [
    *STANDING,
    ("g_plain_term", "agreement", "agreement"),
    ("g_implicit_and", "invoice agreement", "invoice agreement"),
    ("g_explicit_and", "invoice AND agreement", "invoice AND agreement"),
    ("g_or", "invoice OR insurance", "invoice OR insurance"),
    ("g_binary_not", "invoice NOT agreement", "invoice NOT agreement"),
    ("g_grouping", "(invoice OR insurance) AND agreement", "(invoice OR insurance) AND agreement"),
    ("g_phrase", '"purchase order"', '"purchase order"'),
    ("g_term_prefix", "invoic*", "invoic*"),
    ("g_near_default", "invoice NEAR payment", "NEAR(invoice payment, 10)"),
    ("g_near_function_distance", "NEAR(invoice payment, 3)", "NEAR(invoice payment, 3)"),
    ("g_near_infix_slash", "invoice NEAR/3 payment", "NEAR(invoice payment, 3)"),
    ("f_zero", "theosophy", "theosophy"),
    ("f_rare", "influenza", "influenza"),
    ("f_medium", "invoice", "invoice"),
    ("f_frequent", "agreement", "agreement"),
    ("f_very_frequent", "meeting", "meeting"),
    ("f_stress", "and", "and"),
]

assert len(STANDING) == 17
assert len(GRAMMAR) == 29
assert len(GRID) == 34


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def b64(value: str | bytes) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return base64.b64encode(value).decode("ascii")


def csv_ints(value: str) -> list[int]:
    return [int(part) for part in value.split(",") if part]


def csv_words(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def build_comparison_drivers(
    work: pathlib.Path, targets: list[str]
) -> dict[str, pathlib.Path]:
    unknown = sorted(set(targets) - set(COMPARISON_DRIVERS))
    if unknown:
        raise ValueError(
            f"unknown comparison target(s): {', '.join(unknown)}; "
            f"available: {', '.join(sorted(COMPARISON_DRIVERS))}"
        )
    binaries: dict[str, pathlib.Path] = {}
    for target in targets:
        spec = COMPARISON_DRIVERS[target]
        source = BENCH / spec["source"]
        artifact = work / spec["artifact"]
        subprocess.run(
            [
                token.format(source=source, artifact=artifact)
                for token in spec["compile"]
            ],
            check=True,
        )
        binaries[target] = artifact
    return binaries


def ensure_build(
    work: pathlib.Path, targets: list[str] | None = None
) -> tuple[dict[str, pathlib.Path], dict[str, str]]:
    subprocess.run([str(ROOT / "rebar3"), "compile"], cwd=ROOT, check=True)
    binaries = build_comparison_drivers(work, targets or ["sqlite"])
    env = dict(os.environ)
    paths = [
        ROOT / "_build/default/lib/leveled/ebin",
        ROOT / "_build/default/lib/zstd/ebin",
    ]
    existing = env.get("ERL_FLAGS", "")
    env["ERL_FLAGS"] = " ".join([*(f"-pa {path}" for path in paths), existing])
    return binaries, env


def run_json(command: list[str], *, env: dict[str, str] | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"driver emitted no JSON: {' '.join(command)}")
    return json.loads(lines[-1])


def write_queries(path: pathlib.Path, groups: list[str]) -> int:
    catalogs = {"standing": STANDING, "grammar": GRAMMAR, "grid": GRID}
    rows: list[str] = []
    for group in groups:
        for label, ours, sqlite in catalogs[group]:
            rows.append(
                "\t".join((f"{group}:{label}", group, b64(ours), b64(sqlite)))
            )
    path.write_text("\n".join(rows) + "\n", encoding="ascii")
    return len(rows)


def corpus_rows(path: pathlib.Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as source:
        for ordinal, line in enumerate(source):
            raw = json.loads(line)
            text64 = raw.get("text_b64")
            if text64 is None:
                text64 = b64(raw.get("content", ""))
            rows.append(
                {
                    "key": raw.get("chunk_id", f"chunk-{ordinal:08d}"),
                    "udi": raw.get("udi", raw.get("path", f"doc-{ordinal:08d}")),
                    "version": int(raw.get("content_version", 0)),
                    "text_b64": text64,
                }
            )
    return rows


def prepare_rung(rows: list[dict[str, Any]], percent: int, path: pathlib.Path) -> dict[str, int]:
    count = max(1, len(rows) * percent // 100)
    selected = rows[:count]
    text_bytes = 0
    with path.open("w", encoding="ascii") as output:
        for row in selected:
            text_bytes += len(base64.b64decode(row["text_b64"]))
            output.write(
                "\t".join(
                    (
                        b64(row["key"]), b64(row["udi"]),
                        str(row["version"]), row["text_b64"],
                    )
                )
                + "\n"
            )
    return {"documents": count, "text_bytes": text_bytes}


def flatten_fts_run(result: dict[str, Any], rung: int) -> list[dict[str, Any]]:
    common = {
        "engine": result["engine"],
        "store_config": result.get("store_config", {"name": "target_default"}),
        "rung_percent": rung,
        "rank": result["rank"],
        "regime": result["regime"],
        "limit": result["limit"],
        "documents": result["documents"],
        "text_bytes": result["text_bytes"],
        "store_bytes": result["store_bytes"],
    }
    return [
        {**common, "supported": case.get("supported", True), **case}
        for case in result["cases"]
    ]


def add_marginals(rows: list[dict[str, Any]]) -> None:
    groups: dict[tuple[Any, ...], dict[int, dict[str, Any]]] = {}
    for row in rows:
        if not row.get("supported", True):
            continue
        key = (
            row["engine"], row["rung_percent"], row["rank"], row["regime"],
            row["label"],
        )
        groups.setdefault(key, {})[row["limit"]] = row
    for limits in groups.values():
        if 20 not in limits or 200 not in limits:
            continue
        low, high = limits[20], limits[200]
        extra = high["returned"] - low["returned"]
        marginal = 0.0 if extra <= 0 else (
            high["combined_us"] - low["combined_us"]
        ) / extra
        low["marginal_20_200_us_per_row"] = marginal
        high["marginal_20_200_us_per_row"] = marginal


def fts_command(args: argparse.Namespace) -> int:
    output = pathlib.Path(args.output_dir).resolve()
    work = output / "work"
    work.mkdir(parents=True, exist_ok=True)
    targets = csv_words(args.targets)
    if args.gate_target not in targets:
        raise ValueError("--gate-target must be included in --targets")
    comparison_drivers, env = ensure_build(work, targets)
    query_file = work / "queries.tsv"
    query_count = write_queries(query_file, csv_words(args.query_groups))
    source_rows = corpus_rows(pathlib.Path(args.corpus).resolve())
    ranks = csv_words(args.ranks)
    rungs = csv_ints(args.rungs)
    limits = csv_ints(args.limits)
    regimes = csv_words(args.regimes)
    driver_runs: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    corpus_meta: dict[str, Any] = {}

    for rung in rungs:
        tsv = work / f"corpus_{rung}.tsv"
        corpus_meta[str(rung)] = prepare_rung(source_rows, rung, tsv)
        for target in targets:
            spec = COMPARISON_DRIVERS[target]
            target_store = work / f"{target}_{rung}{spec['store_suffix']}"
            for rank in ranks:
                for limit in limits:
                    result = run_json(
                        [
                            str(comparison_drivers[target]), "fts",
                            spec["store_flag"], str(target_store),
                            "--tsv", str(tsv), "--queries", str(query_file),
                            "--rank", rank, "--limit", str(limit),
                            "--runs", str(args.runs),
                            "--warmups", str(args.warmups),
                        ]
                    )
                    if result["engine"] != target:
                        raise RuntimeError(
                            f"{target} driver identified itself as "
                            f"{result['engine']}"
                        )
                    driver_runs.append({"rung_percent": rung, **result})
                    rows.extend(flatten_fts_run(result, rung))

        leveled_root = work / f"leveled_{rung}"
        built = False
        ordered_regimes = [r for r in regimes if r != "dirty_tail"] + [
            r for r in regimes if r == "dirty_tail"
        ]
        for regime in ordered_regimes:
            for rank in ranks:
                for limit in limits:
                    result = run_json(
                        [
                            "escript", str(BENCH / "leveled_bench.erl"), "fts",
                            "--root", str(leveled_root), "--tsv", str(tsv),
                            "--queries", str(query_file), "--rank", rank,
                            "--regime", regime, "--limit", str(limit),
                            "--runs", str(args.runs), "--warmups", str(args.warmups),
                            "--store-config", args.store_config,
                            "--reuse", "true" if built else "false",
                        ],
                        env=env,
                    )
                    built = True
                    driver_runs.append({"rung_percent": rung, **result})
                    rows.extend(flatten_fts_run(result, rung))

    add_marginals(rows)
    comparison_indexes = {
        target: {
            (row["rung_percent"], row["rank"], row["limit"], row["label"]): row
            for row in rows if row["engine"] == target
        }
        for target in targets
    }
    comparison_rows: list[dict[str, Any]] = []
    for row in rows:
        if row["engine"] != "leveled" or row["regime"] != "served":
            continue
        key = (row["rung_percent"], row["rank"], row["limit"], row["label"])
        for target in targets:
            peer = comparison_indexes[target][key]
            supported = row.get("supported", True) and peer.get("supported", True)
            equivalent = None if not supported else (
                row["total"] == peer["total"]
                and row["result_sha256"] == peer["result_sha256"]
                and row["snippet_rows"] == row["returned"]
                and peer["snippet_rows"] == peer["returned"]
                and row["snippet_content_verified"] == row["returned"]
                and peer["snippet_content_verified"] == peer["returned"]
            )
            ratio = None if not supported or peer["combined_us"] <= 0 else (
                row["combined_us"] / peer["combined_us"]
            )
            gated = target == args.gate_target
            comparison_rows.append(
                {
                    "target": target, "gated": gated,
                    "rung_percent": row["rung_percent"], "rank": row["rank"],
                    "limit": row["limit"], "label": row["label"],
                    "supported": supported, "equivalent": equivalent,
                    "ratio": ratio,
                    "pass": (
                        equivalent is True and ratio is not None
                        and ratio <= args.threshold
                    ) if gated and supported else None,
                }
            )
    gate_rows = [
        row for row in comparison_rows if row["gated"] and row["supported"]
    ]
    passed = sum(1 for row in gate_rows if row["pass"])
    summary = {
        "schema_version": SCHEMA_VERSION,
        "subcommand": "fts",
        "generated_at": utc_now(),
        "configuration": {
            "corpus": str(pathlib.Path(args.corpus).resolve()),
            "rungs_percent": rungs,
            "ranks": ranks,
            "regimes": regimes,
            "limits": limits,
            "runs": args.runs,
            "warmups": args.warmups,
            "threshold_ratio": args.threshold,
            "targets": targets,
            "gate_target": args.gate_target,
            "store_config": args.store_config,
            "query_groups": csv_words(args.query_groups),
            "query_count": query_count,
            "corpus_rungs": corpus_meta,
        },
        "gate": {
            "target": args.gate_target,
            "passed": passed,
            "total": len(gate_rows),
            "all_passed": passed == len(gate_rows),
            "rows": gate_rows,
        },
        "comparisons": comparison_rows,
        "driver_runs": driver_runs,
        "rows": rows,
    }
    write_outputs(output, "fts", summary)
    if args.report_only or summary["gate"]["all_passed"]:
        return 0
    return 1


def simple_command(args: argparse.Namespace) -> int:
    output = pathlib.Path(args.output_dir).resolve()
    work = output / "work"
    work.mkdir(parents=True, exist_ok=True)
    comparison_drivers, env = ensure_build(work, ["sqlite"])
    sqlite_driver = comparison_drivers["sqlite"]
    command = args.subcommand
    common = ["--count", str(args.count)]
    if command == "compression":
        sqlite = run_json(
            [str(sqlite_driver), command, "--db", str(work / "sqlite.db"),
             *common, "--bytes", str(args.bytes)]
        )
        leveled = [
            run_json(
                ["escript", str(BENCH / "leveled_bench.erl"), command,
                 "--root", str(work / f"leveled_{method}"), *common,
                 "--bytes", str(args.bytes), "--compression", method],
                env=env,
            )
            for method in csv_words(args.methods)
        ]
        rows = [sqlite, *leveled]
    else:
        sqlite = run_json(
            [str(sqlite_driver), command, "--db", str(work / "sqlite.db"), *common]
        )
        extra = ["--batches", args.batches] if command == "heads" else []
        leveled = run_json(
            ["escript", str(BENCH / "leveled_bench.erl"), command,
             "--root", str(work / "leveled"), *common, *extra],
            env=env,
        )
        rows = [sqlite, leveled]
    summary = {
        "schema_version": SCHEMA_VERSION,
        "subcommand": command,
        "generated_at": utc_now(),
        "configuration": {
            key: value for key, value in vars(args).items() if key != "func"
        },
        "rows": rows,
    }
    write_outputs(output, command, summary)
    return 0


def markdown(summary: dict[str, Any]) -> str:
    command = summary["subcommand"]
    lines = [f"# `{command}` benchmark", "", f"Generated: {summary['generated_at']}", ""]
    if command == "fts":
        gate = summary["gate"]
        lines.extend(
            [
                f"Gate: **{gate['passed']}/{gate['total']}** at <= "
                f"{summary['configuration']['threshold_ratio']:.2f}x with exact "
                "totals, full-set SHA-256, and snippet presence.",
                "",
                "| rung | engine | config | rank | regime | limit | query | total | returned | snippets | retrieval us | hydration us | combined us | marginal 20->200 us/row |",
                "|---:|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in summary["rows"]:
            lines.append(
                "| {rung_percent} | {engine} | {config} | {rank} | {regime} | {limit} | "
                "{label} | {total} | {returned} | {snippet_rows} | "
                "{retrieval_us} | {hydration_us} | {combined_us} | {marginal:.6f} |".format(
                    marginal=row.get("marginal_20_200_us_per_row", 0.0),
                    config=row["store_config"]["name"],
                    **row
                )
            )
    else:
        lines.extend(["```json", json.dumps(summary["rows"], indent=2, sort_keys=True), "```"])
    return "\n".join(lines) + "\n"


def write_outputs(output: pathlib.Path, command: str, summary: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / f"{command}.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / f"{command}.md").write_text(markdown(summary), encoding="utf-8")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subparsers = result.add_subparsers(dest="subcommand", required=True)

    fts = subparsers.add_parser("fts", help="FTS-2 versus SQLite FTS5")
    fts.add_argument("--corpus", required=True, help="corpus JSONL")
    fts.add_argument("--output-dir", required=True)
    fts.add_argument("--rungs", default="25,50,100")
    fts.add_argument("--ranks", default="none,bm25")
    fts.add_argument("--regimes", default="served,bare,dirty_tail")
    fts.add_argument("--limits", default="20,200")
    fts.add_argument("--query-groups", default="standing,grammar,grid")
    fts.add_argument("--runs", type=int, default=7)
    fts.add_argument("--warmups", type=int, default=3)
    fts.add_argument("--threshold", type=float, default=2.0)
    fts.add_argument("--targets", default="sqlite")
    fts.add_argument("--gate-target", default="sqlite")
    fts.add_argument("--report-only", action="store_true")
    fts.add_argument(
        "--store-config",
        choices=("live", "legacy_uncompressed"),
        default="live",
        help="Leveled store geometry; the governing default mirrors the live host",
    )
    fts.set_defaults(func=fts_command)

    for name, help_text in (
        ("index", "secondary-index operations versus SQLite B-tree"),
        ("heads", "batched head-read scaling"),
        ("compression", "store compression matrix"),
    ):
        child = subparsers.add_parser(name, help=help_text)
        child.add_argument("--output-dir", required=True)
        child.add_argument("--count", type=int, default=10000)
        child.add_argument("--batches", default="1,20,200,1000")
        child.add_argument("--bytes", type=int, default=1024)
        child.add_argument("--methods", default="none,native")
        child.set_defaults(func=simple_command)
    return result


def main() -> int:
    args = parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
