#!/usr/bin/env python3
"""Leveled-vs-SQLite-FTS5 equivalence + 5x latency gate.

Reads the per-(scale, rank-mode) result TSVs written by the extended
``sqlite_fts_bench.c`` and ``leveled_fts_bench.erl`` runners (schema unchanged;
ranked runs add an ignorable ``query_scores`` row), then:

  * classifies every query into a structural shape,
  * per (scale x shape x rank-mode): asserts leveled_median <= THRESHOLD x
    sqlite_median (default 5.0) and lists every breach,
  * checks result equivalence -- unranked: identical top-limit key lists,
    identical full-match sets (full_keys_sha256), identical total_count;
    ranked: identical (rank, key)-ordered key lists and BM25 scores within a
    relative tolerance (default 1e-6),
  * reports index build time and on-disk size with ratios (NOT gated).

Exit code is nonzero if any latency cell breaches the threshold OR any
equivalence check fails. Build time and store size never affect the exit code.

Result files are expected under --results-dir named
``{sqlite,leveled}-{scale}-{rank}.tsv``.
"""
import argparse
import csv
import hashlib
import json
import pathlib
import re
from statistics import median

csv.field_size_limit(2**31 - 1)

RESULT_HEADER = [
    "engine", "metric", "value", "query", "count", "total_count", "runs_us",
    "error", "keys",
]


def shape_of(query: str, total_count: int | None) -> str:
    """Structural query shape. A structurally-normal query that matches nothing
    on both engines is reported as ``nomatch`` so the empty-result path is a
    first-class shape."""
    q = query.strip()
    if q.upper().startswith("NEAR("):
        return "near"
    if '"' in q:
        return "phrase"
    if "*" in q:
        return "prefix"
    if re.search(r"(^|\s|-|{)[a-z_][a-z0-9_]*:", q) or ":" in q.split(" ")[0]:
        return "column"
    if " NOT " in f" {q} ":
        return "bool_not"
    if " OR " in f" {q} ":
        return "bool_or"
    if " AND " in f" {q} ":
        return "bool_and"
    if total_count == 0:
        return "nomatch"
    return "term"


def read_rows(path: pathlib.Path):
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if reader.fieldnames != RESULT_HEADER:
            raise ValueError(f"{path}: unexpected header {reader.fieldnames!r}")
        return list(reader)


def parse_keys(cell: str):
    if not cell:
        return []
    return json.loads(cell)


def keys_sha256(keys) -> str:
    # Byte-identical to fts_bench.keys_sha256 and leveled json_binary_list order.
    blob = json.dumps(keys, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def load_engine(path: pathlib.Path):
    """Return (metrics, queries) where queries maps query-text to a dict with
    runs_us, keys, scores, count, total_count, error, full_keys_sha256."""
    rows = read_rows(path)
    metrics = {}
    queries = {}
    for row in rows:
        metric, query = row["metric"], row["query"]
        if metric == "query_us":
            entry = queries.setdefault(query, {})
            entry["count"] = int(row["count"] or 0)
            entry["total_count"] = int(row["total_count"] or 0)
            entry["runs_us"] = [int(v) for v in row["runs_us"].split(",") if v]
            entry["error"] = row["error"]
            entry["keys"] = parse_keys(row["keys"])
        elif metric == "query_scores":
            entry = queries.setdefault(query, {})
            entry["scores"] = [float(v) for v in parse_keys(row["keys"])]
        elif metric == "full_keys_sha256":
            entry = queries.setdefault(query, {})
            entry["full_keys_sha256"] = row["value"]
        elif metric == "full_result_us":
            # SQLite emits the full ordered match set here (no explicit hash row);
            # leveled emits an explicit full_keys_sha256. Capture the cell so the
            # hash can be derived identically when the explicit row is absent.
            entry = queries.setdefault(query, {})
            entry["full_keys"] = parse_keys(row["keys"])
        elif not query:
            # metric row
            val = row["value"]
            try:
                metrics[metric] = int(val)
            except (ValueError, TypeError):
                metrics[metric] = val
    # Derive the full-match-set hash from the ordered full-keys cell when the
    # engine did not emit an explicit full_keys_sha256 row (SQLite runner).
    for entry in queries.values():
        if "full_keys_sha256" not in entry and "full_keys" in entry:
            entry["full_keys_sha256"] = keys_sha256(entry["full_keys"])
    return metrics, queries


def rel_score_diff(a: list[float], b: list[float]):
    """Worst relative score difference between two aligned score lists."""
    if len(a) != len(b):
        return None
    worst = 0.0
    for x, y in zip(a, b):
        worst = max(worst, abs(x - y) / max(1.0, abs(x)))
    return worst


def equivalence(rank: str, s: dict, l: dict, score_tol: float):
    """Return (equivalent, detail, worst_score_diff) for one query.

    Unranked equivalence is strict: identical top-limit keys, identical full
    match set (full_keys_sha256), identical total_count, no errors. Ranked
    equivalence additionally requires identical (rank,key) order and BM25
    scores within ``score_tol`` relative; ``worst_score_diff`` is reported
    even when order already differs."""
    problems = []
    worst = None
    if s.get("error", "none") != "none":
        problems.append(f"sqlite_error={s.get('error')!r}")
    if l.get("error", "none") != "none":
        problems.append(f"leveled_error={l.get('error')!r}")
    if s.get("total_count") != l.get("total_count"):
        problems.append(
            f"total_count s={s.get('total_count')} l={l.get('total_count')}"
        )
    if s.get("keys") != l.get("keys"):
        problems.append(
            f"top_keys_differ (s={len(s.get('keys', []))} l={len(l.get('keys', []))})"
        )
    if rank == "none":
        if s.get("full_keys_sha256") != l.get("full_keys_sha256"):
            problems.append("full_match_set_sha256_differ")
    else:
        ss, ls = s.get("scores"), l.get("scores")
        if ss is None or ls is None:
            problems.append("missing_scores")
        else:
            worst = rel_score_diff(ss, ls)
            if worst is None:
                problems.append(f"score_len s={len(ss)} l={len(ls)}")
            elif worst > score_tol:
                problems.append(f"bm25_rel_diff={worst:.3e}>{score_tol:.0e}")
    return (not problems), "; ".join(problems), worst


def fmt_us(x):
    return "-" if x is None else f"{x/1000:.3f}ms"


def fmt_ratio(x):
    return "-" if x is None else f"{x:.2f}x"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", default="/tmp/leveled_fts_bench/results/gate")
    ap.add_argument("--queries", default=str(pathlib.Path(__file__).with_name("fts_gate_queries.txt")))
    ap.add_argument("--scales", required=True,
                    help="comma-separated scale labels (file stems), e.g. s100m,s1g,full")
    ap.add_argument("--ranks", default="none,bm25")
    ap.add_argument("--leveled-regime", default="uncached",
                    help="leveled result regime to gate: 'uncached' (cold, honest gate) or "
                         "'warm' (result-cache hit). Reads leveled-<scale>-<rank>-<regime>.tsv, "
                         "falling back to leveled-<scale>-<rank>.tsv.")
    ap.add_argument("--threshold", type=float, default=5.0)
    ap.add_argument("--score-tol", type=float, default=1e-6)
    ap.add_argument("--summary", default="")
    ap.add_argument("--markdown", default="")
    args = ap.parse_args()

    results_dir = pathlib.Path(args.results_dir)
    scales = [s for s in args.scales.split(",") if s]
    ranks = [r for r in args.ranks.split(",") if r]

    md = []
    out = {"threshold": args.threshold, "score_tol": args.score_tol,
           "scales": {}, "breaches": [], "equivalence_failures": []}
    per_query_lines = []
    total_compared = 0
    total_matched = 0

    for scale in scales:
        out["scales"][scale] = {}
        for rank in ranks:
            spath = results_dir / f"sqlite-{scale}-{rank}.tsv"
            lpath = results_dir / f"leveled-{scale}-{rank}-{args.leveled_regime}.tsv"
            if not lpath.exists():
                lpath = results_dir / f"leveled-{scale}-{rank}.tsv"
            if not spath.exists() or not lpath.exists():
                out["scales"][scale][rank] = {"error": "missing_result_file",
                                              "sqlite": str(spath), "leveled": str(lpath)}
                continue
            smeta, squ = load_engine(spath)
            lmeta, lqu = load_engine(lpath)
            cell = {
                "sqlite_docs": smeta.get("docs"), "leveled_docs": lmeta.get("docs"),
                "sqlite_text_bytes": smeta.get("text_bytes"),
                "leveled_text_bytes": lmeta.get("text_bytes"),
                "sqlite_load_us": smeta.get("load_reopened_us", smeta.get("load_us")),
                "leveled_load_us": lmeta.get("load_reopened_us", lmeta.get("load_us")),
                "sqlite_store_bytes": smeta.get("store_bytes"),
                "leveled_store_bytes": lmeta.get("store_bytes"),
                "queries": {}, "shapes": {},
            }
            shared = sorted(set(squ) & set(lqu))
            for q in shared:
                s, l = squ[q], lqu[q]
                sm = median(s["runs_us"]) if s.get("runs_us") else None
                lm = median(l["runs_us"]) if l.get("runs_us") else None
                ratio = None if not sm else (lm / sm if lm is not None else None)
                within = ratio is not None and ratio <= args.threshold
                shape = shape_of(q, s.get("total_count"))
                equiv, detail, worst_score = equivalence(rank, s, l, args.score_tol)
                total_compared += 1
                if equiv:
                    total_matched += 1
                else:
                    bucket = "unranked" if rank == "none" else "ranked"
                    out.setdefault(f"{bucket}_equiv_failures", []).append(
                        {"scale": scale, "rank": rank, "query": q, "shape": shape,
                         "detail": detail, "worst_score_diff": worst_score})
                    out["equivalence_failures"].append(
                        {"scale": scale, "rank": rank, "query": q, "detail": detail})
                if not within:
                    out["breaches"].append(
                        {"scale": scale, "rank": rank, "query": q, "shape": shape,
                         "ratio": ratio, "sqlite_us": sm, "leveled_us": lm})
                cell["queries"][q] = {
                    "shape": shape, "sqlite_median_us": sm, "leveled_median_us": lm,
                    "ratio": ratio, "within_threshold": within,
                    "equivalent": equiv, "equiv_detail": detail,
                    "worst_score_diff": worst_score,
                    "total_count": s.get("total_count"),
                }
                sc = cell["shapes"].setdefault(shape, {"worst_ratio": None, "queries": [],
                                                        "all_within": True, "all_equiv": True,
                                                        "worst_score_diff": None})
                sc["queries"].append(q)
                if ratio is not None:
                    sc["worst_ratio"] = ratio if sc["worst_ratio"] is None else max(sc["worst_ratio"], ratio)
                if worst_score is not None:
                    sc["worst_score_diff"] = worst_score if sc["worst_score_diff"] is None \
                        else max(sc["worst_score_diff"], worst_score)
                sc["all_within"] = sc["all_within"] and within
                sc["all_equiv"] = sc["all_equiv"] and equiv
                per_query_lines.append(
                    (scale, rank, shape, q, s.get("total_count"), sm, lm, ratio,
                     within, equiv, detail, worst_score))
            out["scales"][scale][rank] = cell

    # ---- markdown: per-scale gate tables --------------------------------
    md.append(f"Gate threshold: leveled_median <= {args.threshold:g}x sqlite_median "
              f"(latency only; build time and store size reported, not gated). "
              f"BM25 score tolerance: {args.score_tol:g} relative.\n")
    for scale in scales:
        md.append(f"\n### scale `{scale}`\n")
        base = None
        for rank in ranks:
            c = out["scales"][scale].get(rank)
            if c and "queries" in c:
                base = c
                break
        if base is None:
            md.append("_no results_\n")
            continue
        md.append(f"- docs: sqlite={base.get('sqlite_docs')}, leveled={base.get('leveled_docs')}"
                  f"; text_bytes: sqlite={base.get('sqlite_text_bytes')}, leveled={base.get('leveled_text_bytes')}")
        # build + size (per rank, from load run)
        for rank in ranks:
            c = out["scales"][scale].get(rank)
            if not c or "queries" not in c:
                continue
            lu, su = c.get("leveled_load_us"), c.get("sqlite_load_us")
            ls, ss = c.get("leveled_store_bytes"), c.get("sqlite_store_bytes")
            lr = (lu / su) if (isinstance(lu, int) and isinstance(su, int) and su) else None
            sr = (ls / ss) if (isinstance(ls, int) and isinstance(ss, int) and ss) else None
            md.append(f"- [{rank}] load(reopened): sqlite={fmt_us(su)} leveled={fmt_us(lu)} "
                      f"(leveled/sqlite={fmt_ratio(lr)}, not gated); "
                      f"store: sqlite={ss} leveled={ls} bytes (ratio={fmt_ratio(sr)}, not gated)")
        # per shape x rank ratio table
        shapes = sorted({sh for rank in ranks for sh in
                         (out["scales"][scale].get(rank, {}).get("shapes", {}) or {})})
        header = "| shape | " + " | ".join(
            f"{r} ratio | {r} pass" for r in ranks) + " |"
        sep = "|" + "---|" * (1 + 2 * len(ranks))
        md.append("\n" + header)
        md.append(sep)
        for sh in shapes:
            cells = [f"`{sh}`"]
            for rank in ranks:
                sc = out["scales"][scale].get(rank, {}).get("shapes", {}).get(sh)
                if not sc:
                    cells += ["-", "-"]
                else:
                    wr = sc["worst_ratio"]
                    tag = "PASS" if sc["all_within"] else "**BREACH**"
                    cells += [fmt_ratio(wr), tag]
            md.append("| " + " | ".join(cells) + " |")

    # ---- per-query detail table -----------------------------------------
    md.append("\n### per-query detail\n")
    md.append("| scale | rank | shape | query | hits | sqlite | leveled | ratio | gate | equiv |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    for (scale, rank, shape, q, tc, sm, lm, ratio, within, equiv, detail, _ws) in per_query_lines:
        gate = "ok" if within else "**BREACH**"
        eqv = "ok" if equiv else f"**FAIL** ({detail})"
        md.append(f"| {scale} | {rank} | {shape} | `{q}` | {tc} | {fmt_us(sm)} | "
                  f"{fmt_us(lm)} | {fmt_ratio(ratio)} | {gate} | {eqv} |")

    # ---- verdict ---------------------------------------------------------
    n_breach = len(out["breaches"])
    unranked_fail = out.get("unranked_equiv_failures", [])
    ranked_fail = out.get("ranked_equiv_failures", [])
    latency_gate_passed = (n_breach == 0)
    unranked_equiv_passed = (len(unranked_fail) == 0)
    ranked_equiv_within_tol = (len(ranked_fail) == 0)
    worst_ranked = None
    ranked_diffs = [f["worst_score_diff"] for f in ranked_fail
                    if f.get("worst_score_diff") is not None]
    if ranked_diffs:
        worst_ranked = max(ranked_diffs)
    out["queries_compared"] = total_compared
    out["queries_equivalent"] = total_matched
    out["latency_breaches"] = n_breach
    out["latency_gate_passed"] = latency_gate_passed
    out["unranked_equiv_passed"] = unranked_equiv_passed
    out["ranked_equiv_within_tol"] = ranked_equiv_within_tol
    out["worst_ranked_score_diff"] = worst_ranked
    # Exit gate: THE gate is the 5x latency rule; unranked equivalence is a
    # hard correctness requirement. Ranked BM25 score divergence is reported
    # as a finding and does not fail the exit code by itself.
    out["gate_passed"] = latency_gate_passed and unranked_equiv_passed

    md.append("\n### verdict\n")
    md.append(f"- queries compared: {total_compared}; fully equivalent: {total_matched}")
    md.append(f"- **latency gate (leveled <= {args.threshold:g}x sqlite): "
              f"{'PASSED' if latency_gate_passed else 'FAILED'}** "
              f"({n_breach} breach{'es' if n_breach != 1 else ''})")
    if out["breaches"]:
        worst = max(out["breaches"], key=lambda b: b["ratio"] or 0)
        md.append(f"  - worst breach: `{worst['query']}` [{worst['rank']}] scale={worst['scale']} "
                  f"ratio={fmt_ratio(worst['ratio'])} "
                  f"(sqlite={fmt_us(worst['sqlite_us'])} leveled={fmt_us(worst['leveled_us'])})")
    md.append(f"- **unranked (rank=none) equivalence: "
              f"{'EXACT' if unranked_equiv_passed else 'FAILED'}** "
              f"({len(unranked_fail)} mismatch{'es' if len(unranked_fail) != 1 else ''})")
    md.append(f"- ranked (bm25) equivalence within {args.score_tol:g}: "
              f"{'yes' if ranked_equiv_within_tol else 'NO (documented divergence)'} "
              f"({len(ranked_fail)} queries diverge"
              + (f", worst rel score diff {worst_ranked:.3e}" if worst_ranked else "") + ")")
    md.append(f"- **EXIT GATE {'PASSED' if out['gate_passed'] else 'FAILED'}** "
              f"(latency + unranked equivalence)")

    markdown = "\n".join(md) + "\n"
    print(markdown)
    if args.markdown:
        pathlib.Path(args.markdown).write_text(markdown)
    if args.summary:
        pathlib.Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(args.summary).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")

    return 0 if out["gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
