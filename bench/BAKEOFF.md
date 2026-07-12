# FTS layout bake-off — honest verdict (2026-07-12)

Five candidates from three independent design sessions, implemented in
parallel checkouts, measured by me serially on a quiet machine after a
cache-strip round (first-round C2-C5 numbers were invalidated by
library caches added against the no-cache rule; probe: zero
ets:insert/persistent_term sites before measurement).

Basis: 10MB slice, quiescent (same basis as the SQLite baseline),
unranked, medians of 9 runs. Goal: every shape <= 3x SQLite.

| shape | C1 pages | C2 blocks | C3 journal | C4 rows | C5 idxspecs |
|---|---|---|---|---|---|
| "new york" | 1.74 | 5.30 | 4.10 | 5.21 | 30.63 |
| "united states" | 1.89 | 5.61 | 3.40 | 3.24 | 29.31 |
| NEAR | 2.82 | 26.79 | 13.65 | 46.83 | 242.74 |
| comput* | 2.42 | 2.29 | 1.64 | 2.23 | 3.05 |
| history | 1.12 | 1.86 | 1.80 | 1.21 | 0.80 |
| istanbul | 0.41 | 3.36 | 1.71 | 2.12 | 3.16 |
| london | 0.90 | 1.64 | 1.24 | 1.34 | 1.17 |
| research | 0.83 | 1.10 | 1.86 | 1.24 | 0.88 |
| science AND research | 1.47 | 2.35 | 3.62 | 12.67 | 4.18 |
| science NOT history | 2.24 | 8.37 | 8.76 | 19.91 | 5.91 |
| title:history | 1.23 | 17.79 | 11.85 | **0.38** | 0.44 |
| war OR peace | 0.94 | 1.62 | 2.01 | 1.89 | 1.68 |
| zzmissingfts0 | 0.48 | 0.70 | **0.27** | 1.00 | 1.30 |
| **worst** | **2.82 / 0 breaches** | 26.79 / 6 | 13.65 / 6 | 46.83 / 5 | 242.74 / 7 |

## Verdict

C1 (store-direct token pages + gen-stamped doc-major tail + tailsum,
docs/FTS.md) is the only clean sheet and wins decisively. The losers
still taught the winning refinements, each visible as a sub-1x cell:

- C4's binary-searchable exact-key probes: title:history 0.38x —
  membership tests without decode. Adopted into C1's refinement
  (binary-search docid directory in boolean pages).
- C3/C5's key-resident payloads stream well for plain terms
  (history 0.80x) but collapse on anything positional (NEAR 243x for
  C5): positions must live OUT of the boolean read path. Adopted
  (position-plane split).
- C3's presence rows: nomatch 0.27x; C1's tailsum + leveled blooms
  already deliver 0.41-0.48x.

C1's own s100m residuals (NEAR 5.94x/4.01x, title 6.21x, NOT 3.67x
unranked) share the exact mechanism the losers' strengths remedy; the
refinement implementing them is in flight. Dirty-tail (write-per-query)
regime remains C1's known weak axis (7/13 breach at the slice) - the
consolidation-cadence lever, to be sized on the e2e.
