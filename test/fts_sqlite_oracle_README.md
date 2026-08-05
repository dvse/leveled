# SQLite FTS5 unicode61 differential-oracle corpus

`fts_sqlite_oracle_corpus.eterm` is a machine-generated oracle corpus for testing
parity between our Erlang FTS engine's tokenizer and SQLite FTS5's `unicode61`
tokenizer. **Every ordered hit identity and per-hit occurrence count is derived
from a real sqlite3 process** — none was copied from the Erlang engine.

- Corpus: `test/fts_sqlite_oracle_corpus.eterm` (82 cases, 267 queries)
- Regeneration script: `test/regen_fts_sqlite_oracle.sh`
- Oracle binary used: `/usr/bin/sqlite3`, version
  `3.53.3 2026-06-26 20:14:12 d4c0e51e4aeb96955b99185ab9cde75c339e2c29c3f3f12428d364a10d78alt1 (64-bit)` (FTS5 enabled)

## File format

Read with `file:consult/1`, which yields `{ok, [Cases]}`:

```erlang
#{id => atom(),
  tokenizer_opts => #{remove_diacritics => 0 | 1 | 2,
                      tokenchars => binary(),     %% <<>> when unset
                      separators => binary()},    %% <<>> when unset
  doc => binary(),                                %% exact indexed bytes; MAY be invalid UTF-8
  queries => [#{q => binary(),
                hits => [{binary(), pos_integer()}]}]}
```

`doc` and `q` are emitted as decimal byte lists (`<<97,98,255,99,100>>`) because
several docs deliberately contain invalid UTF-8. A `%%` comment above each case
gives the human-readable form.

## How each case was generated

For each case the script runs one fresh in-memory sqlite3 session:

```sql
.bail on
CREATE VIRTUAL TABLE t USING fts5(body,
    tokenize="unicode61 remove_diacritics R [tokenchars '..'] [separators '..']");
INSERT INTO t(body) VALUES (CAST(X'<dochex>' AS TEXT));
SELECT 'DOC:' || hex(body) FROM t;   -- byte-exact round-trip check
SELECT count(*), highlight(t, 0, X'01', X'02') FROM t
  WHERE body MATCH ('"' || CAST(X'<qhex>' AS TEXT) || '"');
```

Notes on the mechanism:

- Docs are inserted via `CAST(X'..' AS TEXT)` so that invalid UTF-8 bytes and
  even an embedded NUL (`0x00`) reach the table unmodified. The script verifies
  this per case with `SELECT hex(body)` and aborts on any mismatch. (Verified:
  sqlite 3.43.2 preserves embedded NUL through this cast, and FTS5 keeps
  indexing *past* the NUL.)
- Every query is wrapped in double quotes before being handed to MATCH, i.e. it
  is always an FTS5 *string/phrase*, tokenized by the table's own tokenizer —
  never bareword query syntax. A query containing a space is therefore a phrase
  query. Query bytes are also injected via `CAST(X'..' AS TEXT)` so no shell or
  SQL escaping ever touches them.
- A query that tokenizes to zero tokens (e.g. a lone combining mark) does not
  error in this form; it simply returns `hits => []`.
- The fixture has one document named `<<"doc">>`. SQLite `count(*)` supplies
  the exact hit list/order, and the number of FTS5 highlight start markers
  supplies that hit's quoted-term/phrase occurrence count.

Determinism: the full corpus was generated twice (byte-identical output), and
seven representative cases (`inv_nul_rd1`, `md_viet_rd1`, `md_viet_rd2`,
`sep_x_rd1`, `cat_lone_comb_rd0`, `inv_trunc_euro_rd1`, `case_greek_sigma_rd1`)
were additionally re-run three times standalone with identical results.

## Case classes

| id prefix | count | what it exercises |
|---|---|---|
| `sd_*` | 10 | single-diacritic Latin (é ü ñ ç) under `remove_diacritics` 0/1/2, incl. cross-diacritic query `cafè` vs doc `café` |
| `md_*` | 7 | precomposed Latin with **multiple** combining diacritics (ộ U+1ED9, ḉ U+1E09, ệ U+1EC7): mode 1 retains them, mode 2 folds them |
| `gr_*`, `cy_*` | 11 | Greek (ά U+03AC) and Cyrillic (й U+0439, ё U+0451), precomposed vs decomposed, under all modes |
| `lat_decomp_*`, `lat_2comb_*` | 5 | Latin decomposed combining sequences (e+U+0301; o+U+0302+U+0323), all modes |
| `inv_*` | 15 | invalid UTF-8 as token boundary: 0xFF, overlong (C0 80, E0 80 AF), truncated 3- and 4-byte sequences (mid-doc and at EOF), lone continuation byte, encoded surrogate (ED A0 80), embedded NUL |
| `tc_*`, `sep_*` | 12 | custom `tokenchars` (`_`, `-.`) and `separators` (`x`, `0`), incl. tokenchars at token edges and interaction with invalid bytes and diacritic folding |
| `case_*` | 8 | case folding + diacritics (ÉTÉ, MỘT, ß/ẞ, İ, final sigma Σ/ς) |
| `phrase_*` | 3 | phrase queries across normal, tokenchars, and invalid-byte boundaries |
| `cat_*` | 11 | Unicode category boundaries: digits (ASCII + Arabic-Indic Nd), superscript ² (No), Roman numeral Ⅻ (Nl), symbols (+ © €, So/Sc), CJK, lone combining mark, zero-width space (Cf), emoji (So) |

## Oracle behaviours worth knowing (all verified, all in the corpus)

These are the behaviours an implementation is most likely to get wrong:

1. **Truncated 4-byte sequences can FUSE tokens.** Doc `ab F0 9F 92 cd`
   (`inv_f0_trunc_rd1`) indexes as ONE token: FTS5's UTF-8 reader consumes
   `F0 9F 92`, does not validate the continuation count, and produces U+07D2 —
   an NKO **letter** — so the token is `ab<U+07D2>cd`, re-encoded as
   `61 62 DF 92 63 64`. Queries `ab`, `cd`, `abcd` all MISS; the only hit is the
   valid-UTF-8 query `ab<DF 92>cd`. With a space after the truncated sequence
   (`inv_f0_trunc_sep_rd1`) the phantom letter glues to the left token only.
2. **Truncated 3-byte sequences separate instead.** Doc `ab E2 82 cd` decodes
   the fragment to U+0082 (a control char), so tokens are `ab`, `cd` — the `c`
   is *not* consumed (`inv_trunc_euro_rd1`: query `d` misses, `cd` hits).
   Similarly `a C3 bc` gives tokens `a`, `bc` (`inv_c3_eats_rd1`): the invalid
   lead byte does not swallow the following ASCII byte.
3. **0xFF, overlong encodings (C0 80, E0 80 AF), and encoded surrogates
   (ED A0 80) all act as ordinary separators**; the tokens on either side are
   indexed and adjacent, so the phrase `"ab cd"` matches across them.
4. **Embedded NUL is a separator, not a terminator.** Doc `ab 00 cd`: FTS5
   indexes `cd` after the NUL and `"ab cd"` matches as a phrase (`inv_nul_rd1`).
5. **`remove_diacritics` never touches precomposed non-Latin codepoints, in any
   mode** — Greek ά, Cyrillic й and ё do not fold to α/и/е even at mode 2 —
   **but decomposed combining marks are stripped for every script at modes 1/2**:
   doc `α U+0301 λφα` matches query `αλφα` at mode 1 while doc `άλφα` does not
   (`gr_decomp_rd1` vs `gr_alpha_rd1`; same asymmetry for Cyrillic in
   `cy_decomp_rd1`).
6. **The mode 1 / mode 2 split is exactly the multi-diacritic codepoints.**
   Doc `một` (ộ = U+1ED9): mode 1 query `mot` misses, mode 2 hits. Conversely
   the *decomposed* `m o U+0302 U+0323 t` at mode 1 matches `mot` but NOT `một`
   (`lat_2comb_rd1`) — precomposed and decomposed forms of the same text do not
   match each other at mode 1.
7. **Query-side folding applies too:** at mode 2, queries `một`, `môt`, `mọt`
   all match doc `một`; at mode 1 query `cafè` matches doc `café`.
8. **Case folding is simple (not full):** ß does not become `ss`
   (`strasse` misses `straße`), but ẞ (U+1E9E) folds to ß, Σ and final ς both
   fold to σ, Ⅻ folds to ⅻ, and İ (U+0130) folds so that `izmir`, `Izmir`,
   `İzmir` all match doc `İzmir` at mode 1.
9. **A query whose text contains a separator becomes a phrase and can still
   match.** With `separators 'x'`, query `axb` tokenizes to the phrase
   `[a b]` and MATCHES doc `axb` (`sep_x_rd1`); with `separators '0'`, query
   `102` matches doc `102` as phrase `[1 2]` while query `12` misses
   (`sep_digit_rd1`). Same effect for `l·l` (`cat_middot_rd1`) and `a+b`
   (`cat_symbols_rd1`).
10. **A lone combining mark between spaces produces no token and no position**
    (even at mode 0): phrase `"a b"` matches doc `a <U+0300> b`
    (`cat_lone_comb_rd0/rd1`). Combining marks only extend a token already
    started; at mode 0 they are *kept* inside the token (doc `e U+0301 f`
    matches only its exact decomposed bytes, not `éf`, at mode 0).
11. **Superscript ² (category No) is a token character:** doc `x²` is one token;
    query `x` alone misses (`cat_superscript_rd1`). Zero-width space U+200B (Cf)
    and emoji (So) are separators.
12. **`tokenchars` are honoured at token edges** (`_ab`, `cd_` are the indexed
    tokens — bare `ab`/`cd` miss) and an invalid byte still terminates a
    tokenchars token (`ab_ <FF> cd` indexes `ab_`, `cd`).

## Regeneration

```sh
# defaults: SQLITE=/usr/bin/sqlite3, output next to the script
test/regen_fts_sqlite_oracle.sh

# or explicitly:
SQLITE=/usr/bin/sqlite3 \
OUT=/Users/dvse/projects/agents/leveled/test/fts_sqlite_oracle_corpus.eterm \
  test/regen_fts_sqlite_oracle.sh
```

The script aborts (non-zero exit) if the sqlite3 binary lacks FTS5, if any doc
fails the byte round-trip check, or if any query fails to execute. Output is
deterministic for a given sqlite3 build; regenerating with a *different* SQLite
version may legitimately change `hits` values — the header comment of the
`.eterm` records the exact oracle version used.

Validate the generated file parses:

```sh
export PATH="$HOME/.local/share/mise/shims:$PATH"   # if erl is not on PATH
erl -noshell -eval 'case file:consult("test/fts_sqlite_oracle_corpus.eterm") of
  {ok,[L]} when is_list(L) -> io:format("ok: ~p cases~n",[length(L)]), halt(0);
  E -> io:format("~p~n",[E]), halt(1) end.'
```
