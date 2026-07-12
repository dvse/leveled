#!/bin/sh
# regen_fts_sqlite_oracle.sh
#
# Regenerates test/fts_sqlite_oracle_corpus.eterm from scratch by running
# every query of every case against a real SQLite FTS5 table.
#
# The corpus is a differential oracle for an Erlang FTS engine whose
# correctness contract is parity with SQLite FTS5's unicode61 tokenizer.
# Every `match` boolean in the output file is the answer sqlite3 itself
# gave; nothing is inferred.
#
# Usage:
#   test/regen_fts_sqlite_oracle.sh
# Environment overrides:
#   SQLITE=/path/to/sqlite3   (default /usr/bin/sqlite3)
#   OUT=/path/to/output.eterm (default: fts_sqlite_oracle_corpus.eterm next to this script)
#
# Method per case:
#   CREATE VIRTUAL TABLE t USING fts5(body, tokenize="unicode61 remove_diacritics R [tokenchars '..'] [separators '..']");
#   INSERT INTO t(body) VALUES (CAST(X'<dochex>' AS TEXT));   -- X'' blob cast: exact bytes, incl. invalid UTF-8/NUL
#   SELECT 'DOC:' || hex(body) FROM t;                        -- verify the bytes actually reached the table
#   SELECT 'Q:' || count(*) FROM t WHERE body MATCH ('"' || CAST(X'<qhex>' AS TEXT) || '"');
# The query is always wrapped in double quotes, i.e. it is an FTS5 string/phrase
# (tokenized by the table's own tokenizer), never bareword syntax.

set -eu
LC_ALL=C
export LC_ALL

SQLITE="${SQLITE:-/usr/bin/sqlite3}"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
OUT="${OUT:-$SCRIPT_DIR/fts_sqlite_oracle_corpus.eterm}"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/fts_oracle.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# --- Preflight: the sqlite3 binary must support FTS5 -------------------------
if ! printf 'CREATE VIRTUAL TABLE fts5check USING fts5(x);\n' | "$SQLITE" :memory: >/dev/null 2>&1; then
    echo "ERROR: $SQLITE does not support FTS5; cannot generate oracle corpus" >&2
    exit 1
fi
SQLITE_VERSION=$("$SQLITE" -version)

# --- Helpers -----------------------------------------------------------------
# hex_to_bytes AABBCC -> "170,187,204" (decimal byte list for an Erlang binary)
hex_to_bytes() {
    printf '%s' "$1" | awk '
        BEGIN { hd = "0123456789ABCDEF" }
        {
            h = $0; out = ""
            for (i = 1; i <= length(h); i += 2) {
                hi = index(hd, substr(h, i, 1)) - 1
                lo = index(hd, substr(h, i + 1, 1)) - 1
                out = out (out == "" ? "" : ",") (hi * 16 + lo)
            }
            print out
        }'
}

# --- Case table ----------------------------------------------------------------
# Format: id|remove_diacritics|tokenchars|separators|dochex|qhex1 qhex2 ...|comment
# dochex/qhex are the exact bytes (uppercase hex) of the document / query term.
# Queries are FTS5 phrase strings; a qhex containing 0x20 is a phrase query.
cat > "$WORK/cases.txt" <<'CASE_TABLE_EOF'
sd_eacute_rd0|0|||636166C3A9|63616665 636166C3A9 434146C389 636166C3A8|doc=café rd=0; qs=[cafe, café, CAFÉ, cafè]
sd_eacute_rd1|1|||636166C3A9|63616665 636166C3A9 636166C3A8 636166|doc=café rd=1; qs=[cafe, café, cafè, caf]
sd_eacute_rd2|2|||636166C3A9|63616665 636166C3A9|doc=café rd=2; qs=[cafe, café]
sd_uuml_rd0|0|||C3BC626572|75626572 C3BC626572|doc=über rd=0; qs=[uber, über]
sd_uuml_rd1|1|||C3BC626572|75626572 C3BC626572 C39C626572 55424552|doc=über rd=1; qs=[uber, über, Über, UBER]
sd_uuml_rd2|2|||C3BC626572|75626572 C3BC626572|doc=über rd=2; qs=[uber, über]
sd_ntilde_rd0|0|||6E69C3B16F|6E696E6F 6E69C3B16F|doc=niño rd=0; qs=[nino, niño]
sd_ntilde_rd1|1|||6E69C3B16F|6E696E6F 6E69C3B16F|doc=niño rd=1; qs=[nino, niño]
sd_ntilde_rd2|2|||6E69C3B16F|6E696E6F|doc=niño rd=2; qs=[nino]
sd_ccedil_rd1|1|||6661C3A7616465|666163616465 6661C3A7616465|doc=façade rd=1; qs=[facade, façade]
md_viet_rd0|0|||6DE1BB9974|6D6F74 6DE1BB9974 6DC3B474 6DE1BB8D74|doc=một (o=U+1ED9, 2 diacritics) rd=0; qs=[mot, một, môt, mọt]
md_viet_rd1|1|||6DE1BB9974|6D6F74 6DE1BB9974 6DC3B474 6DE1BB8D74|doc=một rd=1 (mode1 retains multi-diacritic codepoints); qs=[mot, một, môt, mọt]
md_viet_rd2|2|||6DE1BB9974|6D6F74 6DE1BB9974 6DC3B474 6DE1BB8D74|doc=một rd=2 (mode2 folds); qs=[mot, một, môt, mọt]
md_cacute_rd1|1|||E1B88961|6361 C3A761 E1B88961|doc=ḉa (U+1E09 c+cedilla+acute) rd=1; qs=[ca, ça, ḉa]
md_cacute_rd2|2|||E1B88961|6361 C3A761 E1B88961|doc=ḉa rd=2; qs=[ca, ça, ḉa]
md_ecirc_rd1|1|||E1BB876D|656D E1BB876D C3AA6D E1BAB96D|doc=ệm (U+1EC7 e+circumflex+dot) rd=1; qs=[em, ệm, êm, ẹm]
md_ecirc_rd2|2|||E1BB876D|656D E1BB876D|doc=ệm rd=2; qs=[em, ệm]
gr_alpha_rd0|0|||CEACCEBBCF86CEB1|CEB1CEBBCF86CEB1 CEACCEBBCF86CEB1|doc=άλφα (ά=U+03AC precomposed) rd=0; qs=[αλφα, άλφα]
gr_alpha_rd1|1|||CEACCEBBCF86CEB1|CEB1CEBBCF86CEB1 CEACCEBBCF86CEB1 CE86CE9BCEA6CE91|doc=άλφα rd=1; qs=[αλφα, άλφα, ΆΛΦΑ]
gr_alpha_rd2|2|||CEACCEBBCF86CEB1|CEB1CEBBCF86CEB1 CEACCEBBCF86CEB1|doc=άλφα rd=2; qs=[αλφα, άλφα]
gr_decomp_rd0|0|||CEB1CC81CEBBCF86CEB1|CEB1CEBBCF86CEB1 CEACCEBBCF86CEB1 CEB1CC81CEBBCF86CEB1|doc=α+U+0301(combining)+λφα rd=0; qs=[αλφα, άλφα, α<U+0301>λφα]
gr_decomp_rd1|1|||CEB1CC81CEBBCF86CEB1|CEB1CEBBCF86CEB1 CEACCEBBCF86CEB1 CEB1CC81CEBBCF86CEB1|doc=α+U+0301+λφα rd=1; qs=[αλφα, άλφα, α<U+0301>λφα]
gr_decomp_rd2|2|||CEB1CC81CEBBCF86CEB1|CEB1CEBBCF86CEB1 CEACCEBBCF86CEB1|doc=α+U+0301+λφα rd=2; qs=[αλφα, άλφα]
cy_shorti_rd1|1|||D0B9D0BED0B4|D0B8D0BED0B4 D0B9D0BED0B4|doc=йод (й=U+0439 precomposed) rd=1; qs=[иод, йод]
cy_shorti_rd2|2|||D0B9D0BED0B4|D0B8D0BED0B4 D0B9D0BED0B4|doc=йод rd=2; qs=[иод, йод]
cy_decomp_rd1|1|||D0B8CC86D0BED0B4|D0B8D0BED0B4 D0B9D0BED0B4 D0B8CC86D0BED0B4|doc=и+U+0306(combining breve)+од rd=1; qs=[иод, йод, и<U+0306>од]
cy_yo_rd1|1|||D191D0B6|D0B5D0B6 D191D0B6|doc=ёж (ё=U+0451) rd=1; qs=[еж, ёж]
cy_yo_rd2|2|||D191D0B6|D0B5D0B6 D191D0B6|doc=ёж rd=2; qs=[еж, ёж]
lat_decomp_rd0|0|||65CC8166|6566 C3A966 65CC8166|doc=e+U+0301(combining acute)+f rd=0; qs=[ef, éf, e<U+0301>f]
lat_decomp_rd1|1|||65CC8166|6566 C3A966 65CC8166|doc=e+U+0301+f rd=1; qs=[ef, éf, e<U+0301>f]
lat_decomp_rd2|2|||65CC8166|6566 C3A966|doc=e+U+0301+f rd=2; qs=[ef, éf]
lat_2comb_rd1|1|||6D6FCC82CCA374|6D6F74 6DE1BB9974 6D6FCC82CCA374|doc=m,o,U+0302,U+0323,t (decomposed một) rd=1; qs=[mot, một, mo<U+0302><U+0323>t]
lat_2comb_rd2|2|||6D6FCC82CCA374|6D6F74 6DE1BB9974|doc=decomposed một rd=2; qs=[mot, một]
inv_ff_mid_rd1|1|||6162FF6364|6162 6364 61626364 6162206364|doc=ab<FF>cd rd=1; qs=[ab, cd, abcd, "ab cd"]
inv_ff_mid_rd0|0|||6162FF6364|6162 6364 61626364|doc=ab<FF>cd rd=0; qs=[ab, cd, abcd]
inv_overlong_nul_rd1|1|||6162C0806364|6162 6364 61626364 6162206364|doc=ab<C0 80>cd (overlong NUL) rd=1; qs=[ab, cd, abcd, "ab cd"]
inv_trunc_euro_rd1|1|||6162E2826364|6162 6364 64 61626364|doc=ab<E2 82>cd (truncated 3-byte seq) rd=1; qs=[ab, cd, d, abcd]
inv_lone_cont_rd1|1|||6162806364|6162 6364 61626364|doc=ab<80>cd (lone continuation byte) rd=1; qs=[ab, cd, abcd]
inv_ff_start_rd1|1|||FF6162|6162 62|doc=<FF>ab rd=1; qs=[ab, b]
inv_ff_end_rd1|1|||6162FF|6162 61|doc=ab<FF> rd=1; qs=[ab, a]
inv_ff_ff_rd1|1|||6162FFFF6364|6162 6364 61626364 6162206364|doc=ab<FF FF>cd rd=1; qs=[ab, cd, abcd, "ab cd"]
inv_c3_eats_rd1|1|||61C36263|61 6263 63 616263|doc=a<C3>bc (C3 expects continuation; b is not) rd=1; qs=[a, bc, c, abc]
inv_f0_trunc_rd1|1|||6162F09F926364|6162 6364 64 61626364 6162DF926364|doc=ab<F0 9F 92>cd rd=1 (truncated 4-byte seq decodes to U+07D2, an NKO LETTER, fusing ab+cd into one token); qs=[ab, cd, d, abcd, ab<DF 92>cd]
inv_f0_trunc_sep_rd1|1|||6162F09F92206364|6162 6364 6162DF92|doc=ab<F0 9F 92>SP cd rd=1 (phantom letter U+07D2 glues to left token only); qs=[ab, cd, ab<DF 92>]
inv_surrogate_rd1|1|||6162EDA0806364|6162 6364 61626364 6162206364|doc=ab<ED A0 80>cd (UTF-8-encoded surrogate D800) rd=1; qs=[ab, cd, abcd, "ab cd"]
inv_nul_rd1|1|||6162006364|6162 6364 61626364 6162206364|doc=ab<00>cd (embedded real NUL) rd=1; qs=[ab, cd, abcd, "ab cd"]
inv_overlong_slash_rd1|1|||6162E080AF6364|6162 6364 61626364|doc=ab<E0 80 AF>cd (overlong /) rd=1; qs=[ab, cd, abcd]
inv_trunc_eof_rd1|1|||6162E282|6162 61|doc=ab<E2 82> (truncated at EOF) rd=1; qs=[ab, a]
tc_underscore_rd1|1|_||61625F6364206566|61625F6364 6162 6364 6566|doc="ab_cd ef" tokenchars=_ rd=1; qs=[ab_cd, ab, cd, ef]
tc_underscore_edges_rd1|1|_||5F61622063645F|5F6162 6162 63645F 6364|doc="_ab cd_" tokenchars=_ rd=1; qs=[_ab, ab, cd_, cd]
tc_dot_dash_rd1|1|-.||76312E322D7263332078|76312E322D726333 7631 726333 78|doc="v1.2-rc3 x" tokenchars=-. rd=1; qs=[v1.2-rc3, v1, rc3, x]
tc_inv_left_rd1|1|_||61625FFF6364|61625F 6162 6364 61625F6364|doc=ab_<FF>cd tokenchars=_ rd=1; qs=[ab_, ab, cd, ab_cd]
tc_inv_right_rd1|1|_||6162FF5F6364|6162 5F6364 6364 61625F6364|doc=ab<FF>_cd tokenchars=_ rd=1; qs=[ab, _cd, cd, ab_cd]
sep_x_rd1|1||x|617862|61 62 617862 6162|doc=axb separators=x rd=1; qs=[a, b, axb, ab] (query axb tokenizes to phrase "a b")
sep_x_inv_rd1|1||x|61FF7862|61 62 617862 6162|doc=a<FF>xb separators=x rd=1; qs=[a, b, axb, ab]
sep_digit_rd1|1||0|61306220313032|61 62 613062 313032 3132 31|doc="a0b 102" separators=0 (default token char made separator) rd=1; qs=[a, b, a0b, 102, 12, 1]
tc_sep_combo_rd1|1|_|x|615F6278635F64|615F62 635F64 615F6278635F64 615F6220635F64|doc=a_bxc_d tokenchars=_ separators=x rd=1; qs=[a_b, c_d, a_bxc_d, "a_b c_d"]
tc_dot_num_rd1|1|.||332E3134203135|332E3134 33 3134 3135|doc="3.14 15" tokenchars=. rd=1; qs=[3.14, 3, 14, 15]
tc_dash_diac_rd2|2|-||636166C3A9732D626172|63616665732D626172 636166C3A9732D626172 6361666573 626172|doc=cafés-bar tokenchars=- rd=2; qs=[cafes-bar, cafés-bar, cafes, bar]
tc_dash_diac_rd0|0|-||636166C3A9732D626172|63616665732D626172 636166C3A9732D626172|doc=cafés-bar tokenchars=- rd=0; qs=[cafes-bar, cafés-bar]
case_ete_rd0|0|||C38954C389|C3A974C3A9 657465 C38954C389 C38974C3A9|doc=ÉTÉ rd=0; qs=[été, ete, ÉTÉ, Été]
case_ete_rd1|1|||C38954C389|C3A974C3A9 657465 455445|doc=ÉTÉ rd=1; qs=[été, ete, ETE]
case_mot_upper_rd1|1|||4DE1BB9854|6DE1BB9974 6D6F74 4D4F54|doc=MỘT (Ộ=U+1ED8) rd=1; qs=[một, mot, MOT]
case_mot_upper_rd2|2|||4DE1BB9854|6DE1BB9974 6D6F74|doc=MỘT rd=2; qs=[một, mot]
case_sharps_rd1|1|||73747261C39F65|73747261C39F65 73747261737365 53545241535345|doc=straße (ß=U+00DF) rd=1; qs=[straße, strasse, STRASSE]
case_cap_sharps_rd1|1|||53545241E1BA9E45|73747261C39F65 73747261737365 53545241E1BA9E45|doc=STRAẞE (ẞ=U+1E9E capital sharp s) rd=1; qs=[straße, strasse, STRAẞE]
case_turkish_dotted_rd1|1|||C4B07A6D6972|697A6D6972 C4B07A6D6972 497A6D6972|doc=İzmir (İ=U+0130) rd=1; qs=[izmir, İzmir, Izmir]
case_greek_sigma_rd1|1|||CE9FCE94CE9FCEA3|CEBFCEB4CEBFCF82 CEBFCEB4CEBFCF83 CE9FCE94CE9FCEA3|doc=ΟΔΟΣ rd=1; qs=[οδος(final sigma), οδοσ, ΟΔΟΣ]
phrase_basic_rd1|1|||6F6E652074776F207468726565|6F6E652074776F 74776F207468726565 6F6E65207468726565 74687265652074776F 6F6E652074776F207468726565|doc="one two three" rd=1; qs=["one two", "two three", "one three", "three two", "one two three"]
phrase_tc_rd1|1|-||61622063642D6566206768|63642D6566 61622063642D6566 6162206364 63642D6566206768|doc="ab cd-ef gh" tokenchars=- rd=1; qs=[cd-ef, "ab cd-ef", "ab cd", "cd-ef gh"]
phrase_inv_rd1|1|||6162FF6364206566|6162206364 6364206566 6162206364206566 6162206566|doc=ab<FF>cd ef rd=1; qs=["ab cd", "cd ef", "ab cd ef", "ab ef"]
cat_digits_rd1|1|||6162632031323320613162|313233 613162 616263|doc="abc 123 a1b" rd=1; qs=[123, a1b, abc]
cat_arabic_digits_rd1|1|||D9A1D9A2D9A320616263|D9A1D9A2D9A3 313233 616263|doc=١٢٣ (Arabic-Indic digits, Nd) abc rd=1; qs=[١٢٣, 123, abc]
cat_superscript_rd1|1|||78C2B22079|78C2B2 7832 78 79|doc=x²y (²=U+00B2 category No) rd=1; qs=[x², x2, x, y]
cat_roman_rd1|1|||E285AB20616263|E285AB E285BB 616263|doc=Ⅻ (U+216B Nl) abc rd=1; qs=[Ⅻ, ⅻ, abc]
cat_symbols_rd1|1|||612B6220C2A96320E282AC64|61 62 63 64 612B62|doc="a+b ©c €d" (+ © € are separators?) rd=1; qs=[a, b, c, d, a+b]
cat_cjk_rd1|1|||E4BDA0E5A5BD20E4B896E7958C|E4BDA0E5A5BD E4BDA0 E5A5BDE4B896 E4B896E7958C E4BDA0E5A5BD20E4B896E7958C|doc=你好 世界 rd=1; qs=[你好, 你, 好世, 世界, "你好 世界"]
cat_lone_comb_rd1|1|||6120CC802062|61 62 612062 CC80|doc=a SP U+0300 SP b (lone combining mark) rd=1; qs=[a, b, "a b", <U+0300>]
cat_lone_comb_rd0|0|||6120CC802062|61 62 612062 CC80|doc=a SP U+0300 SP b rd=0; qs=[a, b, "a b", <U+0300>]
cat_zwsp_rd1|1|||6162E2808B6364|6162 6364 61626364 6162206364|doc=ab<U+200B zero-width space, Cf>cd rd=1; qs=[ab, cd, abcd, "ab cd"]
cat_middot_rd1|1|||6CC2B76C2078|6C 6C6C 6CC2B76C 78|doc="l·l x" (·=U+00B7 Po) rd=1; qs=[l, ll, l·l, x]
cat_emoji_rd1|1|||61F09F988062|61 62 6162 612062|doc=a<U+1F600 emoji, So>b rd=1; qs=[a, b, ab, "a b"]
CASE_TABLE_EOF

# --- Run every case against sqlite3, emit .eterm body ------------------------
: > "$WORK/body.eterm"
total=0
first=1

while IFS='|' read -r id rd tc sep dochex qhexes comment; do
    [ -n "$id" ] || continue

    TOK="unicode61 remove_diacritics $rd"
    if [ -n "$tc" ]; then TOK="$TOK tokenchars '$tc'"; fi
    if [ -n "$sep" ]; then TOK="$TOK separators '$sep'"; fi

    {
        echo ".bail on"
        echo "CREATE VIRTUAL TABLE t USING fts5(body, tokenize=\"$TOK\");"
        echo "INSERT INTO t(body) VALUES (CAST(X'$dochex' AS TEXT));"
        echo "SELECT 'DOC:' || hex(body) FROM t;"
        for q in $qhexes; do
            echo "SELECT 'Q:' || count(*) FROM t WHERE body MATCH ('\"' || CAST(X'$q' AS TEXT) || '\"');"
        done
    } > "$WORK/case.sql"

    if ! "$SQLITE" :memory: < "$WORK/case.sql" > "$WORK/case.out" 2> "$WORK/case.err"; then
        echo "ERROR: sqlite3 failed for case $id:" >&2
        cat "$WORK/case.err" >&2
        exit 1
    fi

    gothex=$(sed -n 's/^DOC://p' "$WORK/case.out")
    if [ "$gothex" != "$dochex" ]; then
        echo "ERROR: case $id: doc bytes did not survive insertion (want $dochex, got $gothex)" >&2
        exit 1
    fi

    set -- $qhexes
    nq=$#
    nr=$(grep -c '^Q:' "$WORK/case.out")
    if [ "$nq" -ne "$nr" ]; then
        echo "ERROR: case $id: expected $nq query results, got $nr" >&2
        exit 1
    fi

    if [ "$first" -eq 1 ]; then first=0; else echo " ," >> "$WORK/body.eterm"; fi
    {
        echo " %% $id: $comment"
        echo " #{id => $id,"
        echo "   tokenizer_opts =>"
        echo "       #{remove_diacritics => $rd,"
        if [ -n "$tc" ]; then
            echo "         tokenchars => <<\"$tc\">>,"
        else
            echo "         tokenchars => <<>>,"
        fi
        if [ -n "$sep" ]; then
            echo "         separators => <<\"$sep\">>},"
        else
            echo "         separators => <<>>},"
        fi
        echo "   doc => <<$(hex_to_bytes "$dochex")>>,"
        echo "   queries =>"
        i=0
        for q in $qhexes; do
            i=$((i + 1))
            n=$(grep '^Q:' "$WORK/case.out" | sed -n "${i}s/^Q://p")
            if [ "$n" -gt 0 ]; then m=true; else m=false; fi
            if [ "$i" -eq 1 ]; then pre="       [#{"; else pre="        #{"; fi
            if [ "$i" -eq "$nq" ]; then post="}]}"; else post="},"; fi
            echo "${pre}q => <<$(hex_to_bytes "$q")>>, match => $m$post"
        done
    } >> "$WORK/body.eterm"
    total=$((total + 1))
done < "$WORK/cases.txt"

# --- Assemble the output file -------------------------------------------------
{
    echo "%% fts_sqlite_oracle_corpus.eterm"
    echo "%% Differential-oracle corpus: unicode61 tokenizer parity with SQLite FTS5."
    echo "%% GENERATED FILE - do not edit; regenerate with test/regen_fts_sqlite_oracle.sh"
    echo "%% Oracle binary: sqlite3 $SQLITE_VERSION"
    echo "%%"
    echo "%% Read with file:consult/1 -> {ok, [Cases]} where Cases is this list."
    echo "%% Case shape:"
    echo "%%   #{id => atom(),"
    echo "%%     tokenizer_opts => #{remove_diacritics => 0|1|2,"
    echo "%%                         tokenchars => binary(), separators => binary()},"
    echo "%%     doc => binary(),        %% exact bytes indexed (may be invalid UTF-8)"
    echo "%%     queries => [#{q => binary(), match => boolean()}]}"
    echo "%%"
    echo "%% Every doc was inserted as CAST(X'<hex>' AS TEXT) and byte-verified with"
    echo "%% SELECT hex(body). Every query ran as: body MATCH '\"' || <bytes> || '\"'"
    echo "%% (an FTS5 string/phrase, tokenized by the table's own tokenizer)."
    echo "%% Every match boolean is sqlite3's own answer (count(*) > 0)."
    echo "["
    cat "$WORK/body.eterm"
    echo "]."
} > "$OUT"

echo "OK: wrote $OUT ($total cases) using sqlite3 $SQLITE_VERSION" >&2
