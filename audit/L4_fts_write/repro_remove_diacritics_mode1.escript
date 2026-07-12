#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    Root = unique_root("remove_diacritics_mode1"),
    %% U+1ED9 LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW is the
    %% documented SQLite mode-1 exception: mode 1 keeps this multiply-
    %% diacritic precomposed character, while mode 2 normalises it to "o".
    MultiDiacritic = <<16#1ED9/utf8>>,
    GreekTonos = <<16#03AC/utf8>>,
    IndexMode1 = #{
        bucket => <<"mode1">>, index => <<"main1">>, columns => [body],
        tokenizer => unicode61, remove_diacritics => 1
    },
    IndexMode2 = #{
        bucket => <<"mode2">>, index => <<"main2">>, columns => [body],
        tokenizer => unicode61, remove_diacritics => 2
    },
    Opts = [{root_path, Root}, {fts_indexes, [IndexMode1, IndexMode2]}],
    try
        {ok, Bookie} = leveled_bookie:book_start(Opts),
        ok = leveled_bookie:book_put(
            Bookie, <<"mode1">>, <<"k">>, #{body => MultiDiacritic}, []
        ),
        ok = leveled_bookie:book_put(
            Bookie, <<"mode2">>, <<"k">>, #{body => GreekTonos}, []
        ),
        LeveledMode1Keys = search_keys(Bookie, <<"mode1">>, <<"main1">>, <<"o">>),
        LeveledMode2GreekKeys = search_keys(
            Bookie, <<"mode2">>, <<"main2">>, <<16#03B1/utf8>>
        ),
        SQLiteCounts = string:trim(os:cmd(
            "/usr/bin/sqlite3 :memory: \"CREATE VIRTUAL TABLE f USING " ++
            "fts5(x, tokenize='unicode61 remove_diacritics 1'); " ++
            "INSERT INTO f VALUES(char(7897)); " ++
            "SELECT count(*) FROM f WHERE f MATCH 'o'; " ++
            "CREATE VIRTUAL TABLE g USING fts5(x, tokenize='unicode61 remove_diacritics 2'); " ++
            "INSERT INTO g VALUES(char(940)); " ++
            "SELECT count(*) FROM g WHERE g MATCH char(945);\""
        )),

        io:format("expected_sqlite_counts_mode1_o_mode2_greek_alpha=~p~n", [
            SQLiteCounts
        ]),
        io:format("actual_leveled_mode1_o_keys=~p~n", [LeveledMode1Keys]),
        io:format("actual_leveled_mode2_greek_alpha_keys=~p~n", [
            LeveledMode2GreekKeys
        ]),

        "0\n0" = SQLiteCounts,
        [<<"k">>] = LeveledMode1Keys,
        [<<"k">>] = LeveledMode2GreekKeys,
        ok = leveled_bookie:book_close(Bookie),
        ok
    after
        cleanup(Root)
    end.

search_keys(Bookie, Bucket, Index, Query) ->
    {async, Runner} = leveled_bookie:book_ftssearch(
        Bookie, Bucket, Index, Query, #{result_cache => false}
    ),
    {ok, Hits} = Runner(),
    lists:sort([maps:get(key, Hit) || Hit <- Hits]).

unique_root(Suffix) ->
    filename:join(
        "/tmp",
        "leveled_l4_" ++ Suffix ++ "_" ++
            integer_to_list(erlang:unique_integer([positive, monotonic]))
    ).

cleanup(Root) ->
    case file:del_dir_r(Root) of
        ok -> ok;
        {error, enoent} -> ok
    end.
