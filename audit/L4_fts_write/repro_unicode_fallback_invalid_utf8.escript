#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    Root = unique_root("unicode_fallback_invalid_utf8"),
    Index = #{
        bucket => <<"docs">>,
        index => <<"main">>,
        columns => [body],
        tokenizer => unicode61,
        remove_diacritics => 2,
        %% A non-empty custom option selects tokenize_unicode/2 instead of
        %% the fast tokenizer. The underscore is not present in the input.
        tokenchars => <<"_">>
    },
    Opts = [{root_path, Root}, {fts_indexes, [Index]}],
    Malformed = <<"bad", 255, "utf8">>,
    try
        {ok, Bookie} = leveled_bookie:book_start(Opts),
        ok = leveled_bookie:book_put(
            Bookie, <<"docs">>, <<"k">>, #{body => Malformed}, []
        ),
        Actual = #{
            bad => search_keys(Bookie, <<"bad">>),
            utf8 => search_keys(Bookie, <<"utf8">>),
            badutf8 => search_keys(Bookie, <<"badutf8">>)
        },
        SQLite = string:trim(os:cmd(
            "/usr/bin/sqlite3 :memory: \"CREATE VIRTUAL TABLE f USING " ++
            "fts5(x, tokenize='unicode61 remove_diacritics 2 tokenchars _'); " ++
            "INSERT INTO f VALUES(CAST(X'626164FF75746638' AS TEXT)); " ++
            "SELECT count(*) FROM f WHERE f MATCH 'bad'; " ++
            "SELECT count(*) FROM f WHERE f MATCH 'utf8'; " ++
            "SELECT count(*) FROM f WHERE f MATCH 'badutf8';\""
        )),

        io:format("expected_sqlite_counts_bad_utf8_badutf8=~p~n", [SQLite]),
        io:format("actual_leveled_keys=~p~n", [Actual]),

        "1\n1\n0" = SQLite,
        #{bad := [], utf8 := [], badutf8 := [<<"k">>]} = Actual,
        ok = leveled_bookie:book_close(Bookie),
        ok
    after
        cleanup(Root)
    end.

search_keys(Bookie, Query) ->
    {async, Runner} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, Query, #{result_cache => false}
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
