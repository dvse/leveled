#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    Root = unique_root("delta_column_count_overflow"),
    Names = [<<"c", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 256)],
    Columns = [#{name => Name, path => [Name]} || Name <- Names],
    Object = maps:from_list([{Name, <<"needle">>} || Name <- Names]),
    Opts = [
        {root_path, Root},
        {fts_indexes, [#{bucket => <<"docs">>, index => <<"main">>, columns => Columns}]}
    ],
    try
        {ok, Bookie} = StartActual = leveled_bookie:book_start(Opts),
        PutActual = leveled_bookie:book_put(
            Bookie, <<"docs">>, <<"k">>, Object, []
        ),
        {ok, Object} = leveled_bookie:book_get(Bookie, <<"docs">>, <<"k">>),
        SearchActual = search(Bookie),

        io:format("expected=start_or_write_rejected_for_unencodable_schema~n", []),
        io:format("actual_start=~p~n", [StartActual]),
        io:format("actual_put=~p~n", [PutActual]),
        io:format("actual_search=~p~n", [SearchActual]),

        ok = PutActual,
        {error, {invalid_fts_payload, delta_cols, 0, _Bytes, []}} = SearchActual,
        ok = leveled_bookie:book_close(Bookie),

        {ok, Restarted} = leveled_bookie:book_start(Opts),
        RestartActual = search(Restarted),
        io:format("actual_search_after_restart=~p~n", [RestartActual]),
        {error, {invalid_fts_payload, delta_cols, 0, _RestartBytes, []}} = RestartActual,
        ok = leveled_bookie:book_close(Restarted),
        ok
    after
        cleanup(Root)
    end.

search(Bookie) ->
    {async, Runner} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, <<"needle">>, #{}
    ),
    Runner().

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
