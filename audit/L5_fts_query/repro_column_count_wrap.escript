#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_column_count_wrap"]),
    ok = reset(Root),
    Names = [<<"c", (integer_to_binary(N))/binary>> || N <- lists:seq(0, 255)],
    Schema = #{
        bucket => <<"docs">>, tag => o, index => <<"main">>,
        columns => [#{name => Name, path => [Name]} || Name <- Names],
        tokenizer => unicode61
    },
    {ok, Bookie} = leveled_bookie:book_start([
        {root_path, Root}, {compression_method, none},
        {ledger_compression, none}, {log_level, warning},
        {fts_indexes, [Schema]}
    ]),
    try
        Object = maps:from_list([{Name, <<"needle">>} || Name <- Names]),
        ok = leveled_bookie:book_put(
            Bookie, <<"docs">>, <<"k">>, Object, [], o, infinity, false
        ),
        {async, Run} = leveled_bookie:book_ftssearch(
            Bookie, <<"docs">>, <<"main">>, <<"needle">>,
            #{columns => [<<"c0">>], result_cache => false}
        ),
        Actual = Run(),
        Expected = {ok, [<<"k">>]},
        io:format("columns=256~nexpected=~p~nactual=~p~n", [Expected, Actual]),
        case Actual of
            {error, {invalid_fts_payload, delta_cols, 0, _Bytes, _Decoded}} ->
                io:format("REPRODUCED: the 8-bit column count wrapped to zero~n"),
                ok;
            _ ->
                erlang:error({not_reproduced, Actual})
        end
    after
        leveled_bookie:book_close(Bookie),
        reset(Root)
    end.

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
