#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_delayed_runner_cache"]),
    ok = reset(Root),
    Schema = #{
        bucket => <<"docs">>, tag => o, index => <<"main">>,
        columns => [#{name => body, path => [body]}], tokenizer => unicode61
    },
    {ok, Bookie} = leveled_bookie:book_start([
        {root_path, Root}, {compression_method, none},
        {ledger_compression, none}, {log_level, warning},
        {fts_indexes, [Schema]}
    ]),
    try
        ok = put_doc(Bookie, <<"old">>),
        [<<"old">>] = run(fresh_runner(Bookie, true)),
        %% The runner has not executed yet. Per book_returnfolder/2, calling
        %% it is what runs the fold over a snapshot of the store.
        DelayedCached = fresh_runner(Bookie, true),
        DelayedUncached = fresh_runner(Bookie, false),
        ok = put_doc(Bookie, <<"new">>),
        CachedActual = run(DelayedCached),
        UncachedControl = run(DelayedUncached),
        FreshActual = run(fresh_runner(Bookie, true)),
        Expected = [<<"new">>, <<"old">>],
        io:format(
            "expected_at_runner_invocation=~p~n"
            "delayed_cached=~p~ndelayed_uncached_control=~p~nfresh=~p~n",
            [Expected, CachedActual, UncachedControl, FreshActual]
        ),
        case {CachedActual, UncachedControl, FreshActual} of
            {[<<"old">>], Expected, Expected} ->
                io:format("REPRODUCED: delayed cached runner is frozen before its snapshot point~n"),
                ok;
            Other ->
                erlang:error({not_reproduced, Other})
        end
    after
        leveled_bookie:book_close(Bookie),
        reset(Root)
    end.

put_doc(Bookie, Key) ->
    leveled_bookie:book_put(
        Bookie, <<"docs">>, Key, #{body => <<"alpha">>}, [], o, infinity, false
    ).

fresh_runner(Bookie, UseCache) ->
    {async, Run} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, <<"alpha">>,
        #{columns => [body], result_cache => UseCache}
    ),
    Run.

run(Runner) ->
    {ok, Hits} = Runner(),
    lists:sort([maps:get(key, H) || H <- Hits]).

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
