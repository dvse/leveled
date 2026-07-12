#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    process_flag(priority, high),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_shard_cache_fill_race"]),
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
        ok = put_doc(Bookie, <<"old">>, <<"alpha old">>),
        [<<"old">>] = search(Bookie),
        Tab = fts_cache(),
        Shard = leveled_fts:shard_id(<<"alpha">>),
        CacheKey = {shard, <<"docs">>, {<<"main">>, o}, Shard},
        true = ets:delete(Tab, CacheKey),

        %% Stop the cold query inside load_shard_state/2. Its ledger snapshot
        %% is already fixed, but it has not yet installed the shard cache row.
        1 = erlang:trace_pattern(
            {leveled_fts, load_shard_state, 2},
            true,
            [local]
        ),
        Parent = self(),
        QueryPid = spawn(fun() ->
            process_flag(priority, low),
            receive go -> Parent ! {old_query_result, search(Bookie)} end
        end),
        1 = erlang:trace(QueryPid, true, [call, {tracer, self()}]),
        QueryPid ! go,
        receive
            {trace, QueryPid, call,
                {leveled_fts, load_shard_state, [_Ctx, Shard]}} -> ok
        after 5000 ->
            erlang:error(trace_timeout)
        end,
        true = erlang:suspend_process(QueryPid),
        %% The writer sees no shard row, so advance_shard_cache/3 deliberately
        %% does nothing. The suspended old query will insert its stale row later.
        [] = ets:lookup(Tab, CacheKey),
        ok = put_doc(Bookie, <<"new">>, <<"alpha new">>),
        [] = ets:lookup(Tab, CacheKey),
        true = erlang:resume_process(QueryPid),
        receive
            {old_query_result, [<<"old">>]} -> ok;
            {old_query_result, Other} -> erlang:error({unexpected_old_query, Other})
        after 5000 ->
            erlang:error(old_query_timeout)
        end,
        [{CacheKey, {OldStamp, _ConsSeq, _Bloom, _Deltas}}] = ets:lookup(Tab, CacheKey),
        Actual = search(Bookie),
        Expected = [<<"new">>, <<"old">>],
        ets:delete(Tab, CacheKey),
        ColdActual = search(Bookie),
        io:format(
            "stale_cache_stamp=~p~nexpected=~p~nwarm_actual=~p~ncold_actual=~p~n",
            [OldStamp, Expected, Actual, ColdActual]
        ),
        case {Actual, ColdActual} of
            {[<<"old">>], Expected} ->
                io:format("REPRODUCED: a cold query repopulates stale shard state after the write~n"),
                ok;
            _ ->
                erlang:error({not_reproduced, Actual, ColdActual})
        end
    after
        erlang:trace_pattern({leveled_fts, load_shard_state, 2}, false, [local]),
        leveled_bookie:book_close(Bookie),
        reset(Root)
    end.

put_doc(Bookie, Key, Body) ->
    leveled_bookie:book_put(
        Bookie, <<"docs">>, Key, #{body => Body}, [], o, infinity, false
    ).

search(Bookie) ->
    {async, Run} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, <<"alpha">>,
        #{columns => [body], rank => none, limit => 10,
          result_cache => false}
    ),
    {ok, Hits} = Run(),
    lists:sort([maps:get(key, H) || H <- Hits]).

fts_cache() ->
    [Tab] = [T || T <- ets:all(), ets:info(T, name) =:= fts_dir_cache],
    Tab.

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
