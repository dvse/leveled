#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

-define(NDOCS, 20000).
-define(NRUNS, 31).

main(_) ->
    application:ensure_all_started(crypto),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_consolidated_slow"]),
    ok = reset(Root),
    Schema = #{
        bucket => <<"docs">>, tag => o, index => <<"main">>,
        columns => [
            #{name => needle, path => [needle]},
            #{name => bulk, path => [bulk]}
        ], tokenizer => unicode61
    },
    {ok, Bookie} = leveled_bookie:book_start([
        {root_path, Root}, {compression_method, none},
        {ledger_compression, none}, {log_level, warning},
        {fts_indexes, [Schema]}
    ]),
    try
        ok = write_entries(Bookie),
        true = erlang:garbage_collect(),
        TargetKey = token(?NDOCS div 2),
        Query = <<"aatarget">>,
        [TargetKey] = search(Bookie, Query),
        Tab = fts_cache(),
        Shard = leveled_fts:shard_id(Query),
        BaseKey = {base, <<"docs">>, {<<"main">>, o}, Shard},
        Before = median([time_query(Bookie, Query, TargetKey) || _ <- lists:seq(1, ?NRUNS)]),

        {async, Consolidate} = leveled_bookie:book_ftsconsolidate(
            Bookie, <<"docs">>, <<"main">>, #{}
        ),
        ok = Consolidate(),
        [TargetKey] = search(Bookie, Query),
        %% Model the known uncached-base regime explicitly. Shard state stays
        %% warm, but each query must fetch and decode the consolidated base.
        ColdTimes = [
            begin
                ets:delete(Tab, BaseKey),
                time_query(Bookie, Query, TargetKey)
            end
         || _ <- lists:seq(1, ?NRUNS)
        ],
        ConsolidatedCold = median(ColdTimes),
        %% With the decoded base retained, the regression disappears; this
        %% isolates the journal fetch/full-base decode as the cause.
        _ = search(Bookie, Query),
        ConsolidatedWarm = median([
            time_query(Bookie, Query, TargetKey) || _ <- lists:seq(1, ?NRUNS)
        ]),
        {ok, BaseBin} = leveled_bookie:book_get(
            Bookie, <<"docs">>, leveled_fts:base_object_key(<<"main">>, Shard), o
        ),
        Ratio = ConsolidatedCold / max(1, Before),
        CachePenalty = ConsolidatedCold / max(1, ConsolidatedWarm),
        io:format(
            "docs=~p runs=~p base_bytes=~p~n"
            "unconsolidated_warm_median_us=~p~n"
            "consolidated_uncached_median_us=~p~n"
            "consolidated_warm_median_us=~p~n"
            "uncached_vs_unconsolidated=~.2fx~n"
            "uncached_vs_decoded_base_cache=~.2fx~n",
            [?NDOCS, ?NRUNS, byte_size(BaseBin), Before, ConsolidatedCold,
             ConsolidatedWarm, Ratio, CachePenalty]
        ),
        case Ratio >= 2.0 andalso CachePenalty >= 5.0 of
            true ->
                io:format("REPRODUCED: uncached consolidated bases are materially slower~n"),
                ok;
            false ->
                erlang:error({not_reproduced, Ratio})
        end
    after
        leveled_bookie:book_close(Bookie),
        reset(Root)
    end.

entry(N) ->
    T = token(N),
    Needle = case N =:= ?NDOCS div 2 of true -> <<"aatarget">>; false -> <<>> end,
    {put, <<"docs">>, T, #{needle => Needle, bulk => T}, [], o, infinity}.

write_entries(Bookie) ->
    Parent = self(),
    {_Pid, Mon} = spawn_monitor(fun() ->
        Entries = [entry(N) || N <- lists:seq(1, ?NDOCS)],
        Parent ! {batch_result, leveled_bookie:book_mput_std(Bookie, Entries, false)}
    end),
    receive
        {batch_result, ok} ->
            receive {'DOWN', Mon, process, _Pid2, normal} -> ok end;
        {batch_result, Other} -> erlang:error({batch_failed, Other});
        {'DOWN', Mon, process, _Pid2, Reason} -> erlang:error({batch_worker_failed, Reason})
    end.

%% Every token begins "aa", so all 20,000 entries occupy one shard/base.
token(N) ->
    NBin = integer_to_binary(N),
    Pad = binary:copy(<<"0">>, 8 - byte_size(NBin)),
    <<"aa", Pad/binary, NBin/binary>>.

search(Bookie, Query) ->
    {async, Run} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, Query,
        #{columns => [needle], rank => none, limit => 10,
          result_cache => false}
    ),
    {ok, Hits} = Run(),
    [maps:get(key, H) || H <- Hits].

time_query(Bookie, Query, TargetKey) ->
    {Micros, [TargetKey]} = timer:tc(fun() -> search(Bookie, Query) end),
    Micros.

median(Values) ->
    Sorted = lists:sort(Values),
    lists:nth((length(Sorted) + 1) div 2, Sorted).

fts_cache() ->
    [Tab] = [T || T <- ets:all(), ets:info(T, name) =:= fts_dir_cache],
    Tab.

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
