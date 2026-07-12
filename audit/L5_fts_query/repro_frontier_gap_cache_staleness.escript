#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_frontier_gap"]),
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
        ok = put_fts(Bookie, <<"old">>, <<"alpha old">>),
        [<<"old">>] = search(Bookie, false),

        %% Faithfully create the documented caller-death window: SQN 2 is
        %% durable in the inker but its caller never sends PUBLISH.
        {ok, Inker, _Schemas} = gen_server:call(Bookie, {write_refs}, infinity),
        HoleKey = leveled_codec:to_objectkey(<<"plain">>, <<"abandoned">>, o),
        {ok, HoleSQN, _Size} = leveled_inker:ink_put(
            Inker, HoleKey, <<"unacked">>, {[], infinity}, false
        ),
        ok = put_fts(Bookie, <<"new">>, <<"alpha new">>),

        %% The later write's ledger rows are already visible, but its FTS
        %% cache advance is buffered behind the missing SQN. Stamp<=Seq lets
        %% the query trust the old shard row and cache the wrong result.
        DuringGap = search(Bookie, true),
        timer:sleep(5200),
        %% A later publish drives the documented five-second gap skip.
        ok = leveled_bookie:book_put(
            Bookie, <<"plain">>, <<"trigger">>, <<"v">>, [], o, infinity, false
        ),
        CachedAfterGap = search(Bookie, true),
        UncachedAfterGap = search(Bookie, false),
        Expected = [<<"new">>, <<"old">>],
        io:format(
            "abandoned_sqn=~p~nexpected=~p~nduring_gap=~p~n"
            "cached_after_gap=~p~nuncached_after_gap=~p~n",
            [HoleSQN, Expected, DuringGap, CachedAfterGap, UncachedAfterGap]
        ),
        case {DuringGap, CachedAfterGap, UncachedAfterGap} of
            {[<<"old">>], [<<"old">>], Expected} ->
                io:format(
                    "REPRODUCED: frontier buffering serves a pre-write shard, "
                    "and the result cache preserves it after the frontier heals~n"
                ),
                ok;
            Other ->
                erlang:error({not_reproduced, Other})
        end
    after
        leveled_bookie:book_close(Bookie),
        reset(Root)
    end.

put_fts(Bookie, Key, Body) ->
    leveled_bookie:book_put(
        Bookie, <<"docs">>, Key, #{body => Body}, [], o, infinity, false
    ).

search(Bookie, UseResultCache) ->
    {async, Run} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, <<"alpha">>,
        #{columns => [body], rank => none, limit => 10,
          result_cache => UseResultCache}
    ),
    {ok, Hits} = Run(),
    lists:sort([maps:get(key, H) || H <- Hits]).

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
