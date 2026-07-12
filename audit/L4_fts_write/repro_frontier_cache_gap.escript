#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    Root = unique_root("frontier_cache_gap"),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>, columns => [body]}],
    Opts = [{root_path, Root}, {fts_indexes, Indexes}],
    try
        {ok, Bookie} = leveled_bookie:book_start(Opts),
        ok = leveled_bookie:book_put(
            Bookie, <<"docs">>, <<"seed">>, #{body => <<"common">>}, []
        ),
        [<<"seed">>] = search_keys(Bookie, #{}),

        %% Execute the caller-side FTS RESOLVE and journal phases, then
        %% deliberately abandon the write before {publish_fts, ...}.
        {ok, FtsSeq, NormalIndexes, Inker} =
            gen_server:call(Bookie, {fts_put_intent}, infinity),
        AbandonedLK = leveled_codec:to_objectkey(
            <<"docs">>, <<"abandoned">>, o
        ),
        {ok, AbandonedChanges, _Touched} =
            leveled_fts:augment_object_changes(
                [{AbandonedLK, #{body => <<"common">>}, {[], infinity}}],
                NormalIndexes,
                FtsSeq
            ),
        {ok, AbandonedJournalSQN, _} =
            leveled_inker:ink_batchput(Inker, AbandonedChanges, false),

        %% This acknowledged write publishes at the next journal SQN, behind
        %% the abandoned gap. Its ledger rows are installed immediately, but
        %% its shard-cache advance is held behind the absorption frontier.
        ok = leveled_bookie:book_put(
            Bookie, <<"docs">>, <<"acked">>, #{body => <<"common">>}, []
        ),
        Expected = [<<"acked">>, <<"seed">>],
        GapActual = search_keys(Bookie, #{}),

        %% Let the five-second gap timeout elapse, then use a non-FTS write to
        %% drive the frontier. The shard cache becomes current, but the result
        %% cached under the unchanged FTS allocator sequence remains stale.
        timer:sleep(5200),
        ok = leveled_bookie:book_put(
            Bookie, <<"plain">>, <<"trigger">>, trigger, []
        ),
        CachedAfterFrontier = search_keys(Bookie, #{}),
        UncachedAfterFrontier = search_keys(Bookie, #{result_cache => false}),

        io:format("expected_after_acked=~p~n", [Expected]),
        io:format("actual_during_gap=~p~n", [GapActual]),
        io:format("actual_after_frontier_cached=~p~n", [CachedAfterFrontier]),
        io:format("actual_after_frontier_uncached=~p~n", [UncachedAfterFrontier]),
        io:format("abandoned_journal_sqn=~p~n", [AbandonedJournalSQN]),

        true = GapActual =/= Expected,
        true = CachedAfterFrontier =/= Expected,
        Expected = UncachedAfterFrontier,

        ok = leveled_bookie:book_close(Bookie),
        {ok, Restarted} = leveled_bookie:book_start(Opts),
        RestartKeys = search_keys(Restarted, #{result_cache => false}),
        io:format("actual_after_restart=~p~n", [RestartKeys]),
        true = lists:member(<<"acked">>, RestartKeys),
        ok = leveled_bookie:book_close(Restarted),
        ok
    after
        cleanup(Root)
    end.

search_keys(Bookie, SearchOpts) ->
    {async, Runner} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, <<"common">>, SearchOpts
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
