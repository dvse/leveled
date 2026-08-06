-module(fts_SUITE).

-include("leveled.hrl").

-define(ZIPF_POOL, 512).
-define(COARSE_SHARDS, 1).
-define(COARSE_SHARD_BOUND_BYTES, (128 * 1024 * 1024)).
-define(FINE_SHARD_BOUND_BYTES, (128 * 1024 * 1024)).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    search_shapes/1,
    update_delete_visibility/1,
    tail_fold_interleaving/1,
    cross_shard_version_skew/1,
    consolidation_restart_equivalence/1,
    consolidation_write_race/1,
    sqlite_oracle_corpus/1,
    parallel_heap_kill_no_hang/1,
    outer_heap_kill_reports_role/1,
    propagated_kill_reports_inner_role/1,
    coarse_shard_partition_reproduces/1,
    parallel_worker_crash_surfaces/1,
    parallel_drain_clears_down_messages/1,
    full_store_build_memory_equivalence/1
]).

all() ->
    [
        search_shapes,
        update_delete_visibility,
        tail_fold_interleaving,
        cross_shard_version_skew,
        consolidation_restart_equivalence,
        consolidation_write_race,
        sqlite_oracle_corpus,
        parallel_heap_kill_no_hang,
        outer_heap_kill_reports_role,
        propagated_kill_reports_inner_role,
        coarse_shard_partition_reproduces,
        parallel_worker_crash_surfaces,
        parallel_drain_clears_down_messages,
        full_store_build_memory_equivalence
    ].

init_per_suite(Config) ->
    testutil:init_per_suite([{suite, "fts"} | Config]),
    Config.

end_per_suite(Config) ->
    testutil:end_per_suite(Config).

search_shapes(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"shapes">>, [title, body], #{}),
        ok = put_doc(Bookie, Schema, <<"1">>,
            #{title => <<"Alpha">>, body => <<"quick brown fox">>}),
        ok = put_doc(Bookie, Schema, <<"2">>,
            #{title => <<"Beta">>, body => <<"quick blue hare">>}),
        ok = put_doc(Bookie, Schema, <<"3">>,
            #{title => <<"Gamma">>, body => <<"slow brown bear">>}),
        [{<<"1">>, 1}, {<<"2">>, 1}] =
            exact_hits(search(Bookie, Schema, <<"quick">>, #{})),
        [{<<"1">>, 2}] =
            exact_hits(search(Bookie, Schema, <<"quick AND fox">>, #{})),
        [{<<"1">>, 1}, {<<"2">>, 1}, {<<"3">>, 1}] =
            exact_hits(search(Bookie, Schema, <<"quick OR bear">>, #{})),
        [{<<"1">>, 1}] =
            exact_hits(search(Bookie, Schema, <<"quick NOT blue">>, #{})),
        [{<<"1">>, 1}] =
            exact_hits(search(Bookie, Schema, <<"\"quick brown\"">>, #{})),
        [{<<"1">>, 1}, {<<"2">>, 1}] =
            exact_hits(search(Bookie, Schema, <<"qui*">>, #{})),
        [{<<"1">>, 1}] = exact_hits(
            search(Bookie, Schema, <<"NEAR(quick fox, 2)">>, #{})
        ),
        [{<<"2">>, 1}] =
            exact_hits(search(Bookie, Schema, <<"title:beta">>, #{})),
        [] = exact_hits(search(Bookie, Schema, <<"title:quick">>, #{})),
        [Hit] = search(Bookie, Schema, <<"quick">>, #{return_positions => true,
            limit => 1}),
        true = maps:is_key(positions, Hit)
    end).

update_delete_visibility(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"updates">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"1">>, #{body => <<"red green">>}),
        {ok, Manifest1} = leveled_bookie:book_headonly(
            Bookie, <<"updates">>, <<"doc">>, <<"1">>
        ),
        {ok, UpdateSpecs} = leveled_fts:update(
            Schema, <<"1">>, #{body => <<"blue green">>}, Manifest1
        ),
        ok = leveled_bookie:book_mput(Bookie, UpdateSpecs),
        [] = exact_hits(search(Bookie, Schema, <<"red">>, #{})),
        [{<<"1">>, 1}] = exact_hits(
            search(Bookie, Schema, <<"blue">>, #{})
        ),
        {ok, Manifest2} = leveled_bookie:book_headonly(
            Bookie, <<"updates">>, <<"doc">>, <<"1">>
        ),
        ok = leveled_bookie:book_mput(
            Bookie, leveled_fts:remove(Schema, <<"1">>, Manifest2)
        ),
        [] = exact_hits(search(Bookie, Schema, <<"blue">>, #{})),
        [] = exact_hits(search(Bookie, Schema, all_docs, #{}))
    end).

tail_fold_interleaving(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"cache">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"seed">>, #{body => <<"common">>}),
        Gate = atomics:new(1, []),
        Hook = fun({_Shard, _Tail}) ->
            case atomics:exchange(Gate, 1, 1) of
                0 -> put_doc(Bookie, Schema, <<"racer">>, #{body => <<"common">>});
                1 -> ok
            end
        end,
        [{<<"seed">>, 1}] = exact_hits(search(
            Bookie, Schema, <<"common">>, #{tail_fold_hook => Hook}
        )),
        %% The forced interleaving returns the exact pre-write snapshot; the
        %% next store-direct query sees the committed tail.
        [{<<"racer">>, 1}, {<<"seed">>, 1}] = exact_hits(
            search(Bookie, Schema, <<"common">>, #{})
        )
    end).

cross_shard_version_skew(_Config) ->
    %% A multi-shard AND must never assemble two versions of one doc
    %% into a false match (the doc-version stamp, FTS.md §2). Setup:
    %% v1 contains only <<"aa">> (shard 97), v2 only <<"zz">> (shard
    %% 122). Shard 97's tail snapshot is folded BEFORE the update;
    %% shard 122 is folded after it. Without the version stamp the
    %% merge sees aa (v1) + zz (v2) and "aa AND zz" false-matches.
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"skew">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"1">>, #{body => <<"aa">>}),
        Gate = atomics:new(1, []),
        Hook = fun({_Shard, _Tail}) ->
            case atomics:exchange(Gate, 1, 1) of
                0 ->
                    {ok, Manifest} = leveled_bookie:book_headonly(
                        Bookie, <<"skew">>, <<"doc">>, <<"1">>
                    ),
                    {ok, Specs} = leveled_fts:update(
                        Schema, <<"1">>, #{body => <<"zz">>}, Manifest
                    ),
                    ok = leveled_bookie:book_mput(Bookie, Specs);
                1 ->
                    ok
            end
        end,
        [] = exact_hits(search(Bookie, Schema, <<"aa AND zz">>,
            #{tail_fold_hook => Hook})),
        %% post-update state is v2 exactly: zz matches, aa does not
        [{<<"1">>, 1}] = exact_hits(
            search(Bookie, Schema, <<"zz">>, #{})
        ),
        [] = exact_hits(search(Bookie, Schema, <<"aa">>, #{}))
    end).

consolidation_restart_equivalence(_Config) ->
    Root = testutil:reset_filestructure(),
    Opts = start_opts(Root),
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    Schema = schema(<<"consolidate">>, [body], #{}),
    ok = put_doc(Bookie1, Schema, <<"1">>, #{body => <<"alpha alpha beta">>}),
    ok = put_doc(Bookie1, Schema, <<"2">>, #{body => <<"alpha beta beta">>}),
    Before = search(Bookie1, Schema, <<"alpha OR beta">>, #{rank => bm25,
        return_positions => true}),
    [{<<"1">>, 3}, {<<"2">>, 3}] = exact_hits(Before),
    {ok, #{skipped := []}} = leveled_fts:consolidate(Bookie1, Schema, #{}),
    Before = search(Bookie1, Schema, <<"alpha OR beta">>, #{rank => bm25,
        return_positions => true}),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    Before = search(Bookie2, Schema, <<"alpha OR beta">>, #{rank => bm25,
        return_positions => true}),
    {ok, Manifest} = leveled_bookie:book_headonly(
        Bookie2, <<"consolidate">>, <<"doc">>, <<"1">>
    ),
    {ok, UpdateSpecs} = leveled_fts:update(
        Schema, <<"1">>, #{body => <<"gamma beta beta">>}, Manifest
    ),
    ok = leveled_bookie:book_mput(Bookie2, UpdateSpecs),
    Updated = search(Bookie2, Schema, <<"gamma OR beta">>, #{
        rank => bm25, return_positions => true
    }),
    {ok, #{skipped := []}} = leveled_fts:consolidate(Bookie2, Schema, #{}),
    Updated = search(Bookie2, Schema, <<"gamma OR beta">>, #{
        rank => bm25, return_positions => true
    }),
    ok = leveled_bookie:book_destroy(Bookie2).

consolidation_write_race(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"race">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"seed">>, #{body => <<"alpha">>}),
        Gate = atomics:new(1, []),
        Hook = fun({_S, _Conditions}) ->
            case atomics:exchange(Gate, 1, 1) of
                0 -> put_doc(Bookie, Schema, <<"late">>, #{body => <<"apple">>});
                1 -> ok
            end
        end,
        {ok, #{skipped := Skipped, consolidated := []}} =
            leveled_fts:consolidate(Bookie, Schema,
                #{before_consolidate_commit => Hook}),
        true = Skipped =:= lists:seq(0, maps:get(shards, Schema) - 1),
        [{<<"late">>, 1}, {<<"seed">>, 1}] = exact_hits(
            search(Bookie, Schema, <<"alpha OR apple">>, #{})
        )
    end).

sqlite_oracle_corpus(_Config) ->
    LibDir = code:lib_dir(leveled),
    Oracle = filename:absname(filename:join(
        [LibDir, "..", "..", "..", "..", "test", "fts_sqlite_oracle_corpus.eterm"]
    )),
    {ok, [Cases]} = file:consult(Oracle),
    with_bookie(fun(Bookie, _Root) ->
        lists:foreach(fun(Case) -> oracle_case(Bookie, Case) end, Cases)
    end).

parallel_heap_kill_no_hang(_Config) ->
    Parent = self(),
    {Pid, Monitor} = spawn_monitor(fun() ->
        try
            leveled_fts:fts2_build_parallel_for_test(
                inner_worker,
                [allocate],
                1,
                fun(_Item) ->
                    Allocation = lists:seq(1, 1000000),
                    length(Allocation)
                end,
                16384,
                Parent
            )
        of
            Result -> exit({unexpected_parallel_result, Result})
        catch
            error:{fts2_parallel_worker_lost, inner_worker, killed} ->
                exit(heap_kill_surfaced);
            Class:Reason ->
                exit({unexpected_parallel_error, Class, Reason})
        end
    end),
    receive
        {'DOWN', Monitor, process, Pid, heap_kill_surfaced} ->
            ok;
        {'DOWN', Monitor, process, Pid, Reason} ->
            ct:fail({unexpected_heap_kill_result, Reason})
    after 5000 ->
        exit(Pid, kill),
        ct:fail(parallel_heap_kill_timed_out)
    end,
    receive
        {fts2_bound_report, inner_worker, killed} -> ok
    after 1000 ->
        ct:fail(inner_worker_bound_report_missing)
    end,
    receive
        {fts2_bound_report, inner_worker, killed} ->
            ct:fail(duplicate_inner_worker_bound_report)
    after 0 ->
        ok
    end.

outer_heap_kill_reports_role(_Config) ->
    try
        leveled_fts:fts2_outer_bound_for_test(
            fun() ->
                Allocation = lists:seq(1, 1000000),
                length(Allocation)
            end,
            16384,
            self()
        )
    of
        Result -> ct:fail({unexpected_outer_result, Result})
    catch
        error:{fts2_parallel_worker_lost, outer_build, killed} -> ok;
        Class:Reason -> ct:fail({unexpected_outer_error, Class, Reason})
    end,
    receive
        {fts2_build_outer, _Pid} -> ok
    after 1000 ->
        ct:fail(outer_build_observation_missing)
    end,
    receive
        {fts2_bound_report, outer_build, killed} -> ok
    after 1000 ->
        ct:fail(outer_build_bound_report_missing)
    end,
    receive
        {fts2_bound_report, outer_build, killed} ->
            ct:fail(duplicate_outer_build_bound_report)
    after 0 ->
        ok
    end.

%% A worker killed by a propagated signal never breaches its own bound, so the
%% runtime emits no report for it. This is the live silent-kill signature: an
%% outer bound breach kills the linked inner workers. The collector must still
%% raise a role-attributed error and emit exactly one role-attributed report.
propagated_kill_reports_inner_role(_Config) ->
    Parent = self(),
    {Pid, Monitor} = spawn_monitor(fun() ->
        try
            leveled_fts:fts2_build_parallel_for_test(
                inner_worker,
                [propagate],
                1,
                fun(_Item) -> exit(self(), kill) end,
                infinity,
                Parent
            )
        of
            Result -> exit({unexpected_parallel_result, Result})
        catch
            error:{fts2_parallel_worker_lost, inner_worker, killed} ->
                exit(propagated_kill_surfaced);
            Class:Reason ->
                exit({unexpected_parallel_error, Class, Reason})
        end
    end),
    receive
        {'DOWN', Monitor, process, Pid, propagated_kill_surfaced} ->
            ok;
        {'DOWN', Monitor, process, Pid, Reason} ->
            ct:fail({unexpected_propagated_kill_result, Reason})
    after 5000 ->
        exit(Pid, kill),
        ct:fail(propagated_kill_timed_out)
    end,
    receive
        {fts2_bound_report, inner_worker, killed} -> ok
    after 1000 ->
        ct:fail(propagated_kill_report_missing)
    end,
    receive
        {fts2_bound_report, inner_worker, killed} ->
            ct:fail(duplicate_propagated_kill_report)
    after 0 ->
        ok
    end.

parallel_worker_crash_surfaces(_Config) ->
    try
        leveled_fts:fts2_build_parallel_for_test(
            [crash],
            1,
            fun(_Item) -> erlang:error(synthetic_parallel_crash) end,
            infinity
        )
    of
        Result -> ct:fail({unexpected_parallel_result, Result})
    catch
        error:synthetic_parallel_crash -> ok;
        Class:Reason -> ct:fail({unexpected_parallel_error, Class, Reason})
    end.

parallel_drain_clears_down_messages(_Config) ->
    {Pid, Monitor} = spawn_monitor(fun() ->
        Outcome =
            try
                leveled_fts:fts2_build_parallel_for_test(
                    [crash, slow_a, slow_b],
                    3,
                    fun
                        (crash) ->
                            erlang:error(synthetic_parallel_crash);
                        (_Slow) ->
                            timer:sleep(50),
                            1
                    end,
                    infinity
                )
            of
                Result -> {unexpected_parallel_result, Result}
            catch
                error:synthetic_parallel_crash -> crash_surfaced;
                Class:Reason -> {unexpected_parallel_error, Class, Reason}
            end,
        Stray =
            receive
                {'DOWN', _Ref, process, _Worker, _Reason} = Down -> Down
            after 0 ->
                none
            end,
        exit({Outcome, Stray})
    end),
    receive
        {'DOWN', Monitor, process, Pid, {crash_surfaced, none}} ->
            ok;
        {'DOWN', Monitor, process, Pid, Reason} ->
            ct:fail({parallel_drain_failed, Reason})
    after 5000 ->
        exit(Pid, kill),
        ct:fail(parallel_drain_timed_out)
    end.

%% A term's postings cannot be split across build shards, so a Zipfian corpus
%% concentrates most positions in a few terms and therefore in a few shards.
%% The superseded coarse partition breaches its bound on this corpus. The
%% production partition builds the same corpus well inside the same bound.
coarse_shard_partition_reproduces(_Config) ->
    ct:timetrap({minutes, 60}),
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    CoarseRoot = testutil:reset_filestructure(
        "test/test_fts_shard_coarse_" ++ Suffix
    ),
    FineRoot = testutil:reset_filestructure(
        "test/test_fts_shard_fine_" ++ Suffix
    ),
    {ok, Coarse} = leveled_bookie:book_start(start_opts(CoarseRoot)),
    {ok, Fine} = leveled_bookie:book_start(start_opts(FineRoot)),
    Schema = schema(<<"shard-partition">>, [body], #{}),
    Sampler = spawn(fun() -> fts_build_sampler(#{}, #{}, []) end),
    try
        LargeBody = iolist_to_binary([
            <<"alpha beta ">>, zipfian_tokens(large_document, 400000)
        ]),
        full_store_put_corpus([Coarse, Fine], Schema, LargeBody),
        WordSize = erlang:system_info(wordsize),
        CoarseBoundWords = ?COARSE_SHARD_BOUND_BYTES div WordSize,
        Outcome =
            case leveled_fts:fts2_consolidate_with_shards_for_test(
                Coarse,
                Schema,
                #{reclaim => false},
                CoarseBoundWords,
                ?COARSE_SHARDS,
                Sampler
            ) of
                {error, {fts2_parallel_worker_lost, LostRole, killed}} ->
                    {killed, LostRole};
                Result ->
                    {completed, Result}
            end,
        Sampler ! {snapshot, self()},
        CoarsePeaks = fts_build_await_peaks(),
        ct:pal(
            "unpartitioned (~B-shard) build: ~p against a ~B-byte bound; "
            "peaks ~p",
            [
                ?COARSE_SHARDS,
                Outcome,
                ?COARSE_SHARD_BOUND_BYTES,
                fts_build_peak_report(CoarsePeaks, WordSize)
            ]
        ),
        Sampler ! reset,
        FineBoundWords = ?FINE_SHARD_BOUND_BYTES div WordSize,
        {ok, #{skipped := []}} = leveled_fts:fts2_consolidate_with_heap_for_test(
            Fine, Schema, #{reclaim => false}, FineBoundWords, Sampler
        ),
        Sampler ! {snapshot, self()},
        FinePeaks = fts_build_await_peaks(),
        ct:pal(
            "production-partition build against a ~B-byte bound; peaks ~p",
            [?FINE_SHARD_BOUND_BYTES, fts_build_peak_report(FinePeaks, WordSize)]
        ),
        {ok, Index} = leveled_fts:fts2_available(Fine, Schema),
        ct:pal(
            "shard-partition fixture: ~B documents, ~B terms, ~B bigrams, "
            "~B indexed tokens, ~B-byte largest document",
            [
                maps:get(chunk_count, Index),
                maps:get(term_count, Index),
                maps:get(bigram_count, Index),
                maps:get(total_length, Index),
                byte_size(LargeBody)
            ]
        ),
        {_FineRole, FineHeapWords, _FineMemory} = fts_build_inner_peak(FinePeaks),
        true = FineHeapWords < FineBoundWords,
        case Outcome of
            {killed, term_shard_worker} ->
                ok;
            _ ->
                ct:fail({
                    unpartitioned_build_did_not_reproduce,
                    Outcome,
                    coarse_term_shard_words(CoarsePeaks)
                })
        end
    after
        Sampler ! stop,
        lists:foreach(
            fun(Bookie) ->
                try
                    leveled_bookie:book_destroy(Bookie)
                catch
                    _:_ -> ok
                end
            end,
            [Coarse, Fine]
        )
    end.

coarse_term_shard_words(Peaks) ->
    case maps:find(term_shard_worker, Peaks) of
        {ok, {HeapWords, _Memory}} -> HeapWords;
        error -> 0
    end.

fts_build_await_peaks() ->
    receive
        {fts_build_peaks, Peaks} -> Peaks
    after 5000 ->
        ct:fail(fts_build_sampler_timed_out)
    end.

fts_build_peak_report(Peaks, WordSize) ->
    lists:sort([
        {Role, HeapWords, HeapWords * WordSize, MemoryBytes}
     || {Role, {HeapWords, MemoryBytes}} <- maps:to_list(Peaks)
    ]).

full_store_build_memory_equivalence(_Config) ->
    ct:timetrap({minutes, 30}),
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    BoundedRoot = testutil:reset_filestructure(
        "test/test_fts_build_memory_bounded_" ++ Suffix
    ),
    UnboundedRoot = testutil:reset_filestructure(
        "test/test_fts_build_memory_unbounded_" ++ Suffix
    ),
    {ok, Bounded} = leveled_bookie:book_start(start_opts(BoundedRoot)),
    {ok, Unbounded} = leveled_bookie:book_start(start_opts(UnboundedRoot)),
    Schema = schema(<<"full-store-build-memory">>, [body], #{}),
    Sampler = spawn(fun() -> fts_build_sampler(#{}, #{}, []) end),
    try
        %% One dense multi-megabyte document over the same shared pool, so the
        %% hottest terms hold both a high document frequency and a very long
        %% position list inside a single chunk.
        LargeBody = iolist_to_binary([
            <<"alpha beta ">>, zipfian_tokens(large_document, 400000)
        ]),
        full_store_put_corpus([Bounded, Unbounded], Schema, LargeBody),
        WordSize = erlang:system_info(wordsize),
        LegacyBoundWords = 1024 * 1024 * 1024 div WordSize,
        try
            leveled_fts:fts2_outer_bound_for_test(
                fun() ->
                    leveled_fts:fts2_legacy_outer_assemble_for_test(
                        Bounded, Schema
                    )
                end,
                LegacyBoundWords,
                Sampler
            )
        of
            LegacyResult -> ct:fail({legacy_outer_did_not_reproduce, LegacyResult})
        catch
            error:{fts2_parallel_worker_lost, outer_build, killed} -> ok
        end,
        Sampler ! {reports, self()},
        receive
            {fts_build_reports, [{outer_build, killed}]} -> ok;
            {fts_build_reports, LegacyReports} ->
                ct:fail({legacy_outer_report_mismatch, LegacyReports})
        after 5000 ->
            ct:fail(legacy_outer_report_timed_out)
        end,
        Sampler ! reset,
        InnerBoundWords = 128 * 1024 * 1024 div WordSize,
        OuterBoundWords = 128 * 1024 * 1024 div WordSize,
        {ok, #{skipped := []}} = leveled_fts:fts2_outer_bound_for_test(
            fun() ->
                leveled_fts:fts2_consolidate_with_heap_for_test(
                    Bounded,
                    Schema,
                    #{reclaim => false},
                    InnerBoundWords,
                    Sampler
                )
            end,
            OuterBoundWords,
            Sampler
        ),
        Sampler ! {snapshot, self()},
        BoundedPeaks =
            receive
                {fts_build_peaks, BoundedPeaksMessage} ->
                    BoundedPeaksMessage
            after 5000 ->
                ct:fail(fts_build_sampler_timed_out)
            end,
        {BoundedOuterHeapWords, BoundedOuterMemoryBytes} =
            maps:get(outer_build, BoundedPeaks),
        {BoundedInnerRole, BoundedInnerHeapWords, BoundedInnerMemoryBytes} =
            fts_build_inner_peak(BoundedPeaks),
        true = BoundedOuterHeapWords < OuterBoundWords,
        true = BoundedInnerHeapWords < InnerBoundWords,
        ct:pal(
            "bounded FTS outer peak: ~B heap words (~B heap bytes), "
            "~B process bytes; inner peak role ~p: ~B words (~B bytes), "
            "~B process bytes",
            [
                BoundedOuterHeapWords,
                BoundedOuterHeapWords * WordSize,
                BoundedOuterMemoryBytes,
                BoundedInnerRole,
                BoundedInnerHeapWords,
                BoundedInnerHeapWords * WordSize,
                BoundedInnerMemoryBytes
            ]
        ),
        Sampler ! reset,
        {ok, #{skipped := []}} = leveled_fts:fts2_outer_bound_for_test(
            fun() ->
                leveled_fts:fts2_consolidate_with_heap_for_test(
                    Unbounded,
                    Schema,
                    #{reclaim => false},
                    infinity,
                    Sampler
                )
            end,
            infinity,
            Sampler
        ),
        Sampler ! {snapshot, self()},
        UnboundedPeaks =
            receive
                {fts_build_peaks, UnboundedPeaksMessage} ->
                    UnboundedPeaksMessage
            after 5000 ->
                ct:fail(fts_build_sampler_timed_out)
            end,
        {UnboundedOuterHeapWords, UnboundedOuterMemoryBytes} =
            maps:get(outer_build, UnboundedPeaks),
        {UnboundedInnerRole, UnboundedInnerHeapWords,
            UnboundedInnerMemoryBytes} = fts_build_inner_peak(UnboundedPeaks),
        ct:pal(
            "unbounded FTS outer peak: ~B heap words (~B heap bytes), "
            "~B process bytes; inner peak role ~p: ~B words (~B bytes), "
            "~B process bytes",
            [
                UnboundedOuterHeapWords,
                UnboundedOuterHeapWords * WordSize,
                UnboundedOuterMemoryBytes,
                UnboundedInnerRole,
                UnboundedInnerHeapWords,
                UnboundedInnerHeapWords * WordSize,
                UnboundedInnerMemoryBytes
            ]
        ),
        {ok, BoundedIndex} = leveled_fts:fts2_available(Bounded, Schema),
        {ok, UnboundedIndex} = leveled_fts:fts2_available(Unbounded, Schema),
        RootFields = [
            group_count,
            chunk_count,
            term_count,
            bigram_count,
            total_length
        ],
        true =
            maps:with(RootFields, BoundedIndex) =:=
                maps:with(RootFields, UnboundedIndex),
        ct:pal(
            "full-store FTS fixture: ~B documents, ~B terms, ~B bigrams, "
            "~B indexed tokens, ~B-byte largest document",
            [
                maps:get(chunk_count, BoundedIndex),
                maps:get(term_count, BoundedIndex),
                maps:get(bigram_count, BoundedIndex),
                maps:get(total_length, BoundedIndex),
                byte_size(LargeBody)
            ]
        ),
        ProbeOpts = #{
            limit => 40,
            rank => bm25,
            resolve_hits => false,
            return_count => true
        },
        lists:foreach(
            fun(Query) ->
                BoundedResult = search(Bounded, Schema, Query, ProbeOpts),
                BoundedResult = search(Unbounded, Schema, Query, ProbeOpts)
            end,
            [
                <<"alpha">>,
                <<"term17p17">>,
                <<"alpha AND term17p17">>,
                <<"term17p17 OR term8191p31">>,
                <<"\"alpha beta\"">>,
                <<"NEAR(alpha beta, 2)">>
            ]
        )
    after
        Sampler ! stop,
        try
            leveled_bookie:book_destroy(Bounded)
        catch
            _:_ -> ok
        end,
        try
            leveled_bookie:book_destroy(Unbounded)
        catch
            _:_ -> ok
        end
    end.

fts_build_sampler(Active, Peaks, Reports) ->
    receive
        {fts2_build_outer, Pid} ->
            fts_build_sampler_add(Pid, outer_build, Active, Peaks, Reports);
        {fts2_build_worker, Role, Pid} ->
            fts_build_sampler_add(Pid, Role, Active, Peaks, Reports);
        {fts2_bound_report, Role, Reason} ->
            fts_build_sampler(Active, Peaks, [{Role, Reason} | Reports]);
        {'DOWN', Monitor, process, Pid, _Reason} ->
            case maps:find(Pid, Active) of
                {ok, {Monitor, _Role}} ->
                    fts_build_sampler(
                        maps:remove(Pid, Active),
                        Peaks,
                        Reports
                    );
                _ ->
                    fts_build_sampler(Active, Peaks, Reports)
            end;
        {snapshot, From} ->
            NextPeaks = fts_build_sample_workers(Active, Peaks),
            From ! {fts_build_peaks, NextPeaks},
            fts_build_sampler(Active, NextPeaks, Reports);
        {reports, From} ->
            From ! {fts_build_reports, lists:reverse(Reports)},
            fts_build_sampler(Active, Peaks, Reports);
        reset ->
            fts_build_sampler(Active, #{}, []);
        stop ->
            ok
    after 1 ->
        fts_build_sampler(
            Active, fts_build_sample_workers(Active, Peaks), Reports
        )
    end.

fts_build_sampler_add(Pid, Role, Active, Peaks, Reports) ->
    case maps:is_key(Pid, Active) of
        true ->
            fts_build_sampler(Active, Peaks, Reports);
        false ->
            Monitor = erlang:monitor(process, Pid),
            fts_build_sampler(
                Active#{Pid => {Monitor, Role}}, Peaks, Reports
            )
    end.

fts_build_sample_workers(Active, Peaks) ->
    maps:fold(
        fun(Pid, {_Monitor, Role}, Acc) ->
            case process_info(Pid, [total_heap_size, memory]) of
                undefined ->
                    Acc;
                Info ->
                    {HeapPeak, MemoryPeak} = maps:get(Role, Acc, {0, 0}),
                    Acc#{Role => {
                        erlang:max(
                            proplists:get_value(total_heap_size, Info), HeapPeak
                        ),
                        erlang:max(
                            proplists:get_value(memory, Info), MemoryPeak
                        )
                    }}
            end
        end,
        Peaks,
        Active
    ).

fts_build_inner_peak(Peaks) ->
    lists:foldl(
        fun({Role, {HeapWords, MemoryBytes}}, {_BestRole, BestHeap, _BestMemory}
            = Best) ->
            case Role =/= outer_build andalso HeapWords > BestHeap of
                true -> {Role, HeapWords, MemoryBytes};
                false -> Best
            end
        end,
        {none, 0, 0},
        maps:to_list(Peaks)
    ).

full_store_put_corpus(Bookies, Schema, LargeBody) ->
    lists:foreach(
        fun(DocumentNumbers) ->
            Specs = lists:append([
                begin
                    Body =
                        case DocumentNumber of
                            1 -> LargeBody;
                            _ -> full_store_document_body(DocumentNumber)
                        end,
                    Key = <<"doc-", DocumentNumber:32/unsigned-big>>,
                    {ok, DocumentSpecs} = leveled_fts:derive(
                        Schema, Key, #{body => Body}
                    ),
                    DocumentSpecs
                end
             || DocumentNumber <- DocumentNumbers
            ]),
            lists:foreach(
                fun(Bookie) -> full_store_mput(Bookie, Specs) end, Bookies
            )
        end,
        chunked(lists:seq(1, 9000), 32, [])
    ).

%% Each document carries 40 terms that occur nowhere else, for cardinality,
%% plus 40 tokens drawn from a shared 512-word pool with a Zipfian rank
%% distribution, for hot terms. The hot terms are what a real English corpus
%% adds: a few terms hold most of the positions, and a term cannot be split
%% across build shards, so they concentrate in one shard worker.
full_store_document_body(DocumentNumber) ->
    Terms = [
        <<"term",
            (integer_to_binary(DocumentNumber))/binary,
            "p",
            (integer_to_binary(Position))/binary>>
     || Position <- lists:seq(1, 40)
    ],
    iolist_to_binary([
        <<"alpha beta ">>,
        lists:join(<<" ">>, Terms),
        <<" ">>,
        zipfian_tokens(DocumentNumber, 40)
    ]).

zipfian_tokens(Seed, Count) ->
    lists:join(<<" ">>, [zipfian_token(Seed, Index) || Index <- lists:seq(1, Count)]).

zipfian_token(Seed, Index) ->
    Uniform = erlang:phash2({Seed, Index}, 1000000) / 1000000,
    Rank = trunc(math:pow(?ZIPF_POOL, Uniform)),
    <<"zw", (integer_to_binary(Rank))/binary>>.

full_store_mput(Bookie, Specs) ->
    case leveled_bookie:book_mput(Bookie, Specs) of
        ok -> ok;
        pause -> timer:sleep(1)
    end.

chunked([], _Size, Acc) ->
    lists:reverse(Acc);
chunked(Items, Size, Acc) ->
    Count = erlang:min(Size, length(Items)),
    {Chunk, Rest} = lists:split(Count, Items),
    chunked(Rest, Size, [Chunk | Acc]).

oracle_case(Bookie, #{id := Id, tokenizer_opts := TokOpts,
        doc := Doc, queries := Queries}) ->
    Index = atom_to_binary(Id, utf8),
    Schema = schema(Index, [body], TokOpts),
    ok = put_doc(Bookie, Schema, <<"doc">>, #{body => Doc}),
    lists:foreach(fun(#{q := Query, hits := Expected}) ->
        Quoted = <<"\"", Query/binary, "\"">>,
        {ok, #{hits := Hits}} = leveled_fts:search(
            Bookie, Schema, Quoted, #{return_count => true}
        ),
        Actual = [
            {maps:get(key, Hit), maps:get(match_count, Hit)}
         || Hit <- Hits
        ],
        case Actual =:= Expected of
            true -> ok;
            false -> ct:fail({sqlite_oracle_mismatch, Id, Query, Expected, Actual})
        end
    end, Queries).

schema(Index, Columns, Opts) ->
    {ok, Schema} = leveled_fts:schema(Opts#{index => Index, columns => Columns}),
    Schema.

put_doc(Bookie, Schema, Key, Object) ->
    {ok, Specs} = leveled_fts:derive(Schema, Key, Object),
    leveled_bookie:book_mput(Bookie, Specs).

search(Bookie, Schema, Query, Opts) ->
    {ok, Hits} = leveled_fts:search(Bookie, Schema, Query, Opts),
    Hits.

exact_hits(Hits) ->
    [{maps:get(key, Hit), maps:get(match_count, Hit)} || Hit <- Hits].

with_bookie(Fun) ->
    Root = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(Root)),
    try Fun(Bookie, Root)
    after
        try leveled_bookie:book_destroy(Bookie)
        catch _:_ -> ok
        end
    end.

start_opts(Root) ->
    [
        {root_path, Root},
        {sync_strategy, testutil:sync_strategy()},
        {compression_method, none},
        {ledger_compression, none},
        {log_level, warning}
    ].
