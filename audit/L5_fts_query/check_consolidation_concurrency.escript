#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    process_flag(priority, high),
    Here = filename:dirname(escript:script_name()),
    RootA = filename:join(Here, "tmp_cons_concurrent_a"),
    RootB = filename:join(Here, "tmp_cons_concurrent_b"),
    ok = reset(RootA), ok = reset(RootB),
    {ok, A} = start(RootA),
    {ok, B} = start(RootB),
    try
        Initial = [{put, key(N), object(initial_text(N))} || N <- lists:seq(1, 500)],
        ok = apply_ops(A, Initial), ok = apply_ops(B, Initial),
        BeforeA = snapshot(A), BeforeB = snapshot(B), BeforeA = BeforeB,

        1 = erlang:trace_pattern({leveled_fts, consolidate_shard, 5}, true, [local]),
        {async, ConsRun} = leveled_bookie:book_ftsconsolidate(A, <<"docs">>, <<"main">>, #{}),
        Parent = self(),
        RunnerPid = spawn(fun() ->
            process_flag(priority, low),
            receive go -> Parent ! {cons_result, ConsRun()} end
        end),
        1 = erlang:trace(RunnerPid, true, [call, set_on_spawn, {tracer, self()}]),
        RunnerPid ! go,
        Worker = receive
            {trace, Pid, call, {leveled_fts, consolidate_shard, _Args}} -> Pid
        after 5000 -> erlang:error(consolidation_trace_timeout)
        end,
        true = erlang:suspend_process(Worker),

        %% These writes land after the consolidation worker's ledger snapshot
        %% but before its old derivation is applied.
        Concurrent =
            [{put, key(N), object(<<"updated alpha concurrent phrase target">>)}
             || N <- lists:seq(1, 30)] ++
            [{delete, key(N)} || N <- lists:seq(31, 45)] ++
            [{put, key(N), object(<<"inserted beta concurrent near target">>)}
             || N <- lists:seq(501, 530)],
        ok = apply_ops(A, Concurrent), ok = apply_ops(B, Concurrent),
        true = erlang:resume_process(Worker),
        receive
            {cons_result, ok} -> ok;
            {cons_result, Other} -> erlang:error({consolidation_failed, Other})
        after 10000 -> erlang:error(consolidation_timeout)
        end,

        %% B is the unconsolidated oracle. Exact hit maps (including BM25
        %% scores, doc lengths, positions, order and windows) must agree.
        AfterConcurrentA = snapshot(A),
        AfterConcurrentB = snapshot(B),
        AfterConcurrentA = AfterConcurrentB,
        {async, ConsB} = leveled_bookie:book_ftsconsolidate(B, <<"docs">>, <<"main">>, #{}),
        ok = ConsB(),
        AfterConcurrentA = snapshot(B),
        {async, ReConsA} = leveled_bookie:book_ftsconsolidate(A, <<"docs">>, <<"main">>, #{}),
        ok = ReConsA(),
        AfterConcurrentA = snapshot(A),
        ok = leveled_bookie:book_close(A), ok = leveled_bookie:book_close(B),
        {ok, A2} = start(RootA), {ok, B2} = start(RootB),
        try
            AfterConcurrentA = snapshot(A2),
            AfterConcurrentA = snapshot(B2),
            io:format(
                "PASS: concurrent snapshot-old consolidation, updates, deletes, "
                "inserts, re-consolidation and restart stayed exactly equal~n"
            )
        after
            leveled_bookie:book_close(A2), leveled_bookie:book_close(B2)
        end
    after
        erlang:trace_pattern({leveled_fts, consolidate_shard, 5}, false, [local]),
        safe_close(A), safe_close(B),
        reset(RootA), reset(RootB)
    end.

start(Root) ->
    Schema = #{
        bucket => <<"docs">>, tag => o, index => <<"main">>,
        columns => [#{name => body, path => [body]}], tokenizer => unicode61
    },
    leveled_bookie:book_start([
        {root_path, Root}, {compression_method, none},
        {ledger_compression, none}, {log_level, warning},
        {fts_indexes, [Schema]}
    ]).

apply_ops(Bookie, Ops) ->
    Entries = [
        case Op of
            {put, K, O} -> {put, <<"docs">>, K, O, [], o, infinity};
            {delete, K} -> {delete, <<"docs">>, K, [], o, infinity}
        end
     || Op <- Ops
    ],
    case leveled_bookie:book_mput_std(Bookie, Entries, false) of
        ok -> ok;
        pause -> ok
    end.

snapshot(Bookie) ->
    Queries = [
        {<<"alpha">>, #{rank => none}},
        {<<"beta AND gamma">>, #{rank => none}},
        {<<"alpha OR concurrent">>, #{rank => none, limit => 37, offset => 3}},
        {<<"alpha NOT deleted">>, #{rank => none}},
        {<<"\"phrase target\"">>, #{rank => none, return_positions => true}},
        {<<"NEAR(concurrent target, 3)">>, #{rank => none}},
        {<<"concur*">>, #{rank => none}},
        {<<"alpha">>, #{rank => bm25, limit => 41}}
    ],
    [{Q, O, query(Bookie, Q, O)} || {Q, O} <- Queries].

query(Bookie, Q, Extra) ->
    Opts = maps:merge(
        #{columns => [body], limit => 20000, result_cache => false}, Extra
    ),
    {async, Run} = leveled_bookie:book_ftssearch(
        Bookie, <<"docs">>, <<"main">>, Q, Opts
    ),
    Run().

initial_text(N) ->
    case N rem 5 of
        0 -> <<"alpha beta phrase target">>;
        1 -> <<"beta gamma near target">>;
        2 -> <<"alpha gamma deleted">>;
        3 -> <<"prefixable delta epsilon">>;
        4 -> <<"alpha beta gamma common">>
    end.

object(Text) -> #{body => Text}.

key(N) ->
    NBin = integer_to_binary(N),
    Pad = binary:copy(<<"0">>, 4 - byte_size(NBin)),
    <<"k", Pad/binary, NBin/binary>>.

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.

safe_close(Pid) ->
    try leveled_bookie:book_close(Pid) of _ -> ok catch exit:_ -> ok end.
