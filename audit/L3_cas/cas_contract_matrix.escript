#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

%% Non-finding coverage matrix for the native CAS contract.

-mode(compile).

main(_) ->
    Base = filename:dirname(escript:script_name()),
    ok = basic_matrix(filename:join(Base, "_cas_contract_matrix_data")),
    ok = penciller_matrix(filename:join(Base, "_cas_penciller_matrix_data")),
    ok = concurrent_oracle(filename:join(Base, "_cas_concurrent_oracle_data")),
    ok = head_only_matrix(filename:join(Base, "_cas_head_only_data")),
    ok = abrupt_reject_matrix(filename:join(Base, "_cas_abrupt_reject_data")),
    io:format("PASS: native CAS contract matrix~n"),
    ok.

basic_matrix(Root) ->
    clean(Root),
    Opts = opts(Root) ++ [{value_cache_size, 1048576}],
    {ok, B} = leveled_bookie:book_plainstart(Opts),
    Tag = o,
    ok = leveled_bookie:book_put(B, <<"objects">>, <<"k">>, v1, [], Tag,
        infinity, true),
    {ok, v1, SQN1} = leveled_bookie:book_get_sqn(B, <<"objects">>, <<"k">>, Tag),
    {ok, v1} = leveled_bookie:book_get(B, <<"objects">>, <<"k">>, Tag),
    {ok, BeforeRejectSQN} = leveled_bookie:book_journalsqn(B),
    StaleSQN = SQN1 + 1,

    %% Exact diagnostic shape, whole-batch rejection, and no SQN allocation.
    LK = {Tag, <<"objects">>, <<"k">>, null},
    {error, {precondition_failed, [
        {precondition_failed, LK, {expected, {sqn, StaleSQN}},
            {actual, {active, SQN1}}}
    ]}} = leveled_bookie:book_casmput(B, [
        {put, <<"objects">>, <<"k">>, bad, [], Tag, infinity},
        {put, <<"objects">>, <<"new">>, bad, [], Tag, infinity}
    ], [{<<"objects">>, <<"k">>, Tag, {sqn, StaleSQN}}], true),
    {ok, BeforeRejectSQN} = leveled_bookie:book_journalsqn(B),
    {ok, v1} = leveled_bookie:book_get(B, <<"objects">>, <<"k">>, Tag),
    not_found = leveled_bookie:book_get(B, <<"objects">>, <<"new">>, Tag),

    %% Duplicate write keys are rejected before execution.
    {error, {duplicate_key, LK}} = leveled_bookie:book_casmput(B, [
        {put, <<"objects">>, <<"k">>, x, [], Tag, infinity},
        {put, <<"objects">>, <<"k">>, y, [], Tag, infinity}
    ], [{<<"objects">>, <<"k">>, Tag, {sqn, SQN1}}]),
    {ok, BeforeRejectSQN} = leveled_bookie:book_journalsqn(B),
    {error, {precondition_failed, [
        {precondition_failed, LK, {expected, present},
            {actual, duplicate_precondition}}
    ]}} = leveled_bookie:book_casmput(B, [
        {put, <<"objects">>, <<"new">>, x, [], Tag, infinity}
    ], [
        {<<"objects">>, <<"k">>, Tag, {sqn, SQN1}},
        {<<"objects">>, <<"k">>, Tag, present}
    ]),

    %% Conditions may guard a key in another bucket and another application tag.
    GuardTag = guard_v1,
    ok = leveled_bookie:book_put(B, <<"guards">>, <<"g">>, token, [], GuardTag,
        infinity, true),
    {ok, token, GuardSQN} = leveled_bookie:book_get_sqn(
        B, <<"guards">>, <<"g">>, GuardTag),
    ok = leveled_bookie:book_casmput(B, [
        {put, <<"targets">>, <<"t">>, committed, [], Tag, infinity}
    ], [{<<"guards">>, <<"g">>, GuardTag, {sqn, GuardSQN}}], true),
    {ok, committed} = leveled_bookie:book_get(B, <<"targets">>, <<"t">>, Tag),
    {error, {precondition_failed, [_]}} = leveled_bookie:book_casmput(B, [
        {put, <<"targets">>, <<"not_written">>, bad, [], Tag, infinity}
    ], [{<<"other_bucket">>, <<"g">>, GuardTag, present}], true),
    not_found = leveled_bookie:book_get(B, <<"targets">>, <<"not_written">>, Tag),

    %% TTL-expired is absent; future TTL is present. CAS-created expired rows
    %% can be replaced under absent.
    Past = leveled_util:integer_now() - 10,
    Future = leveled_util:integer_now() + 600,
    ok = leveled_bookie:book_casput(B, <<"ttl">>, <<"expired">>, old, [], Tag,
        Past, true, absent),
    not_found = leveled_bookie:book_get(B, <<"ttl">>, <<"expired">>, Tag),
    {error, {precondition_failed, [
        {precondition_failed, _, {expected, present}, {actual, expired}}
    ]}} = leveled_bookie:book_casmput(B, [
        {put, <<"ttl">>, <<"side">>, bad, [], Tag, infinity}
    ], [{<<"ttl">>, <<"expired">>, Tag, present}]),
    ok = leveled_bookie:book_casput(B, <<"ttl">>, <<"expired">>, live, [], Tag,
        Future, true, absent),
    {ok, live} = leveled_bookie:book_get(B, <<"ttl">>, <<"expired">>, Tag),

    %% Tombstones have the same absent-CAS semantics as missing/expired heads.
    ok = leveled_bookie:book_delete(B, <<"tomb">>, <<"k">>, []),
    {error, {precondition_failed, [
        {precondition_failed, _, {expected, present}, {actual, tombstone}}
    ]}} = leveled_bookie:book_casmput(B, [
        {put, <<"tomb">>, <<"side">>, bad, [], Tag, infinity}
    ], [{<<"tomb">>, <<"k">>, Tag, present}]),
    ok = leveled_bookie:book_casput(B, <<"tomb">>, <<"k">>, resurrected, [],
        Tag, infinity, true, absent),
    {ok, resurrected} = leveled_bookie:book_get(B, <<"tomb">>, <<"k">>, Tag),

    %% A successful CAS gets a new SQN, so the old value-cache entry is not
    %% addressable by subsequent reads; a failed CAS does not change the head.
    ok = leveled_bookie:book_casput(B, <<"objects">>, <<"k">>, v2, [], Tag,
        infinity, true, {sqn, SQN1}),
    {ok, v2, SQN2} = leveled_bookie:book_get_sqn(B, <<"objects">>, <<"k">>, Tag),
    true = SQN2 > SQN1,
    {ok, v2} = leveled_bookie:book_get(B, <<"objects">>, <<"k">>, Tag),
    {error, {precondition_failed, [_]}} = leveled_bookie:book_casput(
        B, <<"objects">>, <<"k">>, stale, [], Tag, infinity, true, {sqn, SQN1}),
    {ok, v2} = leveled_bookie:book_get(B, <<"objects">>, <<"k">>, Tag),

    %% Validation vocabulary and structured unsupported-tag diagnostic.
    {error, invalid_cas_condition} = leveled_bookie:book_casmput(B, [
        {put, <<"x">>, <<"x">>, x, [], Tag, infinity}
    ], [{<<"x">>, <<"x">>, Tag, {sqn, -1}}]),
    {error, {precondition_failed, [
        {precondition_failed, _, {expected, absent}, {actual, {invalid_tag, h}}}
    ]}} = leveled_bookie:book_casmput(B, [
        {put, <<"x">>, <<"x">>, x, [], Tag, infinity}
    ], [{<<"x">>, {<<"x">>, <<"sub">>}, h, absent}]),

    ok = leveled_bookie:book_close(B),
    {ok, B2} = leveled_bookie:book_plainstart(Opts),
    {ok, v2, SQN2} = leveled_bookie:book_get_sqn(B2, <<"objects">>, <<"k">>, Tag),
    not_found = leveled_bookie:book_get(B2, <<"objects">>, <<"new">>, Tag),
    ok = leveled_bookie:book_destroy(B2),
    clean(Root).

penciller_matrix(Root) ->
    clean(Root),
    %% Reopen after a clean close so the condition head is served by the
    %% Penciller rather than the Bookie's fresh ledger-cache ETS table.
    Opts = opts(Root),
    {ok, B} = leveled_bookie:book_plainstart(Opts),
    ok = leveled_bookie:book_put(B, <<"pcl">>, <<"k">>, v1, [], o, infinity, true),
    {ok, v1, SQN} = leveled_bookie:book_get_sqn(B, <<"pcl">>, <<"k">>, o),
    ok = leveled_bookie:book_close(B),
    {ok, B2} = leveled_bookie:book_plainstart(Opts),
    ok = leveled_bookie:book_casput(B2, <<"pcl">>, <<"k">>, v2, [], o,
        infinity, true, {sqn, SQN}),
    {ok, v2} = leveled_bookie:book_get(B2, <<"pcl">>, <<"k">>, o),
    ok = leveled_bookie:book_destroy(B2),
    clean(Root).

concurrent_oracle(Root) ->
    clean(Root),
    Opts = opts(Root),
    {ok, B} = leveled_bookie:book_plainstart(Opts),
    ok = leveled_bookie:book_put(B, <<"race">>, <<"a">>, 0, [], o, infinity, true),
    ok = leveled_bookie:book_put(B, <<"race">>, <<"b">>, 0, [], o, infinity, true),
    {ok, 0, ASQN} = leveled_bookie:book_get_sqn(B, <<"race">>, <<"a">>, o),
    {ok, 0, BSQN} = leveled_bookie:book_get_sqn(B, <<"race">>, <<"b">>, o),
    Parent = self(),
    Workers = [spawn(fun() ->
        R = leveled_bookie:book_casmput(B, [
            {put, <<"race">>, <<"a">>, N, [], o, infinity},
            {put, <<"race">>, <<"b">>, N, [], o, infinity}
        ], [
            {<<"race">>, <<"a">>, o, {sqn, ASQN}},
            {<<"race">>, <<"b">>, o, {sqn, BSQN}},
            {<<"guard">>, <<"outside">>, o, absent}
        ], true),
        Parent ! {self(), N, R}
    end) || N <- lists:seq(1, 32)],
    Results = [receive {P, N, R} -> {N, R} after 10000 -> error(worker_timeout) end
        || P <- Workers],
    [{Winner, ok}] = [{N, R} || {N, R} <- Results, R =:= ok],
    31 = length([R || {_N, {error, {precondition_failed, [_ | _]}} = R} <- Results]),
    %% Sequential oracle: applying these returned outcomes in any response
    %% order accepts exactly Winner and leaves both batch keys at Winner.
    {ok, Winner, BatchSQN} = leveled_bookie:book_get_sqn(B, <<"race">>, <<"a">>, o),
    {ok, Winner, BatchSQN} = leveled_bookie:book_get_sqn(B, <<"race">>, <<"b">>, o),
    ok = leveled_bookie:book_close(B),
    {ok, B2} = leveled_bookie:book_plainstart(Opts),
    {ok, Winner, BatchSQN} = leveled_bookie:book_get_sqn(B2, <<"race">>, <<"a">>, o),
    {ok, Winner, BatchSQN} = leveled_bookie:book_get_sqn(B2, <<"race">>, <<"b">>, o),
    ok = leveled_bookie:book_destroy(B2),
    clean(Root).

head_only_matrix(Root) ->
    clean(Root),
    {ok, B} = leveled_bookie:book_plainstart(opts(Root) ++ [{head_only, no_lookup}]),
    {unsupported_message, casbatchput} = leveled_bookie:book_casmput(B, [
        {put, <<"b">>, <<"k">>, v, [], o, infinity}
    ], [{<<"b">>, <<"k">>, o, absent}]),
    ok = leveled_bookie:book_destroy(B),
    clean(Root).

abrupt_reject_matrix(Root) ->
    clean(Root),
    Opts = opts(Root),
    {ok, B} = leveled_bookie:book_plainstart(Opts),
    ok = leveled_bookie:book_put(B, <<"seed">>, <<"seed">>, seed, [], o,
        infinity, true),
    {ok, BeforeSQN} = leveled_bookie:book_journalsqn(B),
    Conditions = [{<<"missing">>, integer_to_binary(N), o, present}
        || N <- lists:seq(1, 50000)],
    Parent = self(),
    Caller = spawn(fun() ->
        R = try
            leveled_bookie:book_casmput(B, [
                {put, <<"target">>, <<"target">>, must_not_commit, [], o, infinity}
            ], Conditions, true)
        catch
            Class:Reason -> {Class, Reason}
        end,
        Parent ! {abrupt_result, R}
    end),
    ok = wait_started(B, Caller, 5000),
    true = is_process_alive(Caller),
    Ref = erlang:monitor(process, B),
    exit(B, kill),
    receive {'DOWN', Ref, process, B, _} -> ok after 5000 -> error(kill_timeout) end,
    timer:sleep(250),
    {ok, B2} = leveled_bookie:book_plainstart(Opts),
    {ok, BeforeSQN} = leveled_bookie:book_journalsqn(B2),
    not_found = leveled_bookie:book_get(B2, <<"target">>, <<"target">>, o),
    ok = leveled_bookie:book_destroy(B2),
    receive {abrupt_result, _} -> ok after 1000 -> ok end,
    clean(Root).

wait_started(_B, _Caller, Remaining) when Remaining =< 0 ->
    {error, start_timeout};
wait_started(B, Caller, Remaining) ->
    case {is_process_alive(Caller), process_info(B, current_function)} of
        {true, {current_function, {gen, do_call, 4}}} -> ok;
        {true, {current_function, {leveled_bookie, _, _}}} -> ok;
        _ ->
            timer:sleep(2),
            wait_started(B, Caller, Remaining - 2)
    end.

opts(Root) -> [
    {root_path, Root},
    {max_journalsize, 1000000},
    {cache_size, 500},
    {sync_strategy, riak_sync},
    {compression_method, none}
].

clean(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
