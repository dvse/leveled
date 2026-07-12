#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

main(_) ->
    logger:set_primary_config(level, emergency),
    Root = filename:join(filename:dirname(escript:script_name()),
                         ".repro_persisted_watermark_loses_late_publish.data"),
    rm_rf(Root),
    ok = filelib:ensure_dir(filename:join(Root, "sentinel")),
    Opts = [{root_path, Root}, {cache_size, 100}, {cache_multiple, 1},
            {max_pencillercachesize, 401}],
    {ok, Bookie} = leveled_bookie:book_plainstart(Opts),
    {ok, Inker, _} = gen_server:call(Bookie, {write_refs}, infinity),
    {ok, _Inker2, Penciller} =
        gen_server:call(Bookie, return_actors, infinity),
    Bucket = <<"bucket">>,
    Key = <<"delayed-only-key">>,
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, o),

    %% A live caller has durably appended SQN 1 but is delayed before PUBLISH.
    {ok, SQN1, Size1} =
        leveled_inker:ink_put(Inker, LedgerKey, delayed_value,
                              {[], infinity}, true),
    Changes1 = changes(LedgerKey, SQN1, delayed_value, Size1),

    %% A later caller opens the frontier gap.  Once its fixed deadline expires,
    %% ordinary published writes are allowed to persist above SQN 1.
    ok = leveled_bookie:book_put(
             Bookie, Bucket, <<"later">>, later, [], o, infinity, true),
    timer:sleep(5200),
    ok = leveled_bookie:book_put(
             Bookie, Bucket, <<"expiry-trigger">>, trigger, [], o,
             infinity, true),
    Persisted = force_persisted(Bookie, Penciller, Bucket, SQN1, 1, 12),

    %% The original caller is still alive.  Its PUBLISH returns ok and the value
    %% is current in the live ledger cache.
    ok = gen_server:call(Bookie, {publish, SQN1, Changes1}, infinity),
    {ok, delayed_value} = leveled_bookie:book_get(Bookie, Bucket, Key, o),

    %% Abrupt stop: recovery trusts the ledger watermark and starts journal
    %% replay after it, so the acknowledged SQN below that watermark is skipped.
    BMon = erlang:monitor(process, Bookie),
    IMon = erlang:monitor(process, Inker),
    PMon = erlang:monitor(process, Penciller),
    exit(Bookie, kill),
    ok = wait_down(BMon, Bookie),
    ok = wait_down(IMon, Inker),
    ok = wait_down(PMon, Penciller),
    {ok, Reopened} = leveled_bookie:book_plainstart(Opts),
    AfterRestart = leveled_bookie:book_get(Reopened, Bucket, Key, o),

    io:format("delayed durable SQN: ~p~n", [SQN1]),
    io:format("persisted ledger watermark before late PUBLISH: ~p~n",
              [Persisted]),
    io:format("late PUBLISH returned: ok~n"),
    io:format("live read before abrupt stop: {ok,delayed_value}~n"),
    io:format("expected after restart:      {ok,delayed_value}~n"),
    io:format("actual after restart:        ~p~n", [AfterRestart]),

    ok = leveled_bookie:book_close(Reopened),
    rm_rf(Root),
    case {Persisted > SQN1, AfterRestart} of
        {true, not_found} -> halt(0);
        Other ->
            io:format(standard_error, "BUG DID NOT REPRODUCE: ~p~n", [Other]),
            halt(1)
    end.

force_persisted(_Bookie, Penciller, _Bucket, _Minimum, Round, Max)
        when Round > Max ->
    leveled_penciller:pcl_persistedsqn(Penciller);
force_persisted(Bookie, Penciller, Bucket, Minimum, Round, Max) ->
    Prefix = <<"persist-", (integer_to_binary(Round))/binary>>,
    fill(Bookie, Bucket, Prefix, 350),
    case wait_persisted(Penciller, Minimum, 30) of
        SQN when SQN >= Minimum -> SQN;
        _ -> force_persisted(Bookie, Penciller, Bucket, Minimum, Round + 1, Max)
    end.

wait_persisted(Penciller, Minimum, Remaining) when Remaining > 0 ->
    SQN = leveled_penciller:pcl_persistedsqn(Penciller),
    case SQN >= Minimum of
        true -> SQN;
        false -> timer:sleep(100), wait_persisted(Penciller, Minimum, Remaining - 1)
    end;
wait_persisted(Penciller, _Minimum, 0) ->
    leveled_penciller:pcl_persistedsqn(Penciller).

fill(Bookie, Bucket, Prefix, Count) ->
    lists:foreach(
      fun(N) ->
          K = <<Prefix/binary, "-", (integer_to_binary(N))/binary>>,
          R = leveled_bookie:book_put(
                  Bookie, Bucket, K, {Prefix, N}, [], o, infinity, false),
          true = (R =:= ok orelse R =:= pause)
      end,
      lists:seq(1, Count)).

wait_down(Mon, Pid) ->
    receive {'DOWN', Mon, process, Pid, _} -> ok
    after 5000 -> error({process_still_alive, Pid})
    end.

changes(LedgerKey, SQN, Object, Size) ->
    {_Bucket, _Key, MetaValue, {KeyHash, _ObjectHash}, _LastMods} =
        leveled_codec:generate_ledgerkv(
            LedgerKey, SQN, Object, Size, infinity),
    {KeyHash, SQN, [{LedgerKey, MetaValue}]}.

rm_rf(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
