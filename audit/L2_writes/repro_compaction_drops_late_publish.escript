#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

main(_) ->
    logger:set_primary_config(level, emergency),
    Root = filename:join(filename:dirname(escript:script_name()),
                         ".repro_compaction_drops_late_publish.data"),
    rm_rf(Root),
    ok = filelib:ensure_dir(filename:join(Root, "sentinel")),
    Opts = [
        {root_path, Root},
        {cache_size, 100},
        {cache_multiple, 1},
        {max_pencillercachesize, 401},
        {max_journalsize, 1000000},
        {max_run_length, 1},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, [{o, retain}]},
        {journalcompaction_scoreonein, 1},
        {singlefile_compactionpercentage, 0.0},
        {maxrunlength_compactionpercentage, 0.0}
    ],
    {ok, Bookie} = leveled_bookie:book_plainstart(Opts),
    {ok, Inker, Penciller} =
        gen_server:call(Bookie, return_actors, infinity),
    Bucket = <<"bucket">>,
    Key = <<"delayed">>,
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, o),

    %% A live writer has completed its durable journal append but is delayed
    %% before PUBLISH.
    {ok, SQN1, Size1} =
        leveled_inker:ink_put(Inker, LedgerKey, delayed_value,
                              {[], infinity}, true),
    Changes1 = changes(LedgerKey, SQN1, delayed_value, Size1),

    %% Later publishes open a gap. After five seconds the frontier skips SQN 1
    %% and pushes beyond it; wait until that watermark is durable in the ledger.
    fill(Bookie, Bucket, <<"filler">>, 280),
    timer:sleep(5200),
    ok = leveled_bookie:book_put(
             Bookie, Bucket, <<"expiry-trigger">>, trigger, [], o,
             infinity, true),
    Persisted = wait_persisted(Penciller, SQN1 + 1, 150),

    %% Roll and compact while SQN 1 is absent from the ledger snapshot. The
    %% compactor classifies its journal record as unreachable and removes it.
    ok = leveled_inker:ink_roll(Inker),
    ok = leveled_bookie:book_compactjournal(Bookie, 30000),
    ok = wait_compaction(Bookie, 150),
    CompactionResult = leveled_bookie:book_lastcompactionresult(Bookie),

    %% The slow, still-live writer now publishes and receives ok, but its body
    %% no longer exists in the journal.
    ok = gen_server:call(Bookie, {publish, SQN1, Changes1}, infinity),
    LiveRead = leveled_bookie:book_get(Bookie, Bucket, Key, o),

    BMon = erlang:monitor(process, Bookie),
    IMon = erlang:monitor(process, Inker),
    PMon = erlang:monitor(process, Penciller),
    exit(Bookie, kill),
    ok = wait_down(BMon, Bookie),
    ok = wait_down(IMon, Inker),
    ok = wait_down(PMon, Penciller),
    {ok, Reopened} = leveled_bookie:book_plainstart(Opts),
    RestartRead = leveled_bookie:book_get(Reopened, Bucket, Key, o),

    io:format("delayed durable SQN: ~p~n", [SQN1]),
    io:format("ledger persisted watermark before compaction: ~p~n", [Persisted]),
    io:format("compaction result: ~p~n", [CompactionResult]),
    io:format("late PUBLISH returned: ok~n"),
    io:format("expected live read after acknowledged publish: {ok,delayed_value}~n"),
    io:format("actual live read:                              ~p~n", [LiveRead]),
    io:format("expected restart read: {ok,delayed_value}~n"),
    io:format("actual restart read:   ~p~n", [RestartRead]),

    ok = leveled_bookie:book_close(Reopened),
    rm_rf(Root),
    case {Persisted > SQN1, CompactionResult, LiveRead, RestartRead} of
        {true, {done, N}, not_found, not_found} when is_integer(N), N > 0 ->
            halt(0);
        Other ->
            io:format(standard_error, "BUG DID NOT REPRODUCE: ~p~n", [Other]),
            halt(1)
    end.

fill(Bookie, Bucket, Prefix, Count) ->
    lists:foreach(
      fun(N) ->
          K = <<Prefix/binary, "-", (integer_to_binary(N))/binary>>,
          R = leveled_bookie:book_put(
                  Bookie, Bucket, K, {Prefix, N}, [], o, infinity, false),
          true = (R =:= ok orelse R =:= pause)
      end,
      lists:seq(1, Count)).

wait_persisted(Penciller, Minimum, Remaining) when Remaining > 0 ->
    SQN = leveled_penciller:pcl_persistedsqn(Penciller),
    case SQN >= Minimum of
        true -> SQN;
        false -> timer:sleep(100), wait_persisted(Penciller, Minimum, Remaining - 1)
    end;
wait_persisted(Penciller, _Minimum, 0) ->
    leveled_penciller:pcl_persistedsqn(Penciller).

wait_compaction(Bookie, Remaining) when Remaining > 0 ->
    case leveled_bookie:book_islastcompactionpending(Bookie) of
        false -> ok;
        true -> timer:sleep(100), wait_compaction(Bookie, Remaining - 1)
    end;
wait_compaction(_Bookie, 0) ->
    error(compaction_timeout).

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
