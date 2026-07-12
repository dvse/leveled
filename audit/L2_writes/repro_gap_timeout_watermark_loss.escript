#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

main(_) ->
    logger:set_primary_config(level, emergency),
    Root = filename:join(filename:dirname(escript:script_name()),
                         ".repro_gap_timeout_watermark_loss.data"),
    rm_rf(Root),
    ok = filelib:ensure_dir(filename:join(Root, "sentinel")),
    Opts = [{root_path, Root}, {cache_size, 100}, {cache_multiple, 1}],
    {ok, Bookie} = leveled_bookie:book_plainstart(Opts),
    BookieMon = erlang:monitor(process, Bookie),
    {ok, Inker, _} = gen_server:call(Bookie, {write_refs}, infinity),
    {ok, _Inker2, Penciller} = gen_server:call(Bookie, return_actors, infinity),
    Bucket = <<"bucket">>,
    Key = <<"target">>,
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, o),

    %% SQN 1 is durable, but model a live caller delayed before PUBLISH.
    {ok, SQN1, Size1} =
        leveled_inker:ink_put(Inker, LedgerKey, first, {[], infinity}, true),
    Changes1 = changes(LedgerKey, SQN1, first, Size1),

    %% This later write is acknowledged while SQN 1 is still a frontier gap.
    ok = leveled_bookie:book_put(
             Bookie, Bucket, Key, second, [], o, infinity, true),
    {ok, second} = leveled_bookie:book_get(Bookie, Bucket, Key, o),
    fill(Bookie, Bucket, <<"pre">>, 280),

    %% Let the fixed 5-second gap deadline expire.  The next publish skips
    %% SQN 1 and allows the cache watermark to advance beyond it.
    timer:sleep(5200),
    ok = leveled_bookie:book_put(
             Bookie, Bucket, <<"expiry-trigger">>, trigger, [], o, infinity, true),
    %% #state.ledger_sqn is the highest SQN accepted into the Penciller's
    %% in-memory L0 cache (the cache->Penciller push watermark).
    PencillerWatermark = element(5, sys:get_state(Penciller)),

    %% The original caller is alive, resumes, and gets ok.  Because its SQN is
    %% now behind the frontier, its stale cache value can be pushed under a
    %% much newer watermark by subsequent ordinary writes.
    ok = gen_server:call(Bookie, {publish, SQN1, Changes1}, infinity),
    BeforeRestart = leveled_bookie:book_get(Bookie, Bucket, Key, o),
    CrashCall = fill_until_down(Bookie, Bucket, <<"post">>, 1, 600),
    BookieDown =
        receive
            {'DOWN', BookieMon, process, Bookie, Reason} -> {down, Reason}
        after 5000 ->
            still_alive
        end,

    {ok, Reopened} = leveled_bookie:book_plainstart(Opts),
    AfterRestart = leveled_bookie:book_get(Reopened, Bucket, Key, o),
    io:format("delayed SQN: ~p~n", [SQN1]),
    io:format("Penciller push watermark after timeout skip: ~p~n",
              [PencillerWatermark]),
    io:format("newer write returned ok and initially read: {ok,second}~n"),
    io:format("after slow live caller finally publishes: ~p~n", [BeforeRestart]),
    io:format("next cache push call result: ~p~n", [CrashCall]),
    io:format("Bookie/Penciller failure observed: ~p~n", [BookieDown]),
    io:format("expected after restart (SQN order/direct oracle): {ok,second}~n"),
    io:format("actual after restart:                         ~p~n", [AfterRestart]),
    ok = leveled_bookie:book_close(Reopened),
    rm_rf(Root),
    case {BeforeRestart, AfterRestart, PencillerWatermark > SQN1,
          CrashCall, BookieDown} of
        {{ok, first}, {ok, second}, true,
         {call_failed, _N, _Class, _Reason}, {down, _}} -> halt(0);
        Other ->
            io:format(standard_error, "BUG DID NOT REPRODUCE: ~p~n", [Other]),
            halt(1)
    end.

fill(Bookie, Bucket, Prefix, Count) ->
    lists:foreach(
      fun(N) ->
          Key = <<Prefix/binary, "-", (integer_to_binary(N))/binary>>,
          ok = leveled_bookie:book_put(
                   Bookie, Bucket, Key, {Prefix, N}, [], o, infinity, false)
      end,
      lists:seq(1, Count)).

fill_until_down(_Bookie, _Bucket, _Prefix, N, Max) when N > Max ->
    no_crash;
fill_until_down(Bookie, Bucket, Prefix, N, Max) ->
    Key = <<Prefix/binary, "-", (integer_to_binary(N))/binary>>,
    try leveled_bookie:book_put(
            Bookie, Bucket, Key, {Prefix, N}, [], o, infinity, false) of
        ok -> fill_until_down(Bookie, Bucket, Prefix, N + 1, Max);
        pause -> fill_until_down(Bookie, Bucket, Prefix, N + 1, Max);
        Other -> {unexpected_reply, N, Other}
    catch
        Class:Reason -> {call_failed, N, Class, Reason}
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
