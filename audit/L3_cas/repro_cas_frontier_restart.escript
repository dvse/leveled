#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

%% Deterministically interleave a caller-side book_put with in-Bookie CAS.
%% The Bookie is suspended only to order its mailbox; the ordinary put still
%% executes its journal append in its caller process using the public API.

-mode(compile).

main(_) ->
    Root = filename:join(
        filename:dirname(escript:script_name()),
        "_repro_cas_frontier_restart_data"
    ),
    ok = clean(Root),
    Opts = [
        {root_path, Root},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie} = leveled_bookie:book_plainstart(Opts),
    Bucket = <<"B">>,
    Key = <<"K">>,
    Tag = o,
    ok = leveled_bookie:book_put(
        Bookie, Bucket, Key, seed, [], Tag, infinity, true
    ),
    {ok, seed, SeedSQN} = leveled_bookie:book_get_sqn(
        Bookie, Bucket, Key, Tag
    ),

    %% Keep one process alive so book_put's process-dictionary write_refs cache
    %% is primed before the Bookie is suspended.
    Parent = self(),
    Writer = spawn(fun() -> writer_loop(Parent, Bookie, Tag) end),
    Writer ! prime,
    receive {primed, ok} -> ok after 5000 -> error(prime_timeout) end,

    ok = sys:suspend(Bookie),
    CasPid = spawn(fun() ->
        Result = leveled_bookie:book_casput(
            Bookie, Bucket, Key, cas_value, [], Tag, infinity, true,
            {sqn, SeedSQN}
        ),
        Parent ! {cas_result, Result}
    end),
    ok = wait_queue(Bookie, 1, 5000),
    Writer ! {write, Bucket, Key, caller_put_value},
    ok = wait_queue(Bookie, 2, 5000),
    ok = sys:resume(Bookie),

    CasResult = receive {cas_result, R1} -> R1 after 10000 -> timeout end,
    PutResult = receive {put_result, R2} -> R2 after 10000 -> timeout end,
    Before = leveled_bookie:book_get_sqn(Bookie, Bucket, Key, Tag),
    io:format("seed_sqn=~p cas=~p put=~p before_restart=~p~n",
        [SeedSQN, CasResult, PutResult, Before]),

    %% Abrupt stop: replay must be observationally equivalent to the state
    %% exposed after both acknowledged operations.
    Ref = erlang:monitor(process, Bookie),
    exit(Bookie, kill),
    receive {'DOWN', Ref, process, Bookie, _} -> ok after 5000 -> error(kill_timeout) end,
    timer:sleep(250),
    {ok, Reopened} = leveled_bookie:book_plainstart(Opts),
    After = leveled_bookie:book_get_sqn(Reopened, Bucket, Key, Tag),
    io:format("after_restart=~p~n", [After]),
    case {CasResult, PutResult, Before, After} of
        {ok, ok, {ok, caller_put_value, PutSQN}, {ok, cas_value, CasSQN}}
                when CasSQN > PutSQN ->
            io:format(
                "BUG: acknowledged state changed across restart; "
                "pre-restart SQN ~p was replaced by replayed SQN ~p~n",
                [PutSQN, CasSQN]
            );
        _ ->
            io:format("UNEXPECTED: interleaving did not reproduce~n"),
            halt(2)
    end,
    ok = leveled_bookie:book_destroy(Reopened),
    Writer ! stop,
    exit(CasPid, normal),
    ok = clean(Root),
    ok.

writer_loop(Parent, Bookie, Tag) ->
    receive
        prime ->
            R = leveled_bookie:book_put(
                Bookie, <<"prime">>, <<"prime">>, primed, [], Tag,
                infinity, true
            ),
            Parent ! {primed, R},
            writer_loop(Parent, Bookie, Tag);
        {write, Bucket, Key, Value} ->
            R = leveled_bookie:book_put(
                Bookie, Bucket, Key, Value, [], Tag, infinity, true
            ),
            Parent ! {put_result, R},
            writer_loop(Parent, Bookie, Tag);
        stop ->
            ok
    end.

wait_queue(_Pid, _AtLeast, Remaining) when Remaining =< 0 ->
    {error, queue_timeout};
wait_queue(Pid, AtLeast, Remaining) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, N} when N >= AtLeast -> ok;
        _ ->
            timer:sleep(5),
            wait_queue(Pid, AtLeast, Remaining - 5)
    end.

clean(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
