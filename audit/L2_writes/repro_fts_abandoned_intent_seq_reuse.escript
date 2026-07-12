#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

main(_) ->
    Root = filename:join(filename:dirname(escript:script_name()),
                         ".repro_fts_abandoned_intent_seq_reuse.data"),
    rm_rf(Root),
    ok = filelib:ensure_dir(filename:join(Root, "sentinel")),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>,
                 columns => [body]}],
    Opts = [{root_path, Root}, {fts_indexes, Indexes}],
    {ok, Bookie1} = leveled_bookie:book_plainstart(Opts),

    %% Model a writer that finishes RESOLVE and then exits before its journal
    %% write.  The allocated FTS sequence exists only in Bookie memory.
    Parent = self(),
    {Writer, Mon} = spawn_monitor(fun() ->
        {ok, Seq, _Schemas, _Inker} =
            gen_server:call(Bookie1, {fts_put_intent}, infinity),
        Parent ! {abandoned_seq, Seq}
    end),
    receive {abandoned_seq, AbandonedSeq} -> ok end,
    receive {'DOWN', Mon, process, Writer, normal} -> ok end,

    %% This durable write is stamped one above the abandoned sequence, even
    %% though it is journal SQN 1.
    ok = leveled_bookie:book_put(
             Bookie1, <<"docs">>, <<"doc">>, #{body => <<"alpha">>}, [], o,
             infinity, true),
    {ok, JournalSQN1} = leveled_bookie:book_journalsqn(Bookie1),
    ok = leveled_bookie:book_close(Bookie1),

    %% Restart seeds fts_seq from the journal SQN, forgetting the abandoned
    %% allocation.  The update therefore reuses the first write's FTS stamp.
    {ok, Bookie2} = leveled_bookie:book_plainstart(Opts),
    ok = leveled_bookie:book_put(
             Bookie2, <<"docs">>, <<"doc">>, #{body => <<"beta">>}, [], o,
             infinity, true),
    Alpha = search_keys(Bookie2, <<"alpha">>),
    Beta = search_keys(Bookie2, <<"beta">>),
    Body = leveled_bookie:book_get(Bookie2, <<"docs">>, <<"doc">>, o),

    io:format("abandoned FTS sequence: ~p~n", [AbandonedSeq]),
    io:format("journal SQN after first durable write: ~p~n", [JournalSQN1]),
    io:format("canonical object after update: ~p~n", [Body]),
    io:format("expected alpha hits after update: []~n"),
    io:format("actual alpha hits after update:   ~p~n", [Alpha]),
    io:format("expected beta hits after update: [<<\"doc\">>]~n"),
    io:format("actual beta hits after update:   ~p~n", [Beta]),

    ok = leveled_bookie:book_close(Bookie2),
    rm_rf(Root),
    case {AbandonedSeq, JournalSQN1, Body, Alpha, Beta} of
        {1, 1, {ok, #{body := <<"beta">>}}, [<<"doc">>], [<<"doc">>]} ->
            halt(0);
        Other ->
            io:format(standard_error, "BUG DID NOT REPRODUCE: ~p~n", [Other]),
            halt(1)
    end.

search_keys(Bookie, Query) ->
    {async, Run} =
        leveled_bookie:book_ftssearch(Bookie, <<"docs">>, <<"main">>, Query, #{}),
    {ok, Hits} = Run(),
    lists:sort([maps:get(key, Hit) || Hit <- Hits]).

rm_rf(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
