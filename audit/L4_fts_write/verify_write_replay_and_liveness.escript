#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    Root = unique_root("write_replay_liveness"),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>, columns => [body]}],
    Opts = [{root_path, Root}, {fts_indexes, Indexes}],
    try
        {ok, Bookie} = leveled_bookie:book_start(Opts),
        ok = put(Bookie, caller, <<"caller_live">>, <<"alpha common">>),
        ok = put(Bookie, direct, <<"direct_live">>, <<"alpha common">>),
        ok = put(Bookie, caller, <<"overwrite">>, <<"obsolete common">>),
        ok = put(Bookie, direct, <<"overwrite">>, <<"fresh common">>),
        ok = put(Bookie, direct, <<"deleted">>, <<"dead common">>),
        ok = leveled_bookie:book_delete(Bookie, <<"docs">>, <<"deleted">>, []),

        ok = leveled_bookie:book_mput_std(Bookie, [
            {put, <<"docs">>, <<"batch_caller">>, #{body => <<"batch common">>}, [], o, infinity}
        ]),
        ok = leveled_bookie:book_mput_std_direct(
            Bookie,
            [
                {put, <<"docs">>, <<"batch_direct">>, #{body => <<"batch common">>}, [], o, infinity}
            ],
            false
        ),

        Queries = [<<"alpha">>, <<"obsolete">>, <<"fresh">>, <<"dead">>, <<"batch">>, <<"common">>],
        Before = snapshot(Bookie, Queries),
        [] = maps:get(<<"obsolete">>, Before),
        [] = maps:get(<<"dead">>, Before),
        [<<"overwrite">>] = maps:get(<<"fresh">>, Before),
        [<<"caller_live">>, <<"direct_live">>] = maps:get(<<"alpha">>, Before),

        {async, Consolidate} = leveled_bookie:book_ftsconsolidate(
            Bookie, <<"docs">>, <<"main">>, #{}
        ),
        ok = Consolidate(),
        Before = snapshot(Bookie, Queries),
        ok = leveled_bookie:book_close(Bookie),

        {ok, Restarted} = leveled_bookie:book_start(Opts),
        After = snapshot(Restarted, Queries),
        Before = After,
        ok = leveled_bookie:book_close(Restarted),
        io:format("search_snapshot=~p~n", [After]),
        io:format("PASS caller/direct, overwrite/delete liveness, consolidation, and restart equality~n", []),
        ok
    after
        cleanup(Root)
    end.

put(Bookie, caller, Key, Body) ->
    leveled_bookie:book_put(Bookie, <<"docs">>, Key, #{body => Body}, []);
put(Bookie, direct, Key, Body) ->
    leveled_bookie:book_put_direct(
        Bookie, <<"docs">>, Key, #{body => Body}, [], o, infinity, false
    ).

snapshot(Bookie, Queries) ->
    maps:from_list([{Query, search_keys(Bookie, Query)} || Query <- Queries]).

search_keys(Bookie, Query) ->
    {async, Runner} = leveled_bookie:book_ftssearch(
        Bookie,
        <<"docs">>,
        <<"main">>,
        Query,
        #{result_cache => false}
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
