#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

%% The supplied prebuilt leveled_bookie.beam predates the value-cache commit.
%% Compile a copied HEAD source into /tmp, as permitted by COMMON_BRIEF.md.
main(_) ->
    process_flag(trap_exit, true),
    logger:set_primary_config(level, emergency),
    Build = tmp_dir("valuecache_build"),
    ok = file:make_dir(Build),
    Source = "/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl",
    Include = "/Users/dvse/projects/agents/leveled/include",
    Copy = filename:join(Build, "leveled_bookie.erl"),
    {ok, _} = file:copy(Source, Copy),
    CompileResult = compile:file(Copy, [
        {i, Include},
        {outdir, Build},
        return_errors,
        return_warnings
    ]),
    ok = case CompileResult of
        {ok, leveled_bookie} -> ok;
        {ok, leveled_bookie, _Warnings} -> ok
    end,
    true = code:add_patha(Build),
    {module, leveled_bookie} = code:load_abs(filename:join(Build, "leveled_bookie")),

    Before = cache_terms(),
    Attempts = 4,
    Results = [failed_start(N) || N <- lists:seq(1, Attempts)],
    KillResult = killed_start(),
    After = cache_terms(),
    Leaked = length(After) - length(Before),
    io:format("failed_start_results=~p~n", [Results]),
    io:format("kill_cleanup_result=~p~n", [KillResult]),
    io:format("cache_terms_before=~B after=~B leaked=~B~n", [
        length(Before), length(After), Leaked
    ]),
    lists:foreach(
        fun({Key, _Value}) -> persistent_term:erase(Key) end,
        After -- Before
    ),
    rm_rf(Build),
    case Leaked =:= Attempts + 1 andalso KillResult =:= leaked of
        true ->
            io:format("REPRODUCED: failed init and untrappable death leak persistent_term entries~n"),
            halt(0);
        false ->
            io:format("UNEXPECTED RESULT~n"),
            halt(1)
    end.

killed_start() ->
    Root = tmp_dir("killed_bookie"),
    {ok, Bookie} = leveled_bookie:book_plainstart([
        {root_path, Root},
        {value_cache_size, 1048576}
    ]),
    Key = {leveled_bookie, valuecache, Bookie},
    {_Tid, 1048576} = persistent_term:get(Key),
    Monitor = erlang:monitor(process, Bookie),
    exit(Bookie, kill),
    receive
        {'DOWN', Monitor, process, Bookie, killed} -> ok
    after 5000 ->
        error(kill_timeout)
    end,
    Result =
        case persistent_term:get(Key, undefined) of
            undefined -> cleaned;
            _Stale -> leaked
        end,
    rm_rf(Root),
    Result.

failed_start(N) ->
    BadRoot = tmp_dir(lists:flatten(io_lib:format("badroot_~B", [N]))),
    ok = file:write_file(BadRoot, <<"this is a file, not a directory">>),
    Result0 = leveled_bookie:book_start([
        {root_path, BadRoot},
        {value_cache_size, 1048576}
    ]),
    ok = file:delete(BadRoot),
    case Result0 of
        {error, _Reason} -> error;
        Other -> Other
    end.

cache_terms() ->
    [
        Entry
     || Entry = {{leveled_bookie, valuecache, _Pid}, _CacheInfo} <- persistent_term:get()
    ].

tmp_dir(Slug) ->
    filename:join(
        "/tmp",
        lists:flatten(io_lib:format("leveled_l1_~s_~B_~B", [
            Slug,
            erlang:system_time(microsecond),
            erlang:unique_integer([positive])
        ]))
    ).

rm_rf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            lists:foreach(fun rm_rf/1, filelib:wildcard(filename:join(Path, "*"))),
            file:del_dir(Path);
        false ->
            file:delete(Path)
    end,
    ok.
