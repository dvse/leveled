#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

main(_) ->
    Root = filename:join(filename:dirname(escript:script_name()),
                         ".repro_reordered_publish_reverts.data"),
    rm_rf(Root),
    ok = filelib:ensure_dir(filename:join(Root, "sentinel")),
    Parent = self(),
    Ref = make_ref(),
    Control = ets:new(reordered_publish_control, [set, public]),
    Extract = fun(_Tag, Size, Object) ->
        case Object of
            slow_first ->
                %% Metadata preparation runs after ink_put has committed SQN 1.
                case ets:insert_new(Control, {blocked_once, true}) of
                    true ->
                        Parent ! {sqn1_appended, self()},
                        receive {release_sqn1, Ref} -> ok end;
                    false ->
                        ok
                end;
            _ ->
                ok
        end,
        {{erlang:phash2(Object), Size, undefined}, []}
    end,
    Opts = [{root_path, Root},
            {override_functions, [{extract_metadata, Extract}]}],
    {ok, Bookie} = leveled_bookie:book_plainstart(Opts),
    Bucket = <<"bucket">>,
    Key = <<"same-key">>,
    Tag = audit_tag,
    First = slow_first,

    %% Writer 1 appends SQN 1 and is then held in caller-side ledger
    %% preparation. Writer 2 appends and publishes SQN 2 first using only the
    %% public book_put API.
    {Writer1, Mon1} = spawn_monitor(fun() ->
        Result = leveled_bookie:book_put(
                     Bookie, Bucket, Key, First, [], Tag, infinity, true),
        Parent ! {writer1_result, Result}
    end),
    receive {sqn1_appended, Writer1} -> ok end,
    ok = leveled_bookie:book_put(
             Bookie, Bucket, Key, second, [], Tag, infinity, true),
    AfterNewerAck = leveled_bookie:book_get(Bookie, Bucket, Key, Tag),
    Writer1 ! {release_sqn1, Ref},
    receive {writer1_result, ok} -> ok end,
    receive {'DOWN', Mon1, process, Writer1, normal} -> ok end,
    AfterOlderPublish = leveled_bookie:book_get(Bookie, Bucket, Key, Tag),

    %% Restart replay is the reference oracle: journal SQN order must restore
    %% the later acknowledged value even though the live ledger cache reverted.
    ok = leveled_bookie:book_close(Bookie),
    {ok, Reopened} = leveled_bookie:book_plainstart(Opts),
    AfterRestart = leveled_bookie:book_get(Reopened, Bucket, Key, Tag),

    io:format("newer writer returned ok and read: ~p~n", [AfterNewerAck]),
    io:format("after delayed older writer returned ok: ~p~n", [AfterOlderPublish]),
    io:format("expected after restart (journal SQN order): {ok,second}~n"),
    io:format("actual after restart:                       ~p~n", [AfterRestart]),

    ok = leveled_bookie:book_close(Reopened),
    application:unset_env(leveled, extract_metadata),
    ets:delete(Control),
    rm_rf(Root),
    case {AfterNewerAck, AfterOlderPublish, AfterRestart} of
        {{ok, second}, {ok, First}, {ok, second}} -> halt(0);
        Other ->
            io:format(standard_error, "BUG DID NOT REPRODUCE: ~p~n", [Other]),
            halt(1)
    end.

rm_rf(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
