#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

main(_) ->
    Root = tmp_dir("mhead_headonly"),
    ok = file:make_dir(Root),
    {ok, Bookie} = leveled_bookie:book_start([
        {root_path, Root},
        {head_only, with_lookup}
    ]),
    Bucket = <<"bucket">>,
    Key = <<"key">>,
    SubKey = <<"subkey">>,
    CompositeKey = {Key, SubKey},
    ok = leveled_bookie:book_mput(
        Bookie,
        [{add, Bucket, Key, SubKey, stored_head_value}]
    ),
    Single = leveled_bookie:book_head(Bookie, Bucket, CompositeKey, h),
    Expected = [{CompositeKey, Single}],
    Actual = leveled_bookie:book_mhead(Bookie, Bucket, [CompositeKey], h),
    io:format("expected=~p~nactual=~p~n", [Expected, Actual]),
    ok = leveled_bookie:book_destroy(Bookie),
    case Actual =:= Expected of
        true ->
            io:format("UNEXPECTED PASS~n"),
            halt(1);
        false ->
            io:format("REPRODUCED: plural HEAD is unsupported although singular HEAD succeeds~n"),
            halt(0)
    end.

tmp_dir(Slug) ->
    filename:join(
        "/tmp",
        lists:flatten(io_lib:format("leveled_l1_~s_~B_~B", [
            Slug,
            erlang:system_time(microsecond),
            erlang:unique_integer([positive])
        ]))
    ).
