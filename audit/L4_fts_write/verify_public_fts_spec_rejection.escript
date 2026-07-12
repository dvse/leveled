#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    Root = unique_root("public_fts_spec_rejection"),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>, columns => [body]}],
    Opts = [{root_path, Root}, {fts_indexes, Indexes}],
    Forged = [
        {add_payload,
            {fts_term, <<"main">>, o},
            <<0:16/unsigned-big, 1:8, 1:64/unsigned-big>>,
            <<"forged-internal-delta">>}
    ],
    try
        {ok, Bookie} = leveled_bookie:book_start(Opts),
        Future = calendar:datetime_to_gregorian_seconds(calendar:universal_time()) + 3600,
        Results = [
            {book_put,
                leveled_bookie:book_put(
                    Bookie, <<"docs">>, <<"put">>, #{body => <<"x">>}, Forged
                )},
            {book_put_direct,
                leveled_bookie:book_put_direct(
                    Bookie, <<"docs">>, <<"direct">>, #{body => <<"x">>}, Forged,
                    o, infinity, false
                )},
            {book_tempput,
                leveled_bookie:book_tempput(
                    Bookie, <<"docs">>, <<"temp">>, #{body => <<"x">>}, Forged,
                    o, Future
                )},
            {book_delete,
                leveled_bookie:book_delete(
                    Bookie, <<"docs">>, <<"delete">>, Forged
                )},
            {book_mput_std,
                leveled_bookie:book_mput_std(Bookie, [
                    {put, <<"docs">>, <<"batch">>, #{body => <<"x">>}, Forged, o, infinity}
                ])},
            {book_mput_std_direct,
                leveled_bookie:book_mput_std_direct(
                    Bookie,
                    [
                        {put, <<"docs">>, <<"batch_direct">>, #{body => <<"x">>}, Forged, o, infinity}
                    ],
                    false
                )},
            {book_casput,
                leveled_bookie:book_casput(
                    Bookie, <<"docs">>, <<"cas">>, #{body => <<"x">>}, Forged,
                    o, infinity, false, absent
                )},
            {book_casmput,
                leveled_bookie:book_casmput(
                    Bookie,
                    [
                        {put, <<"docs">>, <<"cas_batch">>, #{body => <<"x">>}, Forged, o, infinity}
                    ],
                    [{<<"docs">>, <<"cas_batch">>, o, absent}],
                    false
                )}
        ],
        lists:foreach(
            fun({_Path, Result}) -> {error, invalid_index_specs} = Result end,
            Results
        ),
        io:format("rejections=~p~n", [Results]),
        io:format("PASS forged fts_term payload rejected on every standard write surface~n", []),
        ok = leveled_bookie:book_close(Bookie),
        ok
    after
        cleanup(Root)
    end.

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
