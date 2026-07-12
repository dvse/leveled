#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_bm25_position_cap"]),
    ok = reset(Root),
    Schema = #{
        bucket => <<"docs">>,
        tag => o,
        index => <<"main">>,
        columns => [#{name => body, path => [body]}],
        tokenizer => unicode61
    },
    {ok, Bookie} = leveled_bookie:book_start([
        {root_path, Root},
        {compression_method, none},
        {ledger_compression, none},
        {log_level, warning},
        {fts_indexes, [Schema]}
    ]),
    try
        %% One-byte position deltas make 65,525 the persisted tf ceiling.
        %% Both documents contain only the query term, so with uncapped tf
        %% BM25 must score the 70,000-occurrence document slightly higher.
        ok = put_doc(Bookie, <<"a_65000">>, 65000),
        ok = put_doc(Bookie, <<"b_70000">>, 70000),
        {async, Run} = leveled_bookie:book_ftssearch(
            Bookie, <<"docs">>, <<"main">>, <<"z">>,
            #{columns => [body], rank => bm25, limit => 10,
              result_cache => false}
        ),
        {ok, Hits} = Run(),
        Actual = [{maps:get(key, H), maps:get(score, H), maps:get(doc_length, H)} || H <- Hits],
        AvgDl = (65000 + 70000) / 2,
        ExpectedA = bm25(65000, 65000, AvgDl),
        ExpectedB = bm25(70000, 70000, AvgDl),
        ActualOrder = [K || {K, _S, _D} <- Actual],
        ExpectedOrder = [<<"b_70000">>, <<"a_65000">>],
        io:format("expected_order=~p~nactual_order=~p~n", [ExpectedOrder, ActualOrder]),
        io:format("expected_full_tf_scores=[{a_65000,~.16g},{b_70000,~.16g}]~n", [ExpectedA, ExpectedB]),
        io:format("actual_hits=~p~n", [Actual]),
        case ActualOrder of
            [<<"a_65000">>, <<"b_70000">>] ->
                io:format("REPRODUCED: capped positions reverse the full-tf BM25 order~n"),
                ok;
            _ ->
                erlang:error({not_reproduced, Actual})
        end
    after
        leveled_bookie:book_close(Bookie),
        reset(Root)
    end.

put_doc(Bookie, Key, Count) ->
    Body = binary:copy(<<"z ">>, Count),
    leveled_bookie:book_put(
        Bookie, <<"docs">>, Key, #{body => Body}, [], o, infinity, false
    ).

bm25(Tf, Dl, AvgDl) ->
    K1 = 1.2,
    B = 0.75,
    Idf = 1.0e-6,
    Idf * (Tf * (K1 + 1)) /
        (Tf + K1 * (1 - B + B * (Dl / AvgDl))).

reset(Path) ->
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.
