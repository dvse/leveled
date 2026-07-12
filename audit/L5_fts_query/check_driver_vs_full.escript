#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin -pa /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

main(_) ->
    application:ensure_all_started(crypto),
    Root = filename:join([filename:dirname(escript:script_name()), "tmp_driver_vs_full"]),
    ok = reset(Root),
    Schema = #{
        bucket => <<"docs">>, tag => o, index => <<"main">>,
        columns => [
            #{name => title, path => [title]},
            #{name => body, path => [body]}
        ], tokenizer => unicode61
    },
    {ok, B} = leveled_bookie:book_start([
        {root_path, Root}, {compression_method, none},
        {ledger_compression, none}, {log_level, warning},
        {fts_indexes, [Schema]}
    ]),
    try
        Entries = [doc(N) || N <- lists:seq(1, 300)],
        ok = leveled_bookie:book_mput_std(B, Entries, false),
        Queries = [
            <<"alpha AND rare">>,
            <<"alpha OR unicorn">>,
            <<"alpha NOT gamma">>,
            <<"\"alpha beta\"">>,
            <<"NEAR(alpha delta, 3)">>,
            <<"alp* AND delta">>,
            <<"title:alpha OR body:unicorn">>,
            <<"(alpha OR beta) NOT (gamma AND delta)">>,
            <<"NEAR(\"alpha beta\" delta, 4)">>
        ],
        compare(B, Queries),
        {async, Cons} = leveled_bookie:book_ftsconsolidate(B, <<"docs">>, <<"main">>, #{}),
        ok = Cons(),
        compare(B, Queries),
        ok = leveled_bookie:book_close(B),
        {ok, B2} = leveled_bookie:book_start([
            {root_path, Root}, {compression_method, none},
            {ledger_compression, none}, {log_level, warning},
            {fts_indexes, [Schema]}
        ]),
        try
            compare(B2, Queries),
            io:format(
                "PASS: driver-optimized and forced all-terms-loaded match sets "
                "agreed for ~p ASTs before/after consolidation and restart~n",
                [length(Queries)]
            )
        after
            leveled_bookie:book_close(B2)
        end
    after
        safe_close(B),
        reset(Root)
    end.

compare(B, Queries) ->
    lists:foreach(
        fun(Q0) ->
            %% Quoted allmarker is a phrase leaf. In rank=>bm25 it forces
            %% ranked_term_metas/full mode, while rank=>none uses drivers.
            Q = <<"(", Q0/binary, ") AND \"allmarker\"">>,
            Driver = keys(query(B, Q, none)),
            Full = keys(query(B, Q, bm25)),
            Driver = Full
        end,
        Queries
    ).

query(B, Q, Rank) ->
    {async, Run} = leveled_bookie:book_ftssearch(
        B, <<"docs">>, <<"main">>, Q,
        #{columns => [title, body], rank => Rank, limit => 20000,
          result_cache => false}
    ),
    {ok, Hits} = Run(), Hits.

keys(Hits) -> lists:sort([maps:get(key, H) || H <- Hits]).

doc(N) ->
    Title = case N rem 4 of 0 -> <<"alpha rare allmarker">>; 1 -> <<"beta allmarker">>;
        2 -> <<"alpha unicorn allmarker">>; 3 -> <<"gamma allmarker">> end,
    Body = case N rem 5 of 0 -> <<"alpha beta delta allmarker">>;
        1 -> <<"alpha x delta allmarker">>; 2 -> <<"gamma delta allmarker">>;
        3 -> <<"unicorn rare allmarker">>; 4 -> <<"beta gamma allmarker">> end,
    {put, <<"docs">>, key(N), #{title => Title, body => Body}, [], o, infinity}.

key(N) -> <<"k", (integer_to_binary(N))/binary>>.

reset(Path) ->
    case file:del_dir_r(Path) of ok -> ok; {error, enoent} -> ok end.

safe_close(Pid) ->
    try leveled_bookie:book_close(Pid) of _ -> ok catch exit:_ -> ok end.
