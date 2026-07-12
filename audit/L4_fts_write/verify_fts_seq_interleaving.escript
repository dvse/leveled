#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

-define(FTS_SOURCE, "/Users/dvse/projects/agents/leveled/src/leveled_fts.erl").
-define(BOOKIE_SOURCE, "/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl").
-define(INCLUDE_DIR, "/Users/dvse/projects/agents/leveled/include").

main(_) ->
    ok = load_export_all(leveled_fts, ?FTS_SOURCE),
    ok = load_export_all(leveled_bookie, ?BOOKIE_SOURCE),
    Root = unique_root("fts_seq_interleaving"),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>, columns => [body]}],
    Opts = [{root_path, Root}, {fts_indexes, Indexes}],
    try
        {ok, Bookie} = leveled_bookie:book_start(Opts),
        Held = [
            gen_server:call(Bookie, {fts_put_intent}, infinity)
         || _ <- lists:seq(1, 5)
        ],
        lists:foreach(
            fun(I) ->
                Key = <<"direct", (integer_to_binary(I))/binary>>,
                ok = leveled_bookie:book_put_direct(
                    Bookie,
                    <<"docs">>,
                    Key,
                    #{body => <<"direct common">>},
                    [],
                    o,
                    infinity,
                    false
                )
            end,
            lists:seq(1, 5)
        ),
        Fresh = gen_server:call(Bookie, {fts_put_intent}, infinity),
        HeldSeqs = [Seq || {ok, Seq, _Ix, _Inker} <- Held],
        {ok, FreshSeq, _FreshIx, _FreshInker} = Fresh,
        false = lists:member(FreshSeq, HeldSeqs),
        true = FreshSeq > lists:max(HeldSeqs),

        lists:foreach(
            fun({I, {ok, Seq, NormalIndexes, Inker}}) ->
                Key = <<"held", (integer_to_binary(I))/binary>>,
                LK = leveled_codec:to_objectkey(<<"docs">>, Key, o),
                Object = #{body => <<"held common">>},
                {ok, Augmented, Touched} =
                    leveled_fts:augment_object_changes(
                        [{LK, Object, {[], infinity}}], NormalIndexes, Seq
                    ),
                Markers = leveled_fts:marker_cache_updates(
                    Augmented, NormalIndexes
                ),
                ok = leveled_bookie:publish_fts_changes(
                    Bookie,
                    Inker,
                    Augmented,
                    {Touched, Seq, Seq - 1, Markers},
                    false
                )
            end,
            lists:zip(lists:seq(1, 6), Held ++ [Fresh])
        ),

        %% An intent with no journal append is only an embedded-sequence leak;
        %% it must not open the journal absorption frontier or stall a writer.
        {ok, LeakedSeq, _LeakedIx, _LeakedInker} =
            gen_server:call(Bookie, {fts_put_intent}, infinity),
        ok = leveled_bookie:book_put(
            Bookie,
            <<"docs">>,
            <<"post_intent_leak">>,
            #{body => <<"common">>},
            []
        ),

        KeysBefore = search_keys(Bookie),
        12 = length(KeysBefore),
        ok = leveled_bookie:book_close(Bookie),
        {ok, Restarted} = leveled_bookie:book_start(Opts),
        KeysAfter = search_keys(Restarted),
        KeysBefore = KeysAfter,
        ok = leveled_bookie:book_close(Restarted),
        io:format("held_seqs=~p fresh_seq=~p leaked_seq=~p~n", [
            HeldSeqs, FreshSeq, LeakedSeq
        ]),
        io:format("searchable_before_and_after_restart=~p~n", [KeysAfter]),
        io:format("PASS no posting-carrier collision or intent-only stall~n", []),
        ok
    after
        cleanup(Root)
    end.

search_keys(Bookie) ->
    {async, Runner} = leveled_bookie:book_ftssearch(
        Bookie,
        <<"docs">>,
        <<"main">>,
        <<"common">>,
        #{result_cache => false}
    ),
    {ok, Hits} = Runner(),
    lists:sort([maps:get(key, Hit) || Hit <- Hits]).

load_export_all(Module, Source) ->
    Result = compile:file(Source, [binary, export_all, {i, ?INCLUDE_DIR}]),
    Binary =
        case Result of
            {ok, Module, Bin} -> Bin;
            {ok, Module, Bin, _Warnings} -> Bin
        end,
    _ = code:purge(Module),
    _ = code:delete(Module),
    {module, Module} = code:load_binary(Module, Source, Binary),
    ok.

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
