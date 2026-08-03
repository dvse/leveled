#!/usr/bin/env escript
%%! -noshell

-mode(compile).

-define(BUCKET, <<"bench">>).
-define(MAX_LIMIT, 20000).

main([Command | Args]) ->
    try
        Opts = parse_args(Args, #{}),
        Result = run(list_to_atom(Command), Opts),
        io:put_chars([json(Result), $\n])
    catch
        Class:Reason:Stack ->
            io:format(standard_error, "leveled_bench ~p:~p~n~p~n", [
                Class, Reason, Stack
            ]),
            halt(2)
    end;
main(_) ->
    io:put_chars(standard_error,
        "usage: leveled_bench.erl <fts|index|heads|compression> --root PATH ...\n"),
    halt(2).

run(fts, Opts) -> run_fts(Opts);
run(index, Opts) -> run_index(Opts);
run(heads, Opts) -> run_heads(Opts);
run(compression, Opts) -> run_compression(Opts);
run(Command, _Opts) -> erlang:error({unknown_subcommand, Command}).

%% ------------------------------------------------------------------
%% FTS-2: retrieval and hydration are timed as two public-seam phases.
%% ------------------------------------------------------------------

run_fts(Opts) ->
    Root = required(root, Opts),
    Tsv = required(tsv, Opts),
    QueriesPath = required(queries, Opts),
    Reuse = atom_opt(reuse, Opts, false),
    case Reuse of false -> reset_root(Root); true -> ok end,
    StoreConfig = atom_opt(store_config, Opts, live),
    {StartOpts, StoreConfigReport} = fts_start_options(Root, StoreConfig),
    {ok, Bookie0} = leveled_bookie:book_start(StartOpts),
    {ok, Schema0} = leveled_fts:schema(#{
        index => ?BUCKET,
        columns => [content],
        text_field => content,
        hit_fields => [udi, path, ipath_vec, content_version],
        candidate_fields => [udi, path, ipath_vec, content_version],
        candidate_group_fields => [udi, path, ipath_vec, content_version],
        candidate_version_field => content_version,
        prefixes => [5, 11],
        remove_diacritics => 1
    }),
    Schema = Schema0#{trim_journal => true},
    {Count, TextBytes, DeriveUs, WriteUs, Sentinel, LoadUs,
        ConsolidateUs} = case Reuse of
        false ->
            LoadStart = now_us(),
            {LoadedCount, LoadedBytes, LoadedDeriveUs, LoadedWriteUs,
                LoadedSentinel} = load_fts(Tsv, Bookie0, Schema),
            LoadedUs = now_us() - LoadStart,
            ConsolidateStart = now_us(),
            {ok, #{skipped := []}} = leveled_fts:consolidate(
                Bookie0, Schema, #{}
            ),
            {LoadedCount, LoadedBytes, LoadedDeriveUs, LoadedWriteUs,
                LoadedSentinel, LoadedUs, now_us() - ConsolidateStart};
        true ->
            {ExistingCount, ExistingBytes, ExistingSentinel} =
                fts_tsv_stats(Tsv),
            {ExistingCount, ExistingBytes, 0, 0, ExistingSentinel, 0, 0}
    end,
    ok = leveled_bookie:book_close(Bookie0),
    {OpenUs, {ok, Bookie}} = timer:tc(
        leveled_bookie, book_start, [StartOpts]
    ),
    Rank = atom_opt(rank, Opts, bm25),
    Regime = atom_opt(regime, Opts, served),
    Limit = int_opt(limit, Opts, 20),
    Runs = int_opt(runs, Opts, 7),
    Warmups = int_opt(warmups, Opts, 3),
    Cases = [
        measure_fts_case(
            Bookie, Schema, Query, Rank, Regime, Limit, Runs, Warmups,
            Sentinel
        )
     || Query <- read_queries(QueriesPath)
    ],
    ok = leveled_bookie:book_close(Bookie),
    #{
        schema_version => 1,
        engine => leveled,
        subcommand => fts,
        rank => Rank,
        regime => Regime,
        store_config => StoreConfigReport,
        limit => Limit,
        documents => Count,
        text_bytes => TextBytes,
        ingest => #{
            load_us => LoadUs,
            derive_us => DeriveUs,
            write_us => WriteUs,
            consolidate_us => ConsolidateUs
        },
        open_us => OpenUs,
        store_bytes => directory_bytes(Root),
        cases => Cases
    }.

load_fts(Path, Bookie, Schema) ->
    {ok, File} = file:open(Path, [read, raw, binary, {read_ahead, 1048576}]),
    try load_fts_lines(File, Bookie, Schema, 0, 0, 0, 0, undefined)
    after ok = file:close(File) end.

load_fts_lines(File, Bookie, Schema, Count, Bytes, DeriveUs, WriteUs, Sentinel) ->
    case file:read_line(File) of
        eof -> {Count, Bytes, DeriveUs, WriteUs, Sentinel};
        {ok, RawLine} ->
            Line = string:trim(RawLine, trailing, "\n"),
            [Key64, Udi64, VersionBin, Content64] =
                binary:split(Line, <<"\t">>, [global]),
            Key = base64:decode(Key64),
            Content = base64:decode(Content64),
            Object = #{
                content => Content,
                udi => base64:decode(Udi64),
                path => base64:decode(Udi64),
                ipath_vec => [],
                content_version => binary_to_integer(VersionBin)
            },
            {ThisDeriveUs, {ok, Specs}} = timer:tc(
                leveled_fts, derive, [Schema, Key, Object]
            ),
            {ThisWriteUs, WriteResult} = timer:tc(
                leveled_bookie, book_mput, [Bookie, Specs]
            ),
            true = WriteResult =:= ok orelse WriteResult =:= pause,
            NextSentinel = case Sentinel of
                undefined -> {Key, Object};
                _ -> Sentinel
            end,
            load_fts_lines(
                File, Bookie, Schema, Count + 1, Bytes + byte_size(Content),
                DeriveUs + ThisDeriveUs, WriteUs + ThisWriteUs, NextSentinel
            )
    end.

fts_tsv_stats(Path) ->
    {ok, File} = file:open(Path, [read, raw, binary, {read_ahead, 1048576}]),
    try fts_tsv_stats(File, 0, 0, undefined)
    after ok = file:close(File) end.

fts_tsv_stats(File, Count, Bytes, Sentinel) ->
    case file:read_line(File) of
        eof -> {Count, Bytes, Sentinel};
        {ok, RawLine} ->
            Line = string:trim(RawLine, trailing, "\n"),
            [Key64, Udi64, VersionBin, Content64] =
                binary:split(Line, <<"\t">>, [global]),
            Content = base64:decode(Content64),
            NextSentinel = case Sentinel of
                undefined ->
                    {base64:decode(Key64), #{
                        content => Content,
                        udi => base64:decode(Udi64),
                        path => base64:decode(Udi64),
                        ipath_vec => [],
                        content_version => binary_to_integer(VersionBin)
                    }};
                _ -> Sentinel
            end,
            fts_tsv_stats(
                File, Count + 1, Bytes + byte_size(Content), NextSentinel
            )
    end.

measure_fts_case(
    Bookie, Schema, #{label := Label, group := Group, ours := Query}, Rank,
    Regime, Limit, Runs, Warmups, Sentinel
) ->
    lists:foreach(
        fun(_) ->
            maybe_dirty(Bookie, Schema, Regime, Sentinel),
            {_Retrieval, _Hydration} = fts_phases(
                Bookie, Schema, Query, Rank, Regime, Limit
            )
        end,
        lists:seq(1, Warmups)
    ),
    Samples = [
        begin
            maybe_dirty(Bookie, Schema, Regime, Sentinel),
            {Retrieval, Hydration} = fts_phases(
                Bookie, Schema, Query, Rank, Regime, Limit
            ),
            #{
                retrieval_us => maps:get(wall_us, Retrieval),
                hydration_us => maps:get(wall_us, Hydration),
                combined_us => maps:get(wall_us, Retrieval) +
                    maps:get(wall_us, Hydration)
            }
        end
     || _ <- lists:seq(1, Runs)
    ],
    {FinalRetrieval, FinalHydration} = fts_phases(
        Bookie, Schema, Query, Rank, Regime, Limit
    ),
    {FullUs, {ok, Full}} = timer:tc(
        leveled_fts,
        search,
        [Bookie, Schema, Query, retrieval_opts(Rank, Regime, ?MAX_LIMIT)]
    ),
    FullHits = hydrate_search_page(
        Bookie, Schema, Query, Regime, maps:get(hits, Full)
    ),
    FullIdentities = lists:sort([
        maps:get(udi, maps:get(candidate_record, Hit))
     || Hit <- FullHits
    ]),
    Total = maps:get(total, FinalRetrieval),
    Returned = maps:get(returned, FinalRetrieval),
    #{
        label => Label,
        group => Group,
        query => Query,
        limit => Limit,
        total => Total,
        returned => Returned,
        snippet_rows => maps:get(snippet_rows, FinalHydration),
        snippet_content_verified => maps:get(
            snippet_content_verified, FinalHydration
        ),
        result_sha256 => sha_lines(FullIdentities),
        full_set_us => FullUs,
        retrieval_us => median([maps:get(retrieval_us, S) || S <- Samples]),
        hydration_us => median([maps:get(hydration_us, S) || S <- Samples]),
        combined_us => median([maps:get(combined_us, S) || S <- Samples]),
        marginal_us_per_row => marginal(
            [maps:get(combined_us, S) || S <- Samples], Returned
        )
    }.

fts_phases(Bookie, Schema, Query, Rank, Regime, Limit) ->
    {RetrievalUs, {ok, Retrieval}} = timer:tc(
        leveled_fts,
        search,
        [Bookie, Schema, Query, retrieval_opts(Rank, Regime, Limit)]
    ),
    Hits = maps:get(hits, Retrieval),
    {HydrationUs, {ok, Hydrated}} = timer:tc(
        fun() ->
            hydrate_with_snippets(
                Bookie, Schema, Query,
                hydrate_search_page(Bookie, Schema, Query, Regime, Hits)
            )
        end
    ),
    {
        #{
            wall_us => RetrievalUs,
            total => maps:get(count, Retrieval),
            returned => length(Hits)
        },
        #{
            wall_us => HydrationUs,
            snippet_rows => length(Hydrated),
            snippet_content_verified => length([
                ok
             || #{snippet_sha256 := Digest} <- Hydrated,
                byte_size(Digest) =:= 64
            ])
        }
    }.

retrieval_opts(Rank, Regime, Limit) ->
    Base = #{
        columns => [content],
        rank => Rank,
        limit => Limit,
        offset => 0,
        return_count => true,
        resolve_hits => false,
        return_positions => false,
        return_terms => false
    },
    case Regime of
        bare -> Base#{rank_tie_fields => []};
        served -> Base#{
            page_only => true,
            rank_tie_fields => [udi, path, ipath_vec, content_version]
        };
        dirty_tail -> Base#{rank_tie_fields => [udi]}
    end.

hydrate_search_page(_Bookie, _Schema, _Query, _Regime, []) -> [];
hydrate_search_page(Bookie, Schema, _Query, served, Hits) ->
    Addresses = [
        {maps:get(group_id, Hit), maps:get(chunk_id, Hit)}
     || Hit <- Hits,
        maps:is_key(group_id, Hit),
        maps:is_key(chunk_id, Hit)
    ],
    {ok, Rows} = leveled_fts:hydrate_page(Bookie, Schema, Addresses),
    [
        maps:merge(
            Hit,
            maps:get(
                {maps:get(group_id, Hit), maps:get(chunk_id, Hit)}, Rows, #{}
            )
        )
     || Hit <- Hits
    ];
hydrate_search_page(Bookie, Schema, Query, _Regime, Hits) ->
    Keys = [maps:get(candidate_key, Hit) || Hit <- Hits],
    {ok, Hydrated} = leveled_fts:posting_read(
        Bookie,
        Schema,
        Query,
        Keys,
        #{resolve_hits => true, return_positions => false}
    ),
    Hydrated.

hydrate_with_snippets(_Bookie, _Schema, _Query, []) -> {ok, []};
hydrate_with_snippets(Bookie, Schema, _Query, Hits) ->
    Requests = [snippet_request(Hit) || Hit <- Hits],
    Blocks = case leveled_fts:text_blocks_batch(Bookie, Schema, Requests) of
        {ok, FoundBlocks} -> FoundBlocks;
        {error, Reason} -> erlang:error({snippet_hydration_failed, Requests, Reason})
    end,
    Snippets = [
        begin
            DocId = maps:get(doc_id, Hit),
            {_DocId, BlockNo, _LastBlock} = snippet_request(Hit),
            Text = maps:get({DocId, BlockNo}, Blocks, <<>>),
            true = byte_size(Text) > 0,
            #{key => maps:get(key, Hit), snippet_sha256 => sha(Text)}
        end
     || Hit <- Hits
    ],
    {ok, Snippets}.

snippet_request(Hit) ->
    Directory = maps:get(text_blocks, Hit, []),
    BlockNo = case Directory of
        [{FirstBlock, _FirstOrdinal, _Byte} | _] -> FirstBlock;
        [] -> 0
    end,
    {maps:get(doc_id, Hit), BlockNo, BlockNo}.

maybe_dirty(Bookie, Schema, dirty_tail, {Key, Object}) ->
    {ok, Specs} = leveled_fts:derive(Schema, Key, Object),
    ok = leveled_bookie:book_mput(Bookie, Specs);
maybe_dirty(_Bookie, _Schema, _Regime, _Sentinel) -> ok.

%% ------------------------------------------------------------------
%% Current generic-index, batched-head, and compression paths.
%% ------------------------------------------------------------------

run_index(Opts) ->
    Root = required(root, Opts),
    Count = int_opt(count, Opts, 10000),
    reset_root(Root),
    {ok, Bookie} = leveled_bookie:book_start(
        object_start_options(Root, none)
    ),
    {PutUs, ok} = timer:tc(fun() ->
        lists:foreach(
            fun(I) ->
                Key = integer_to_binary(I),
                ok = leveled_bookie:book_put(
                    Bookie, ?BUCKET, Key, <<I:64>>, [
                        {add, <<"value_int">>, I rem 1000}
                    ]
                )
            end,
            lists:seq(1, Count)
        )
    end),
    Fold = fun(_Bucket, {_Value, _Key}, Acc) -> Acc + 1 end,
    {QueryUs, {async, Runner}} = timer:tc(
        leveled_bookie,
        book_indexfold,
        [Bookie, ?BUCKET, {Fold, 0}, {<<"value_int">>, 100, 199},
            {true, undefined}]
    ),
    {RunUs, Returned} = timer:tc(Runner),
    ok = leveled_bookie:book_close(Bookie),
    #{
        schema_version => 1, engine => leveled, subcommand => index,
        count => Count, put_us => PutUs, prepare_us => QueryUs,
        query_us => RunUs, returned => Returned,
        store_bytes => directory_bytes(Root)
    }.

run_heads(Opts) ->
    Root = required(root, Opts),
    Count = int_opt(count, Opts, 10000),
    BatchSizes = int_list_opt(batches, Opts, [1, 20, 200, 1000]),
    reset_root(Root),
    {ok, Bookie} = leveled_bookie:book_start(start_options(Root, none)),
    Specs = [
        {add, ?BUCKET, <<I:64>>, <<0:32>>, <<I:64>>}
     || I <- lists:seq(1, Count)
    ],
    ok = put_slices(Bookie, Specs),
    Rows = [
        begin
            Requests = [
                {<<I:64>>, <<0:32>>}
             || I <- lists:seq(1, erlang:min(Batch, Count))
            ],
            {Us, Values} = timer:tc(
                leveled_bookie, book_headonly_many,
                [Bookie, ?BUCKET, Requests]
            ),
            #{batch => Batch, wall_us => Us, returned => length(Values)}
        end
     || Batch <- BatchSizes
    ],
    ok = leveled_bookie:book_close(Bookie),
    #{
        schema_version => 1, engine => leveled, subcommand => heads,
        count => Count, rows => Rows, store_bytes => directory_bytes(Root)
    }.

run_compression(Opts) ->
    Root = required(root, Opts),
    Count = int_opt(count, Opts, 10000),
    Bytes = int_opt(bytes, Opts, 1024),
    Method = atom_opt(compression, Opts, native),
    reset_root(Root),
    {ok, Bookie} = leveled_bookie:book_start(
        object_start_options(Root, Method)
    ),
    Value = binary:copy(<<"compressible-benchmark-payload-">>,
        (Bytes div 31) + 1),
    Payload = binary:part(Value, 0, Bytes),
    {PutUs, ok} = timer:tc(fun() ->
        lists:foreach(
            fun(I) ->
                ok = leveled_bookie:book_put(
                    Bookie, ?BUCKET, <<I:64>>, Payload, []
                )
            end,
            lists:seq(1, Count)
        )
    end),
    ok = leveled_bookie:book_close(Bookie),
    #{
        schema_version => 1, engine => leveled,
        subcommand => compression, method => Method, count => Count,
        value_bytes => Bytes, put_us => PutUs,
        store_bytes => directory_bytes(Root)
    }.

put_slices(_Bookie, []) -> ok;
put_slices(Bookie, Specs) ->
    {Slice, Rest} = lists:split(erlang:min(200, length(Specs)), Specs),
    ok = leveled_bookie:book_mput(Bookie, Slice),
    put_slices(Bookie, Rest).

%% ------------------------------------------------------------------
%% Input, options, measurements, and deterministic JSON output.
%% ------------------------------------------------------------------

read_queries(Path) ->
    {ok, Bin} = file:read_file(Path),
    [
        begin
            [Label, Group, Ours64, _Sqlite64] =
                binary:split(Line, <<"\t">>, [global]),
            #{
                label => Label,
                group => Group,
                ours => base64:decode(Ours64)
            }
        end
     || Line <- binary:split(Bin, <<"\n">>, [global]),
        Line =/= <<>>
    ].

start_options(Root, Compression) ->
    [
        {root_path, Root},
        {head_only, with_lookup},
        {sync_strategy, none},
        {compression_method, Compression},
        {ledger_compression, none},
        {database_id, erlang:phash2(Root, 65535) + 1},
        {log_level, warning}
    ].

fts_start_options(Root, live) ->
    {
        [
            {root_path, Root},
            {head_only, with_lookup},
            {sync_strategy, none},
            {compression_method, native},
            {ledger_compression, as_store},
            {max_pencillercachesize, 8000},
            {max_sstslots, 256},
            {max_mergebelow, 24},
            {cache_size, 2500},
            {block_version, 2},
            {database_id, erlang:phash2(Root, 65535) + 1},
            {log_level, warning}
        ],
        #{
            name => live,
            compression_method => native,
            ledger_compression => native,
            max_pencillercachesize => 8000,
            max_sstslots => 256,
            max_mergebelow => 24,
            cache_size => 2500,
            block_version => 2
        }
    };
fts_start_options(Root, legacy_uncompressed) ->
    {
        start_options(Root, none),
        #{
            name => legacy_uncompressed,
            compression_method => none,
            ledger_compression => none,
            max_pencillercachesize => 28000,
            max_sstslots => 256,
            max_mergebelow => 24,
            cache_size => 2500,
            block_version => 2
        }
    }.

object_start_options(Root, Compression) ->
    [Option || Option <- start_options(Root, Compression),
        element(1, Option) =/= head_only].

parse_args([], Opts) -> Opts;
parse_args(["--" ++ Name, Value | Rest], Opts) ->
    Key = list_to_atom(lists:flatten(string:replace(Name, "-", "_", all))),
    parse_args(Rest, Opts#{Key => Value});
parse_args([Bad | _], _Opts) -> erlang:error({invalid_argument, Bad}).

required(Key, Opts) ->
    case maps:find(Key, Opts) of
        {ok, Value} -> Value;
        error -> erlang:error({missing_argument, Key})
    end.

int_opt(Key, Opts, Default) ->
    case maps:get(Key, Opts, Default) of
        Value when is_integer(Value) -> Value;
        Value -> list_to_integer(Value)
    end.

atom_opt(Key, Opts, Default) ->
    case maps:get(Key, Opts, Default) of
        Value when is_atom(Value) -> Value;
        Value -> list_to_atom(Value)
    end.

int_list_opt(Key, Opts, Default) ->
    case maps:find(Key, Opts) of
        error -> Default;
        {ok, Value} -> [list_to_integer(N) || N <- string:split(Value, ",", all)]
    end.

reset_root(Root) ->
    Absolute = filename:absname(Root),
    true = lists:prefix("/tmp/", Absolute) orelse
        lists:prefix("/home/dvse/bench3/", Absolute),
    case filelib:is_dir(Absolute) of
        true -> ok = file:del_dir_r(Absolute);
        false -> ok
    end,
    ok = filelib:ensure_dir(filename:join(Absolute, "placeholder")),
    ok.

directory_bytes(Path) ->
    case file:read_file_info(Path) of
        {ok, Info} when element(3, Info) =:= directory ->
            lists:sum([
                directory_bytes(filename:join(Path, Name))
             || Name <- case file:list_dir(Path) of
                    {ok, Names} -> Names;
                    _ -> []
                end
            ]);
        {ok, Info} -> element(2, Info);
        _ -> 0
    end.

now_us() -> erlang:monotonic_time(microsecond).

median([]) -> 0;
median(Values) ->
    Sorted = lists:sort(Values),
    lists:nth((length(Sorted) + 1) div 2, Sorted).

marginal(_Samples, 0) -> 0.0;
marginal(Samples, Returned) -> median(Samples) / Returned.

sha(Bin) -> binary:encode_hex(crypto:hash(sha256, Bin), lowercase).

sha_lines(Lines) ->
    sha(iolist_to_binary([
        [integer_to_binary(byte_size(Line)), $:, Line, $\n]
     || Line <- Lines
    ])).

json(Map) when is_map(Map) ->
    Pairs = lists:sort(maps:to_list(Map)),
    [$\{, join([[json_key(K), $:, json(V)] || {K, V} <- Pairs], $,), $\}];
json(List) when is_list(List) ->
    [$[, join([json(V) || V <- List], $,), $]];
json(Bin) when is_binary(Bin) -> [$", json_escape(Bin), $"];
json(true) -> "true";
json(false) -> "false";
json(undefined) -> "null";
json(null) -> "null";
json(Atom) when is_atom(Atom) -> json(atom_to_binary(Atom, utf8));
json(Number) when is_integer(Number) -> integer_to_binary(Number);
json(Number) when is_float(Number) -> float_to_binary(Number, [short]).

json_key(Key) when is_atom(Key) -> json(atom_to_binary(Key, utf8));
json_key(Key) -> json(Key).

json_escape(Bin) ->
    [case Byte of
        $" -> "\\\"";
        $\\ -> "\\\\";
        $\n -> "\\n";
        $\r -> "\\r";
        $\t -> "\\t";
        B when B < 32 -> io_lib:format("\\u~4.16.0B", [B]);
        B -> B
    end || <<Byte>> <= Bin].

join([], _Sep) -> [];
join([One], _Sep) -> One;
join([First | Rest], Sep) -> [First, Sep, join(Rest, Sep)].
