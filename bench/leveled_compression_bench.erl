-module(leveled_compression_bench).

-include("leveled.hrl").

-export([main/1]).

-define(DOC_COUNT, 5000).
-define(BATCH_DOCS, 200).
-define(CHUNK_BUCKET, <<"compression-chunks">>).
-define(CHUNK_KEY, <<"value">>).
-define(FTS_BUCKET, <<"compression-fts">>).
-define(META_BUCKET, <<"compression-meta">>).
-define(FRAME_BYTES, 8).

main(["prepare", Source, Workload]) ->
    prepare(Source, Workload);
main(["sanity", Source]) ->
    sanity(Source);
main(["metadata", Source, Root, Method0, Receipt0, Ledger0]) ->
    metadata(
        Source,
        Root,
        list_to_atom(Method0),
        receipt(Receipt0),
        list_to_atom(Ledger0)
    );
main(["run", Workload, Root, Method0, Receipt0, Ledger0, Load0]) ->
    run(
        Workload,
        Root,
        list_to_atom(Method0),
        receipt(Receipt0),
        list_to_atom(Ledger0),
        list_to_atom(Load0),
        default
    );
main(["run0", Workload, Root, Method0, Receipt0, Ledger0, Load0]) ->
    run(
        Workload,
        Root,
        list_to_atom(Method0),
        receipt(Receipt0),
        list_to_atom(Ledger0),
        list_to_atom(Load0),
        0
    );
main(_Args) ->
    erlang:error(
        {usage,
            "prepare SOURCE WORKLOAD | sanity SOURCE | "
            "metadata SOURCE ROOT METHOD true|false LEDGER | "
            "run|run0 WORKLOAD ROOT METHOD true|false LEDGER text|fts|mixed"}
    ).

receipt("true") -> on_receipt;
receipt("false") -> on_compact.

schema() ->
    {ok, Schema} = leveled_fts:schema(#{
        index => ?FTS_BUCKET,
        columns => [body],
        shards => 256,
        remove_diacritics => 2
    }),
    Schema.

prepare(SourcePath, WorkloadPath) ->
    {ok, Source} = file:read_file(SourcePath),
    true = byte_size(Source) > 1000000,
    ok = filelib:ensure_dir(WorkloadPath),
    {ok, File} = file:open(WorkloadPath, [write, raw, binary]),
    Schema = schema(),
    Start = erlang:monotonic_time(microsecond),
    {RawBytes, FTSRows, FTSValueBytes} = lists:foldl(
        fun(N, {RawAcc, RowAcc, ValueAcc}) ->
            Size = doc_size(N),
            Value = natural_value(Source, Size, N),
            Key = doc_key(N),
            {ok, Specs} = leveled_fts:derive(
                Schema, Key, #{body => Value}
            ),
            ok = write_frame(File, {Key, Value, Specs}),
            {RawAcc + Size,
                RowAcc + length(Specs),
                ValueAcc + lists:sum([
                    byte_size(V) || {_Op, _B, _K, _SK, V} <- Specs
                ])}
        end,
        {0, 0, 0},
        lists:seq(1, ?DOC_COUNT)
    ),
    ok = file:close(File),
    Stop = erlang:monotonic_time(microsecond),
    io:format(
        "PREP\tdocs=~B\traw_bytes=~B\tp50=~B\tp95=~B\tp99=~B\tmax=~B"
        "\tfts_rows=~B\tfts_value_bytes=~B\tprepare_us=~B\tworkload_bytes=~B~n",
        [?DOC_COUNT, RawBytes, doc_size(2500), doc_size(4750),
            doc_size(4950), doc_size(5000), FTSRows, FTSValueBytes,
            Stop - Start, filelib:file_size(WorkloadPath)]
    ),
    ok.

metadata(SourcePath, Root, Method, CompressionPoint, Ledger) ->
    {ok, Source} = file:read_file(SourcePath),
    reset_dir(Root),
    Opts = start_opts(Root, Method, CompressionPoint, Ledger, default),
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    {RawBytes, WriteUs, Pauses} = write_metadata(Bookie, Source, 1, 50000,
        0, 0, 0),
    ok = leveled_bookie:book_close(Bookie),
    TailDisk = disk_sizes(Root),
    Keys = [meta_key(N) || N <- lists:seq(1, 50000)],
    {ok, ReadBookie} = leveled_bookie:book_start(Opts),
    Read = latency_pair(
        ReadBookie, metadata, sample_ids(stable_shuffle(Keys), 5000)
    ),
    ok = leveled_bookie:book_close(ReadBookie),
    {ok, CompactBookie} = leveled_bookie:book_start(Opts),
    CompactStart = erlang:monotonic_time(microsecond),
    ok = compact_cycle(CompactBookie, 2),
    CompactStop = erlang:monotonic_time(microsecond),
    ok = leveled_bookie:book_close(CompactBookie),
    CompactDisk = disk_sizes(Root),
    io:format(
        "META\tmethod=~p\treceipt=~p\tledger=~p\trows=50000"
        "\traw_bytes=~B\twrite_us=~B\twrite_mib_s=~.3f\trows_s=~.1f"
        "\tpauses=~B\ttail_journal=~B\ttail_ledger=~B\ttail_ratio=~.5f"
        "\tcompact_us=~B\tcompact_journal=~B\tcompact_ledger=~B"
        "\tcompact_ratio=~.5f\tcold_p50_us=~.3f\tcold_p99_us=~.3f"
        "\twarm_p50_us=~.3f\twarm_p99_us=~.3f~n",
        [Method, CompressionPoint, Ledger, RawBytes, WriteUs,
            mib_per_second(RawBytes, WriteUs), per_second(50000, WriteUs),
            Pauses, maps:get(journal, TailDisk), maps:get(ledger, TailDisk),
            disk_ratio(TailDisk, RawBytes), CompactStop - CompactStart,
            maps:get(journal, CompactDisk), maps:get(ledger, CompactDisk),
            disk_ratio(CompactDisk, RawBytes), maps:get(cold_p50, Read),
            maps:get(cold_p99, Read), maps:get(warm_p50, Read),
            maps:get(warm_p99, Read)]
    ),
    ok.

write_metadata(_Bookie, _Source, N, Limit, RawBytes, WriteUs, Pauses)
        when N > Limit ->
    {RawBytes, WriteUs, Pauses};
write_metadata(Bookie, Source, N, Limit, RawBytes, WriteUs, Pauses) ->
    Size = meta_size(N),
    Value = natural_value(Source, Size, N),
    Start = erlang:monotonic_time(microsecond),
    Result = leveled_bookie:book_put(
        Bookie, ?META_BUCKET, meta_key(N), Value, []
    ),
    Pause = case Result of
        ok -> 0;
        pause -> timer:sleep(50), 1
    end,
    Stop = erlang:monotonic_time(microsecond),
    write_metadata(Bookie, Source, N + 1, Limit, RawBytes + Size,
        WriteUs + Stop - Start, Pauses + Pause).

meta_size(N) when N < 25000 ->
    round(128 + (512 - 128) * math:pow(N / 25000, 1.4));
meta_size(25000) -> 512;
meta_size(N) when N < 47500 ->
    X = (N - 25000) / 22500,
    round(512 + (2048 - 512) * math:pow(X, 8));
meta_size(47500) -> 2048;
meta_size(N) when N < 49500 ->
    X = (N - 47500) / 2000,
    round(2048 + (4096 - 2048) * math:pow(X, 4));
meta_size(49500) -> 4096;
meta_size(N) when N < 50000 ->
    X = (N - 49500) / 500,
    round(4096 + (16384 - 4096) * math:pow(X, 6));
meta_size(50000) -> 16384.

meta_key(N) ->
    iolist_to_binary(io_lib:format("fact-~8..0B", [N])).

doc_size(N) when N < 2500 ->
    round(1024 + (9100 - 1024) * math:pow(N / 2500, 1.4));
doc_size(2500) -> 9100;
doc_size(N) when N < 4750 ->
    X = (N - 2500) / 2250,
    round(9100 + (86000 - 9100) * math:pow(X, 8));
doc_size(4750) -> 86000;
doc_size(N) when N < 4950 ->
    X = (N - 4750) / 200,
    round(86000 + (233000 - 86000) * math:pow(X, 4));
doc_size(4950) -> 233000;
doc_size(N) when N < 5000 ->
    X = (N - 4950) / 50,
    round(233000 + (1000000 - 233000) * math:pow(X, 6));
doc_size(5000) -> 4750000.

doc_key(N) ->
    iolist_to_binary(io_lib:format("chunk-~8..0B", [N])).

natural_value(Source, Size, N) ->
    SourceBytes = byte_size(Source),
    Offset = erlang:phash2(N, SourceBytes),
    circular_slice(Source, SourceBytes, Offset, Size).

circular_slice(Source, SourceBytes, Offset, Size)
        when Offset + Size =< SourceBytes ->
    binary:copy(binary:part(Source, Offset, Size));
circular_slice(Source, SourceBytes, Offset, Size) ->
    SuffixBytes = SourceBytes - Offset,
    Suffix = binary:part(Source, Offset, SuffixBytes),
    Remaining = Size - SuffixBytes,
    FullCopies = Remaining div SourceBytes,
    PrefixBytes = Remaining rem SourceBytes,
    Prefix = binary:part(Source, 0, PrefixBytes),
    iolist_to_binary([Suffix, binary:copy(Source, FullCopies), Prefix]).

write_frame(File, Term) ->
    Bin = term_to_binary(Term, [compressed]),
    file:write(File, <<(byte_size(Bin)):64/unsigned-big, Bin/binary>>).

read_frame(File) ->
    case file:read(File, ?FRAME_BYTES) of
        eof -> eof;
        {ok, <<Size:64/unsigned-big>>} ->
            {ok, Bin} = file:read(File, Size),
            {ok, binary_to_term(Bin)}
    end.

run(Workload, Root, Method, CompressionPoint, Ledger, Load, PressLevel)
        when Method =:= none; Method =:= lz4; Method =:= native;
            Method =:= zstd ->
    true = lists:member(Ledger, [none, lz4, native, zstd]),
    true = lists:member(Load, [text, fts, mixed]),
    reset_dir(Root),
    Opts = start_opts(Root, Method, CompressionPoint, Ledger, PressLevel),
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    {ok, File} = file:open(Workload, [read, raw, binary, {read_ahead, 1048576}]),
    IngestStart = erlang:monotonic_time(microsecond),
    Stats0 = #{docs => 0, raw_bytes => 0, rows => 0,
        write_us => 0, pauses => 0, batches => 0},
    Stats = ingest_loop(File, Bookie, Load, [], 0, Stats0),
    IngestStop = erlang:monotonic_time(microsecond),
    ok = file:close(File),
    ok = leveled_bookie:book_close(Bookie),
    TailDisk = disk_sizes(Root),

    {ConsolidateUs, ConsolidatedDisk} = case Load of
        text -> {0, TailDisk};
        _ ->
            {ok, ConsolidateBookie} = leveled_bookie:book_start(Opts),
            {Us, {ok, _Result}} = timer:tc(
                leveled_fts, consolidate,
                [ConsolidateBookie, schema(), #{}]
            ),
            ok = leveled_bookie:book_close(ConsolidateBookie),
            {Us, disk_sizes(Root)}
    end,

    ok = validate_counts(Opts, Load),
    {ChunkRead, PageRead, PageCount} = measure_reads(Opts, Load),

    {ok, CompactBookie} = leveled_bookie:book_start(Opts),
    CompactStart = erlang:monotonic_time(microsecond),
    ok = compact_cycle(CompactBookie, 2),
    CompactStop = erlang:monotonic_time(microsecond),
    ok = leveled_bookie:book_close(CompactBookie),
    CompactDisk = disk_sizes(Root),

    RawBytes = maps:get(raw_bytes, Stats),
    WriteUs = maps:get(write_us, Stats),
    Rows = maps:get(rows, Stats),
    io:format(
        "RESULT\tmethod=~p\treceipt=~p\tledger=~p\tpress_level=~p\tload=~p"
        "\tdocs=~B\traw_bytes=~B\trows=~B\tbatches=~B\tpauses=~B"
        "\twrite_us=~B\tingest_wall_us=~B\twrite_mib_s=~.3f\trows_s=~.1f"
        "\ttail_journal=~B\ttail_ledger=~B\ttail_ratio=~.5f"
        "\tconsolidate_us=~B\tconsolidated_journal=~B"
        "\tconsolidated_ledger=~B\tconsolidated_ratio=~.5f"
        "\tcompact_us=~B\tcompact_journal=~B\tcompact_ledger=~B"
        "\tcompact_ratio=~.5f\tpage_count=~B"
        "\tchunk_cold_p50_us=~.3f\tchunk_cold_p99_us=~.3f"
        "\tchunk_warm_p50_us=~.3f\tchunk_warm_p99_us=~.3f"
        "\tpage_cold_p50_us=~.3f\tpage_cold_p99_us=~.3f"
        "\tpage_warm_p50_us=~.3f\tpage_warm_p99_us=~.3f~n",
        [Method, CompressionPoint, Ledger, PressLevel, Load,
            maps:get(docs, Stats), RawBytes, Rows,
            maps:get(batches, Stats), maps:get(pauses, Stats),
            WriteUs, IngestStop - IngestStart,
            mib_per_second(RawBytes, WriteUs), per_second(Rows, WriteUs),
            maps:get(journal, TailDisk), maps:get(ledger, TailDisk),
            disk_ratio(TailDisk, RawBytes), ConsolidateUs,
            maps:get(journal, ConsolidatedDisk),
            maps:get(ledger, ConsolidatedDisk),
            disk_ratio(ConsolidatedDisk, RawBytes),
            CompactStop - CompactStart,
            maps:get(journal, CompactDisk), maps:get(ledger, CompactDisk),
            disk_ratio(CompactDisk, RawBytes), PageCount,
            maps:get(cold_p50, ChunkRead), maps:get(cold_p99, ChunkRead),
            maps:get(warm_p50, ChunkRead), maps:get(warm_p99, ChunkRead),
            maps:get(cold_p50, PageRead), maps:get(cold_p99, PageRead),
            maps:get(warm_p50, PageRead), maps:get(warm_p99, PageRead)]
    ),
    ok.

validate_counts(Opts, Load) ->
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    ChunkCount = count_rows(Bookie, ?CHUNK_BUCKET, ?CHUNK_KEY),
    DocCount = count_rows(Bookie, ?FTS_BUCKET, <<"doc">>),
    ok = leveled_bookie:book_close(Bookie),
    case Load of
        text -> true = ChunkCount =:= ?DOC_COUNT, true = DocCount =:= 0;
        fts -> true = ChunkCount =:= 0, true = DocCount =:= ?DOC_COUNT;
        mixed ->
            true = ChunkCount =:= ?DOC_COUNT,
            true = DocCount =:= ?DOC_COUNT
    end,
    ok.

count_rows(Bookie, Bucket, RowKey) ->
    Fold = fun
        (B, {K, _SubKey}, _Value, N) when B =:= Bucket, K =:= RowKey ->
            N + 1;
        (_B, _K, _Value, N) -> N
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie, ?HEAD_TAG, {range, Bucket, all},
        {Fold, 0}, false, true, false
    ),
    Runner().

start_opts(Root, Method, CompressionPoint, Ledger, PressLevel) ->
    [
        {root_path, Root},
        {sync_strategy, none},
        {log_level, warn},
        {monitor_loglist, []},
        {stats_percentage, 100},
        {max_journalsize, 16000000},
        {max_journalobjectcount, 200000},
        {compression_method, Method},
        {compression_point, CompressionPoint},
        {ledger_compression, Ledger}
    ] ++ press_level_opt(PressLevel).

press_level_opt(default) -> [];
press_level_opt(Level) -> [{compression_level, Level}].

ingest_loop(File, Bookie, Load, Batch, BatchDocs, Stats) ->
    case read_frame(File) of
        eof -> flush_batch(Bookie, Batch, BatchDocs, Stats);
        {ok, {Key, Value, FTSSpecs}} ->
            Specs = load_specs(Load, Key, Value, FTSSpecs),
            Stats1 = Stats#{
                docs := maps:get(docs, Stats) + 1,
                raw_bytes := maps:get(raw_bytes, Stats) + byte_size(Value)
            },
            Batch1 = [Specs | Batch],
            case BatchDocs + 1 >= ?BATCH_DOCS of
                true ->
                    Stats2 = flush_batch(Bookie, Batch1, BatchDocs + 1, Stats1),
                    ingest_loop(File, Bookie, Load, [], 0, Stats2);
                false ->
                    ingest_loop(
                        File, Bookie, Load, Batch1, BatchDocs + 1, Stats1
                    )
            end
    end.

load_specs(text, Key, Value, _FTSSpecs) ->
    [{add, ?CHUNK_BUCKET, ?CHUNK_KEY, Key, Value}];
load_specs(fts, _Key, _Value, FTSSpecs) ->
    FTSSpecs;
load_specs(mixed, Key, Value, FTSSpecs) ->
    [{add, ?CHUNK_BUCKET, ?CHUNK_KEY, Key, Value} | FTSSpecs].

flush_batch(_Bookie, [], _BatchDocs, Stats) -> Stats;
flush_batch(Bookie, SpecLists, _BatchDocs, Stats) ->
    Specs = dedupe_specs(lists:append(lists:reverse(SpecLists))),
    Start = erlang:monotonic_time(microsecond),
    Result = leveled_bookie:book_mput(Bookie, Specs),
    Pause = case Result of
        ok -> 0;
        pause -> timer:sleep(50), 1
    end,
    Stop = erlang:monotonic_time(microsecond),
    Stats#{
        rows := maps:get(rows, Stats) + length(Specs),
        batches := maps:get(batches, Stats) + 1,
        pauses := maps:get(pauses, Stats) + Pause,
        write_us := maps:get(write_us, Stats) + Stop - Start
    }.

dedupe_specs(Specs) ->
    maps:values(lists:foldl(
        fun({_, B, K, SK, _} = Spec, Acc) -> Acc#{{B, K, SK} => Spec} end,
        #{},
        Specs
    )).

measure_reads(Opts, Load) ->
    {ok, Bookie0} = leveled_bookie:book_start(Opts),
    PageIds = case Load of
        text -> [];
        _ -> collect_page_ids(Bookie0)
    end,
    ok = leveled_bookie:book_close(Bookie0),
    ChunkIds = case Load of
        fts -> [];
        _ -> [doc_key(N) || N <- lists:seq(1, ?DOC_COUNT)]
    end,
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    ChunkRead = latency_pair(Bookie, chunk, stable_shuffle(ChunkIds)),
    PageRead = latency_pair(
        Bookie, page, sample_ids(stable_shuffle(PageIds), 5000)
    ),
    ok = leveled_bookie:book_close(Bookie),
    {ChunkRead, PageRead, length(PageIds)}.

collect_page_ids(Bookie) ->
    Fold = fun
        (B, {<<"t:", _/binary>> = Key, <<_Plane:8, _Column:8, _Page:16>> = SK},
                _Value, Acc) when B =:= ?FTS_BUCKET ->
            [{Key, SK} | Acc];
        (_B, _K, _V, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie, ?HEAD_TAG, {range, ?FTS_BUCKET, all},
        {Fold, []}, false, true, false
    ),
    Runner().

stable_shuffle(Ids) ->
    lists:sort(
        fun(A, B) -> erlang:phash2(A) < erlang:phash2(B) end,
        Ids
    ).

sample_ids(Ids, Limit) ->
    lists:sublist(Ids, Limit).

latency_pair(_Bookie, _Kind, []) -> empty_latency();
latency_pair(Bookie, Kind, Ids) ->
    Cold = latency_samples(Bookie, Kind, Ids, []),
    Warm = latency_samples(Bookie, Kind, Ids, []),
    #{cold_p50 => percentile(Cold, 0.50),
        cold_p99 => percentile(Cold, 0.99),
        warm_p50 => percentile(Warm, 0.50),
        warm_p99 => percentile(Warm, 0.99)}.

empty_latency() ->
    #{cold_p50 => 0.0, cold_p99 => 0.0,
        warm_p50 => 0.0, warm_p99 => 0.0}.

latency_samples(_Bookie, _Kind, [], Acc) -> Acc;
latency_samples(Bookie, Kind, [Id | Rest], Acc) ->
    Start = erlang:monotonic_time(nanosecond),
    {ok, _Value} = read_id(Bookie, Kind, Id),
    Stop = erlang:monotonic_time(nanosecond),
    latency_samples(Bookie, Kind, Rest, [(Stop - Start) / 1000 | Acc]).

read_id(Bookie, chunk, Key) ->
    leveled_bookie:book_headonly(
        Bookie, ?CHUNK_BUCKET, ?CHUNK_KEY, Key
    );
read_id(Bookie, page, {Key, SubKey}) ->
    leveled_bookie:book_headonly(Bookie, ?FTS_BUCKET, Key, SubKey);
read_id(Bookie, metadata, Key) ->
    leveled_bookie:book_get(Bookie, ?META_BUCKET, Key).

percentile(Values, Fraction) ->
    Sorted = lists:sort(Values),
    Index = max(1, min(length(Sorted), ceil(length(Sorted) * Fraction))),
    lists:nth(Index, Sorted).

compact_cycle(_Bookie, 0) -> ok;
compact_cycle(Bookie, N) ->
    case leveled_bookie:book_compactjournal(Bookie, 300000) of
        ok -> ok;
        busy -> ok
    end,
    ok = wait_compaction(Bookie, 6000),
    compact_cycle(Bookie, N - 1).

wait_compaction(_Bookie, 0) -> erlang:error(journal_compaction_timeout);
wait_compaction(Bookie, Attempts) ->
    case leveled_bookie:book_islastcompactionpending(Bookie) of
        false -> ok;
        true -> timer:sleep(100), wait_compaction(Bookie, Attempts - 1)
    end.

disk_sizes(Root) ->
    Journal = dir_bytes(filename:join(Root, "journal")),
    Ledger = dir_bytes(filename:join(Root, "ledger")),
    #{journal => Journal, ledger => Ledger, total => Journal + Ledger}.

dir_bytes(Path) ->
    filelib:fold_files(
        Path, ".*", true,
        fun(File, Acc) -> Acc + filelib:file_size(File) end,
        0
    ).

disk_ratio(Disk, RawBytes) -> maps:get(total, Disk) / RawBytes.

mib_per_second(_Bytes, 0) -> 0.0;
mib_per_second(Bytes, Us) -> Bytes / 1048576 / (Us / 1000000).

per_second(_Count, 0) -> 0.0;
per_second(Count, Us) -> Count / (Us / 1000000).

reset_dir(Root) ->
    _ = os:cmd("rm -rf -- " ++ shell_quote(Root)),
    ok = filelib:ensure_dir(filename:join(Root, "placeholder")),
    ok.

shell_quote(S) ->
    "'" ++ lists:append([shell_quote_char(C) || C <- S]) ++ "'".

shell_quote_char($') -> "'\\''";
shell_quote_char(C) -> [C].

sanity(SourcePath) ->
    {ok, Source} = file:read_file(SourcePath),
    lists:foreach(
        fun(Size) ->
            Value = natural_value(Source, Size, Size),
            lists:foreach(
                fun(Method) -> sanity_one(Method, Value) end,
                [none, lz4, native, zstd]
            )
        end,
        [512, 2048, 9100, 86000, 233000, 4750000]
    ),
    ok.

sanity_one(Method, Value) ->
    _ = codec_compress(Method, Value),
    {CompressUs, Compressed} = timer:tc(
        fun() -> codec_compress(Method, Value) end
    ),
    {DecompressUs, RoundTrip} = timer:tc(
        fun() -> codec_decompress(Method, Compressed) end
    ),
    true = RoundTrip =:= Value,
    io:format(
        "SANITY\tmethod=~p\traw_bytes=~B\tcompressed_bytes=~B\tratio=~.5f"
        "\tcompress_us=~B\tdecompress_us=~B\tok=true~n",
        [Method, byte_size(Value), byte_size(Compressed),
            byte_size(Compressed) / byte_size(Value),
            CompressUs, DecompressUs]
    ).

codec_compress(none, Bin) -> Bin;
codec_compress(lz4, Bin) ->
    {ok, Compressed} = lz4:pack(Bin),
    Compressed;
codec_compress(native, Bin) -> zlib:compress(Bin);
codec_compress(zstd, Bin) -> iolist_to_binary(zstd:compress(Bin)).

codec_decompress(none, Bin) -> Bin;
codec_decompress(lz4, Bin) ->
    {ok, Decompressed} = lz4:unpack(Bin),
    Decompressed;
codec_decompress(native, Bin) -> zlib:uncompress(Bin);
codec_decompress(zstd, Bin) -> iolist_to_binary(zstd:decompress(Bin)).
