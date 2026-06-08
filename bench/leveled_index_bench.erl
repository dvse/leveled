-module(leveled_index_bench).

-include("leveled.hrl").
-include_lib("kernel/include/file.hrl").

-export([main/1]).

main(Args) ->
    case parse_args(Args) of
        {ok, Opts} ->
            run(Opts);
        {error, Reason} ->
            io:format(standard_error, "leveled_index_bench: ~p~n", [Reason]),
            {error, Reason}
    end.

parse_args(Args) ->
    Defaults = #{
        batch => 200,
        bucket => <<"bench">>,
        compression_method => native,
        ledger_compression => as_store,
        limit => 20,
        runs => 5,
        warmup => 1
    },
    parse_args(Args, Defaults).

parse_args([], Opts) ->
    Required = [tsv, root, queries, result],
    case [K || K <- Required, not maps:is_key(K, Opts)] of
        [] -> {ok, Opts};
        Missing -> {error, {missing_args, Missing}}
    end;
parse_args(["--tsv", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{tsv => Path});
parse_args(["--root", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{root => Path});
parse_args(["--queries", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{queries => Path});
parse_args(["--result", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{result => Path});
parse_args(["--batch", N | Rest], Opts) ->
    parse_args(Rest, Opts#{batch => list_to_integer(N)});
parse_args(["--compression-method", Method | Rest], Opts) ->
    case compression_method(Method) of
        {ok, Atom} -> parse_args(Rest, Opts#{compression_method => Atom});
        error -> {error, {invalid_compression_method, Method}}
    end;
parse_args(["--ledger-compression", Method | Rest], Opts) ->
    case ledger_compression(Method) of
        {ok, Atom} -> parse_args(Rest, Opts#{ledger_compression => Atom});
        error -> {error, {invalid_ledger_compression, Method}}
    end;
parse_args(["--limit", N | Rest], Opts) ->
    parse_args(Rest, Opts#{limit => list_to_integer(N)});
parse_args(["--runs", N | Rest], Opts) ->
    parse_args(Rest, Opts#{runs => list_to_integer(N)});
parse_args(["--warmup", N | Rest], Opts) ->
    parse_args(Rest, Opts#{warmup => list_to_integer(N)});
parse_args(["--bucket", B | Rest], Opts) ->
    parse_args(Rest, Opts#{bucket => unicode:characters_to_binary(B)});
parse_args([Other | _Rest], _Opts) ->
    {error, {unknown_arg, Other}}.

run(Opts) ->
    Root = maps:get(root, Opts),
    ok = reset_dir(Root),
    StartOpts = [
        {root_path, Root},
        {sync_strategy, none},
        {log_level, warn},
        {max_journalsize, 1000000000},
        {max_journalobjectcount, 200000},
        {compression_method, maps:get(compression_method, Opts)},
        {ledger_compression, maps:get(ledger_compression, Opts)},
        {stats_percentage, 100},
        {monitor_loglist, []}
    ],
    {ok, Bookie} = leveled_bookie:book_start(StartOpts),
    LoadStart = erlang:monotonic_time(microsecond),
    LoadResult = load_tsv(Bookie, Opts),
    LoadEnd = erlang:monotonic_time(microsecond),
    Result =
        case LoadResult of
            {ok, DocCount, TextBytes, LoadStats} ->
                LoadStatus = leveled_bookie:book_status(Bookie),
                Queries = read_queries(maps:get(queries, Opts)),
                QueryResults = run_queries(Bookie, Queries, Opts),
                CloseStart = erlang:monotonic_time(microsecond),
                ok = leveled_bookie:book_close(Bookie),
                CloseEnd = erlang:monotonic_time(microsecond),
                write_results(
                    maps:get(result, Opts),
                    DocCount,
                    TextBytes,
                    LoadEnd - LoadStart,
                    add_load_status(
                        LoadStatus,
                        LoadStats#{close_us => CloseEnd - CloseStart}
                    ),
                    QueryResults,
                    Opts
                );
            {error, Reason} ->
                ok = leveled_bookie:book_close(Bookie),
                {error, Reason}
        end,
    Result.

reset_dir(Root) ->
    _ = os:cmd("rm -rf -- " ++ shell_quote(Root)),
    ok = filelib:ensure_dir(filename:join(Root, "placeholder")),
    ok.

shell_quote(S) ->
    "'" ++ lists:append([shell_quote_char(C) || C <- S]) ++ "'".

shell_quote_char($') -> "'\\''";
shell_quote_char(C) -> [C].

compression_method("native") -> {ok, native};
compression_method("lz4") -> {ok, lz4};
compression_method("zstd") -> {ok, zstd};
compression_method("none") -> {ok, none};
compression_method(_) -> error.

ledger_compression("as_store") -> {ok, as_store};
ledger_compression(Method) -> compression_method(Method).

load_tsv(Bookie, Opts) ->
    Tsv = maps:get(tsv, Opts),
    BatchSize = maps:get(batch, Opts),
    case file:open(Tsv, [read, binary, {read_ahead, 1024 * 1024}]) of
        {ok, File} ->
            Result = load_loop(
                File,
                Bookie,
                Opts,
                BatchSize,
                [],
                0,
                0,
                0,
                #{parse_us => 0, write_us => 0, batches => 0}
            ),
            ok = file:close(File),
            Result;
        {error, Reason} ->
            {error, {open_tsv, Reason}}
    end.

load_loop(File, Bookie, Opts, BatchSize, Batch, BatchCount, DocCount, TextBytes, Stats) ->
    case file:read_line(File) of
        eof ->
            case timed_write_batch(Bookie, Batch, Stats) of
                {ok, FinalStats} -> {ok, DocCount, TextBytes, FinalStats};
                {error, Reason} -> {error, Reason}
            end;
        {ok, Line} ->
            ParseStart = erlang:monotonic_time(microsecond),
            ParseResult = parse_doc(Line, DocCount + 1, Opts),
            ParseEnd = erlang:monotonic_time(microsecond),
            Stats1 = add_stat(parse_us, ParseEnd - ParseStart, Stats),
            case ParseResult of
                {ok, Spec, Bytes} ->
                    NextBatch = [Spec | Batch],
                    NextBatchCount = BatchCount + 1,
                    NextDocCount = DocCount + 1,
                    NextTextBytes = TextBytes + Bytes,
                    maybe_progress(NextDocCount),
                    case NextBatchCount >= BatchSize of
                        true ->
                            case timed_write_batch(Bookie, NextBatch, Stats1) of
                                {ok, Stats2} ->
                                    load_loop(
                                        File,
                                        Bookie,
                                        Opts,
                                        BatchSize,
                                        [],
                                        0,
                                        NextDocCount,
                                        NextTextBytes,
                                        Stats2
                                    );
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        false ->
                            load_loop(
                                File,
                                Bookie,
                                Opts,
                                BatchSize,
                                NextBatch,
                                NextBatchCount,
                                NextDocCount,
                                NextTextBytes,
                                Stats1
                            )
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {read_tsv, Reason}}
    end.

parse_doc(Line0, DocNumber, Opts) ->
    Line = trim_newline(Line0),
    case binary:split(Line, <<"\t">>, [global]) of
        [Key, Title64, Body64] ->
            try
                Title = base64:decode(Title64),
                Body = base64:decode(Body64),
                Bucket = maps:get(bucket, Opts),
                IndexSpecs = [
                    {add, <<"mod100_int">>, DocNumber rem 100},
                    {add, <<"mod10_int">>, DocNumber rem 10},
                    {add, <<"mod2_int">>, DocNumber rem 2}
                ],
                Spec = {put, Bucket, Key, {Title, Body}, IndexSpecs, ?STD_TAG, infinity},
                {ok, Spec, byte_size(Title) + byte_size(Body)}
            catch
                _:Reason ->
                    {error, {invalid_doc_line, Reason}}
            end;
        Other ->
            {error, {invalid_doc_line, Other}}
    end.

trim_newline(Bin) ->
    case byte_size(Bin) of
        0 ->
            Bin;
        Size ->
            case binary:last(Bin) of
                $\n -> trim_newline(binary:part(Bin, 0, Size - 1));
                $\r -> trim_newline(binary:part(Bin, 0, Size - 1));
                _Other -> Bin
            end
    end.

write_batch(_Bookie, []) ->
    ok;
write_batch(Bookie, Batch) ->
    case leveled_bookie:book_batchput(Bookie, lists:reverse(Batch), false) of
        ok ->
            ok;
        pause ->
            timer:sleep(50),
            pause;
        {error, Reason} ->
            {error, Reason}
    end.

timed_write_batch(_Bookie, [], Stats) ->
    {ok, Stats};
timed_write_batch(Bookie, Batch, Stats) ->
    Start = erlang:monotonic_time(microsecond),
    Result = write_batch(Bookie, Batch),
    Stop = erlang:monotonic_time(microsecond),
    case Result of
        ok ->
            {ok, add_stat(batches, 1, add_stat(write_us, Stop - Start, Stats))};
        pause ->
            {ok,
                add_stat(
                    pauses,
                    1,
                    add_stat(batches, 1, add_stat(write_us, Stop - Start, Stats))
                )};
        {error, Reason} ->
            {error, Reason}
    end.

add_stat(Key, Value, Stats) ->
    Stats#{Key => maps:get(Key, Stats, 0) + Value}.

add_load_status(Status, Stats) ->
    InkUs = maps:get(put_ink_time, Status, 0),
    PrepUs = maps:get(put_prep_time, Status, 0),
    MemUs = maps:get(put_mem_time, Status, 0),
    DoBatchputProfiledUs = InkUs + PrepUs + MemUs,
    WriteUs = maps:get(write_us, Stats, 0),
    Stats#{
        put_sample_count => maps:get(put_sample_count, Status, 0),
        load_do_batchput_ink_us => InkUs,
        load_do_batchput_prep_us => PrepUs,
        load_do_batchput_mem_us => MemUs,
        load_do_batchput_profiled_us => DoBatchputProfiledUs,
        load_write_profiled_us => DoBatchputProfiledUs,
        load_write_unprofiled_us => max(0, WriteUs - DoBatchputProfiledUs)
    }.

maybe_progress(Count) when Count rem 10000 == 0 ->
    io:format(standard_error, "leveled index loaded ~p docs~n", [Count]);
maybe_progress(_Count) ->
    ok.

read_queries(Path) ->
    {ok, Bin} = file:read_file(Path),
    [
        Query
     || Line <- binary:split(Bin, <<"\n">>, [global]),
        Query <- [trim_query(Line)],
        Query =/= <<>>,
        not is_comment(Query)
    ].

trim_query(Line) ->
    unicode:characters_to_binary(string:trim(binary_to_list(Line))).

is_comment(<<"#", _/binary>>) -> true;
is_comment(_Other) -> false.

run_queries(Bookie, Queries, Opts) ->
    [run_query(Bookie, Query, Opts) || Query <- Queries].

run_query(Bookie, Query, Opts) ->
    Warmup = maps:get(warmup, Opts),
    Runs = maps:get(runs, Opts),
    Limit = maps:get(limit, Opts),
    Bucket = maps:get(bucket, Opts),
    case parse_query(Query) of
        {ok, Index, Value} ->
            TotalCount = count_index(Bookie, Bucket, Index, Value),
            _ = [query_once(Bookie, Bucket, Index, Value, Limit) || _ <- lists:seq(1, Warmup)],
            Timed =
                [
                    begin
                        Start = erlang:monotonic_time(microsecond),
                        Result = query_once(Bookie, Bucket, Index, Value, Limit),
                        Stop = erlang:monotonic_time(microsecond),
                        {Stop - Start, Result}
                    end
                 || _ <- lists:seq(1, Runs)
                ],
            {Times, Results} = lists:unzip(Timed),
            Count =
                case Results of
                    [{ok, CountKeys} | _] -> length(CountKeys);
                    [{error, _Reason} | _] -> 0;
                    [] -> 0
                end,
            Keys =
                case Results of
                    [{ok, Keys0} | _] -> Keys0;
                    _ -> []
                end,
            Error =
                case [Reason || {error, Reason} <- Results] of
                    [] -> none;
                    [Reason | _] -> Reason
                end,
            #{
                query => Query,
                count => Count,
                total_count => TotalCount,
                runs_us => Times,
                error => Error,
                keys => Keys
            };
        {error, Reason} ->
            #{
                query => Query,
                count => 0,
                total_count => 0,
                runs_us => [0 || _ <- lists:seq(1, Runs)],
                error => Reason,
                keys => []
            }
    end.

parse_query(Query) ->
    case binary:split(Query, <<":">>) of
        [Name, ValueBin] ->
            case {index_name(Name), parse_int(ValueBin)} of
                {{ok, Index}, {ok, Value}} -> {ok, Index, Value};
                {error, _} -> {error, {unknown_index_query, Query}};
                {_, error} -> {error, {invalid_index_value, Query}}
            end;
        _Other ->
            {error, {invalid_index_query, Query}}
    end.

index_name(<<"mod100">>) -> {ok, <<"mod100_int">>};
index_name(<<"mod10">>) -> {ok, <<"mod10_int">>};
index_name(<<"mod2">>) -> {ok, <<"mod2_int">>};
index_name(_Other) -> error.

parse_int(Bin) ->
    try {ok, binary_to_integer(Bin)} catch _:_ -> error end.

count_index(Bookie, Bucket, Index, Value) ->
    FoldFun = fun(_B, _K, Count) -> Count + 1 end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Bookie,
            {Bucket, <<>>},
            {FoldFun, 0},
            {Index, Value, Value},
            {false, undefined}
        ),
    Runner().

query_once(Bookie, Bucket, Index, Value, Limit) ->
    FoldFun =
        fun(_B, K, {Count, Keys}) ->
            NextCount = Count + 1,
            NextKeys = [K | Keys],
            case Limit > 0 andalso NextCount >= Limit of
                true -> throw({stop_fold, NextCount, NextKeys});
                false -> {NextCount, NextKeys}
            end
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Bookie,
            {Bucket, <<>>},
            {FoldFun, {0, []}},
            {Index, Value, Value},
            {false, undefined}
        ),
    try Runner() of
        {_ReturnedCount, ReturnedKeysRev} ->
            {ok, lists:reverse(ReturnedKeysRev)}
    catch
        throw:{stop_fold, _StopCount, StopKeysRev} ->
            {ok, lists:reverse(StopKeysRev)};
        Class:Reason ->
            {error, {Class, Reason}}
    end.

write_results(Path, DocCount, TextBytes, LoadUs, LoadStats, QueryResults, Opts) ->
    ok = filelib:ensure_dir(Path),
    {ok, File} = file:open(Path, [write, binary]),
    ok = io:format(File, "engine\tmetric\tvalue\tquery\tcount\ttotal_count\truns_us\terror\tkeys~n", []),
    ok = io:format(File, "leveled\ttsv\t~s\t\t\t\t\t\t~n", [
        escape_cell(unicode:characters_to_binary(maps:get(tsv, Opts)))
    ]),
    ok = io:format(File, "leveled\ttsv_sha256\t~s\t\t\t\t\t\t~n", [
        escape_cell(file_sha256_hex(maps:get(tsv, Opts)))
    ]),
    ok = io:format(File, "leveled\troot\t~s\t\t\t\t\t\t~n", [
        escape_cell(unicode:characters_to_binary(maps:get(root, Opts)))
    ]),
    ok = io:format(File, "leveled\tdocs\t~p\t\t\t\t\t\t~n", [DocCount]),
    ok = io:format(File, "leveled\ttext_bytes\t~p\t\t\t\t\t\t~n", [TextBytes]),
    ok = io:format(File, "leveled\tload_us\t~p\t\t\t\t\t\t~n", [LoadUs]),
    ok = io:format(File, "leveled\tload_finalized_us\t~p\t\t\t\t\t\t~n", [
        LoadUs + maps:get(close_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_parse_us\t~p\t\t\t\t\t\t~n", [
        maps:get(parse_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_write_us\t~p\t\t\t\t\t\t~n", [
        maps:get(write_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_other_us\t~p\t\t\t\t\t\t~n", [
        LoadUs - maps:get(parse_us, LoadStats, 0) - maps:get(write_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_batches\t~p\t\t\t\t\t\t~n", [
        maps:get(batches, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_pauses\t~p\t\t\t\t\t\t~n", [
        maps:get(pauses, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_put_sample_count\t~p\t\t\t\t\t\t~n", [
        maps:get(put_sample_count, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_do_batchput_ink_us\t~p\t\t\t\t\t\t~n", [
        maps:get(load_do_batchput_ink_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_do_batchput_prep_us\t~p\t\t\t\t\t\t~n", [
        maps:get(load_do_batchput_prep_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_do_batchput_mem_us\t~p\t\t\t\t\t\t~n", [
        maps:get(load_do_batchput_mem_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_do_batchput_profiled_us\t~p\t\t\t\t\t\t~n", [
        maps:get(load_do_batchput_profiled_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_write_profiled_us\t~p\t\t\t\t\t\t~n", [
        maps:get(load_write_profiled_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_write_unprofiled_us\t~p\t\t\t\t\t\t~n", [
        maps:get(load_write_unprofiled_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tclose_us\t~p\t\t\t\t\t\t~n", [
        maps:get(close_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tstore_bytes\t~p\t\t\t\t\t\t~n", [
        directory_size(maps:get(root, Opts))
    ]),
    ok = io:format(File, "leveled\tcompression_method\t~p\t\t\t\t\t\t~n", [
        maps:get(compression_method, Opts)
    ]),
    ok = io:format(File, "leveled\tledger_compression\t~p\t\t\t\t\t\t~n", [
        maps:get(ledger_compression, Opts)
    ]),
    ok = io:format(File, "leveled\tbenchmark_mode\tsecondary_index\t\t\t\t\t\t~n", []),
    ok = io:format(File, "leveled\tcontent_object\t~s\t\t\t\t\t\t~n", [
        escape_cell(<<"{Title, Body}">>)
    ]),
    ok = io:format(File, "leveled\tindex_contract\t~s\t\t\t\t\t\t~n", [
        escape_cell(<<"mod100_int,mod10_int,mod2_int from document ordinal">>)
    ]),
    lists:foreach(fun(Result) -> write_query_result(File, Result) end, QueryResults),
    ok = file:close(File),
    ok.

write_query_result(
    File,
    #{query := Query, count := Count, total_count := TotalCount, runs_us := Runs, error := Error, keys := Keys}
) ->
    RunsBin = join_integer_list(Runs),
    KeysCell = json_binary_list(Keys),
    ok = io:format(
        File,
        "leveled\tquery_us\t\t~s\t~p\t~p\t~s\t~p\t~s~n",
        [escape_cell(Query), Count, TotalCount, RunsBin, Error, escape_cell(KeysCell)]
    ).

join_integer_list([]) ->
    "";
join_integer_list([N | Rest]) ->
    integer_to_list(N) ++ lists:append(["," ++ integer_to_list(I) || I <- Rest]).

json_binary_list(Keys) ->
    iolist_to_binary([$[, json_binary_items(Keys), $]]).

json_binary_items([]) ->
    [];
json_binary_items([Key]) ->
    json_binary(Key);
json_binary_items([Key | Rest]) ->
    [json_binary(Key), $,, json_binary_items(Rest)].

json_binary(Key) ->
    [$", json_binary_chars(unicode:characters_to_binary(Key), []), $"].

json_binary_chars(<<>>, Acc) ->
    lists:reverse(Acc);
json_binary_chars(<<$", Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$", $\\ | Acc]);
json_binary_chars(<<$\\, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$\\, $\\ | Acc]);
json_binary_chars(<<$\b, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$b, $\\ | Acc]);
json_binary_chars(<<$\f, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$f, $\\ | Acc]);
json_binary_chars(<<$\n, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$n, $\\ | Acc]);
json_binary_chars(<<$\r, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$r, $\\ | Acc]);
json_binary_chars(<<$\t, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [$t, $\\ | Acc]);
json_binary_chars(<<C, Rest/binary>>, Acc) when C < 16#20 ->
    Hex = io_lib:format("\\u~4.16.0B", [C]),
    json_binary_chars(Rest, lists:reverse(Hex) ++ Acc);
json_binary_chars(<<C, Rest/binary>>, Acc) ->
    json_binary_chars(Rest, [C | Acc]).

file_sha256_hex(Path) ->
    {ok, File} = file:open(Path, [read, binary, raw]),
    try
        file_sha256_hex(File, crypto:hash_init(sha256))
    after
        ok = file:close(File)
    end.

file_sha256_hex(File, State0) ->
    case file:read(File, 1024 * 1024) of
        eof ->
            hex_binary(crypto:hash_final(State0));
        {ok, Chunk} ->
            file_sha256_hex(File, crypto:hash_update(State0, Chunk))
    end.

hex_binary(Hash) ->
    Hex = [
        begin
            <<Hi:4, Lo:4>> = <<Byte>>,
            [hex_digit(Hi), hex_digit(Lo)]
        end
     || <<Byte>> <= Hash
    ],
    list_to_binary(Hex).

hex_digit(N) when N < 10 ->
    $0 + N;
hex_digit(N) ->
    $a + (N - 10).

escape_cell(Bin) when is_binary(Bin) ->
    Chars = binary_to_list(Bin),
    Escaped =
        lists:append([
            case C of
                $" -> "\"\"";
                _ -> [C]
            end
         || C <- Chars
        ]),
    case lists:any(fun(C) -> C == $\t orelse C == $\n orelse C == $\r orelse C == $" end, Chars) of
        true -> "\"" ++ Escaped ++ "\"";
        false -> Escaped
    end.

directory_size(Path) ->
    case filelib:is_dir(Path) of
        true -> directory_size([Path], 0);
        false -> 0
    end.

directory_size([], Acc) ->
    Acc;
directory_size([Path | Rest], Acc) ->
    case file:list_dir(Path) of
        {ok, Names} ->
            {Dirs, Size} =
                lists:foldl(
                    fun(Name, {DirAcc, SizeAcc}) ->
                        Child = filename:join(Path, Name),
                        case file:read_file_info(Child) of
                            {ok, #file_info{type = directory}} ->
                                {[Child | DirAcc], SizeAcc};
                            {ok, #file_info{size = Bytes}} ->
                                {DirAcc, SizeAcc + Bytes};
                            {error, _Reason} ->
                                {DirAcc, SizeAcc}
                        end
                    end,
                    {[], Acc},
                    Names
                ),
            directory_size(Dirs ++ Rest, Size);
        {error, _Reason} ->
            directory_size(Rest, Acc)
    end.
