-module(leveled_fts_bench).

-include("leveled.hrl").
-include_lib("kernel/include/file.hrl").

-export([main/1]).

main(Args) ->
    case parse_args(Args) of
        {ok, Opts} ->
            run(Opts);
        {error, Reason} ->
            io:format(standard_error, "leveled_fts_bench: ~p~n", [Reason]),
            {error, Reason}
    end.

parse_args(Args) ->
    Defaults = #{
        batch => 200,
        bucket => <<"bench">>,
        compression_method => native,
        cache_size => default,
        cache_multiple => default,
        max_pencillercachesize => default,
        index => <<"main">>,
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
parse_args(["--cache-size", N | Rest], Opts) ->
    parse_args(Rest, Opts#{cache_size => list_to_integer(N)});
parse_args(["--cache-multiple", N | Rest], Opts) ->
    parse_args(Rest, Opts#{cache_multiple => list_to_integer(N)});
parse_args(["--max-pencillercachesize", N | Rest], Opts) ->
    parse_args(Rest, Opts#{max_pencillercachesize => list_to_integer(N)});
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
parse_args(["--index", I | Rest], Opts) ->
    parse_args(Rest, Opts#{index => unicode:characters_to_binary(I)});
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
        {monitor_loglist, []},
        {fts_indexes, [
            #{
                bucket => maps:get(bucket, Opts),
                index => maps:get(index, Opts),
                tag => ?STD_TAG,
                columns => [
                    #{name => title, path => [1]},
                    #{name => body, path => [2]}
                ],
                prefixes => [5, 11],
                remove_diacritics => 2
            }
        ]}
    ] ++ cache_start_opts(Opts),
    {ok, Bookie} = leveled_bookie:book_start(StartOpts),
    LoadStart = erlang:monotonic_time(microsecond),
    LoadResult = load_tsv(Bookie, Opts),
    LoadEnd = erlang:monotonic_time(microsecond),
    Result =
        case LoadResult of
            {ok, DocCount, TextBytes, LoadStats} ->
                LoadStatus = leveled_bookie:book_status(Bookie),
                CloseStart = erlang:monotonic_time(microsecond),
                ok = leveled_bookie:book_close(Bookie),
                CloseEnd = erlang:monotonic_time(microsecond),
                ReopenStart = erlang:monotonic_time(microsecond),
                {ok, QueryBookie} = leveled_bookie:book_start(StartOpts),
                ReopenEnd = erlang:monotonic_time(microsecond),
                Queries = read_queries(maps:get(queries, Opts)),
                QueryResults = run_queries(QueryBookie, Queries, Opts#{doc_count => DocCount}),
                QueryCloseStart = erlang:monotonic_time(microsecond),
                ok = leveled_bookie:book_close(QueryBookie),
                QueryCloseEnd = erlang:monotonic_time(microsecond),
                write_results(
                    maps:get(result, Opts),
                    DocCount,
                    TextBytes,
                    LoadEnd - LoadStart,
                    add_load_status(
                        LoadStatus,
                        LoadStats#{
                            close_us => CloseEnd - CloseStart,
                            query_reopen_us => ReopenEnd - ReopenStart,
                            query_close_us => QueryCloseEnd - QueryCloseStart
                        }
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
            ParseResult = parse_doc(Line, Opts),
            ParseEnd = erlang:monotonic_time(microsecond),
            Stats1 = add_stat(parse_us, ParseEnd - ParseStart, Stats),
            case ParseResult of
                {ok, Op, Bytes} ->
                    NextBatch = [Op | Batch],
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

parse_doc(Line0, Opts) ->
    Line = trim_newline(Line0),
    case binary:split(Line, <<"\t">>, [global]) of
        [Key, Title64, Body64] ->
            try
                Title = base64:decode(Title64),
                Body = base64:decode(Body64),
                Bucket = maps:get(bucket, Opts),
                Op = {put, Bucket, Key, {Title, Body}, [], ?STD_TAG, infinity},
                {ok, Op, byte_size(Title) + byte_size(Body)}
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
    io:format(standard_error, "leveled loaded ~p docs~n", [Count]);
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
    Parent = self(),
    Ref = make_ref(),
    {Pid, Mon} =
        spawn_monitor(fun() ->
            Parent ! {Ref, self(), run_query_1(Bookie, Query, Opts)}
        end),
    receive
        {Ref, Pid, Result} ->
            erlang:demonitor(Mon, [flush]),
            Result;
        {'DOWN', Mon, process, Pid, Reason} ->
            query_error_result(Query, Reason)
    end.

run_query_1(Bookie, Query, Opts) ->
    Warmup = maps:get(warmup, Opts),
    Runs = maps:get(runs, Opts),
    Limit = maps:get(limit, Opts),
    Bucket = maps:get(bucket, Opts),
    Index = maps:get(index, Opts),
    SearchOpts = search_opts(Limit, Opts),
    _ = [search_once(Bookie, Bucket, Index, Query, SearchOpts) || _ <- lists:seq(1, Warmup)],
    Timed =
        [
            begin
                Start = erlang:monotonic_time(microsecond),
                Result = search_once(Bookie, Bucket, Index, Query, SearchOpts),
                Stop = erlang:monotonic_time(microsecond),
                {Stop - Start, result_summary(Result)}
            end
         || _ <- lists:seq(1, Runs)
        ],
    {Times, Summaries} = lists:unzip(Timed),
    LastSummary =
        case Summaries of
            [] -> result_summary({error, no_timed_runs});
            _ -> lists:last(Summaries)
        end,
    TotalLimit = max(Limit, maps:get(doc_count, Opts, Limit)),
    TotalSearchOpts = SearchOpts#{limit := TotalLimit},
    TotalStart = erlang:monotonic_time(microsecond),
    TotalResult = search_once(Bookie, Bucket, Index, Query, TotalSearchOpts),
    TotalStop = erlang:monotonic_time(microsecond),
    TotalCount = result_count(TotalResult),
    TotalError = result_error(total_count, TotalResult),
    Error = first_error([timed_error(Summaries), drift_error(Summaries), TotalError]),
    #{query => Query, count => summary_count(LastSummary), total_count => TotalCount,
        full_result_us => TotalStop - TotalStart, runs_us => Times, error => Error,
        keys => summary_keys(LastSummary), full_keys => result_keys(TotalResult)}.

query_error_result(Query, Reason) ->
    #{query => Query, count => 0, total_count => 0, full_result_us => 0, runs_us => [],
        error => {query_worker, Reason}, keys => [], full_keys => []}.

result_count({ok, Hits}) ->
    length(Hits);
result_count({error, _Reason}) ->
    0.

result_keys({ok, Hits}) ->
    [maps:get(key, Hit) || Hit <- Hits];
result_keys({error, _Reason}) ->
    [].

result_summary({ok, Hits}) ->
    Keys = [maps:get(key, Hit) || Hit <- Hits],
    #{count => length(Keys), keys => Keys, error => none, signature => {ok, Keys}};
result_summary({error, Reason}) ->
    #{count => 0, keys => [], error => {query, Reason}, signature => {error, Reason}}.

summary_count(Summary) ->
    maps:get(count, Summary).

summary_keys(Summary) ->
    maps:get(keys, Summary).

result_error(_Stage, {ok, _Hits}) ->
    none;
result_error(Stage, {error, Reason}) ->
    {Stage, Reason}.

timed_error(Results) ->
    case [Error || #{error := Error} <- Results, Error =/= none] of
        [] -> none;
        [Error | _] -> Error
    end.

drift_error([]) ->
    none;
drift_error([First | Rest]) ->
    FirstSignature = maps:get(signature, First),
    case lists:all(fun(Result) -> maps:get(signature, Result) =:= FirstSignature end, Rest) of
        true -> none;
        false -> inconsistent_timed_results
    end.

first_error([]) ->
    none;
first_error([none | Rest]) ->
    first_error(Rest);
first_error([Error | _Rest]) ->
    Error.

search_once(Bookie, Bucket, Index, Query, SearchOpts) ->
    {async, Runner} = leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, SearchOpts),
    Runner().

search_opts(Limit, _Opts) ->
    #{
        rank => none,
        limit => Limit,
        columns => [title, body],
        prefixes => [5, 11],
        remove_diacritics => 2
    }.

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
    ok = io:format(File, "leveled\tqueries\t~s\t\t\t\t\t\t~n", [
        escape_cell(unicode:characters_to_binary(maps:get(queries, Opts)))
    ]),
    ok = io:format(File, "leveled\tquery_count\t~p\t\t\t\t\t\t~n", [
        length(QueryResults)
    ]),
    ok = io:format(File, "leveled\tquery_sha256\t~s\t\t\t\t\t\t~n", [
        escape_cell(query_results_sha256(QueryResults))
    ]),
    ok = io:format(File, "leveled\tlimit\t~p\t\t\t\t\t\t~n", [maps:get(limit, Opts)]),
    ok = io:format(File, "leveled\truns\t~p\t\t\t\t\t\t~n", [maps:get(runs, Opts)]),
    ok = io:format(File, "leveled\twarmup\t~p\t\t\t\t\t\t~n", [maps:get(warmup, Opts)]),
    ok = io:format(File, "leveled\tsync_strategy\tnone\t\t\t\t\t\t~n", []),
    ok = io:format(File, "leveled\tdocs\t~p\t\t\t\t\t\t~n", [DocCount]),
    ok = io:format(File, "leveled\ttext_bytes\t~p\t\t\t\t\t\t~n", [TextBytes]),
    ok = io:format(File, "leveled\tload_us\t~p\t\t\t\t\t\t~n", [LoadUs]),
    ok = io:format(File, "leveled\tload_finalized_us\t~p\t\t\t\t\t\t~n", [
        LoadUs + maps:get(close_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_reopened_us\t~p\t\t\t\t\t\t~n", [
        LoadUs + maps:get(close_us, LoadStats, 0) + maps:get(query_reopen_us, LoadStats, 0)
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
    ok = io:format(File, "leveled\tload_batch_size\t~p\t\t\t\t\t\t~n", [
        maps:get(batch, Opts)
    ]),
    ok = io:format(File, "leveled\tload_batches\t~p\t\t\t\t\t\t~n", [
        maps:get(batches, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tload_pauses\t~p\t\t\t\t\t\t~n", [
        maps:get(pauses, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tcollapsed_ops_count\t0\t\t\t\t\t\t~n", []),
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
    ok = io:format(File, "leveled\tquery_reopen_us\t~p\t\t\t\t\t\t~n", [
        maps:get(query_reopen_us, LoadStats, 0)
    ]),
    ok = io:format(File, "leveled\tquery_close_us\t~p\t\t\t\t\t\t~n", [
        maps:get(query_close_us, LoadStats, 0)
    ]),
    ok = io:format(
        File,
        "leveled\tload_transaction_contract\t~s\t\t\t\t\t\t~n",
        [escape_cell(<<"book_batchput with automatic fts_indexes every --batch TSV documents">>)]
    ),
    ok = io:format(
        File,
        "leveled\tquery_connection_contract\t~s\t\t\t\t\t\t~n",
        [escape_cell(<<"close after load, reopen before timed queries">>)]
    ),
    ok = io:format(File, "leveled\tstore_bytes\t~p\t\t\t\t\t\t~n", [
        directory_size(maps:get(root, Opts))
    ]),
    ok = io:format(File, "leveled\tcompression_method\t~p\t\t\t\t\t\t~n", [
        maps:get(compression_method, Opts)
    ]),
    ok = io:format(File, "leveled\tledger_compression\t~p\t\t\t\t\t\t~n", [
        maps:get(ledger_compression, Opts)
    ]),
    ok = io:format(File, "leveled\tcache_size\t~p\t\t\t\t\t\t~n", [
        maps:get(cache_size, Opts, default)
    ]),
    ok = io:format(File, "leveled\tcache_multiple\t~p\t\t\t\t\t\t~n", [
        maps:get(cache_multiple, Opts, default)
    ]),
    ok = io:format(
        File,
        "leveled\tfts_representation_contract\t~s\t\t\t\t\t\t~n",
        [escape_cell(<<"secondary_index_payload_postings">>)]
    ),
    ok = io:format(File, "leveled\tprefix_execution_contract\t~s\t\t\t\t\t\t~n", [
        escape_cell(prefix_execution_contract())
    ]),
    ok = io:format(File, "leveled\tpayload_visibility_contract\t~s\t\t\t\t\t\t~n", [
        escape_cell(payload_visibility_contract())
    ]),
    ok = io:format(File, "leveled\trank_none_snapshot_contract\t~s\t\t\t\t\t\t~n", [
        escape_cell(rank_none_snapshot_contract())
    ]),
    ok = io:format(File, "leveled\tsource_object_shape\t~s\t\t\t\t\t\t~n", [
        escape_cell(<<"{Title, Body}">>)
    ]),
    ok = io:format(File, "leveled\tquery_order_contract\t~s\t\t\t\t\t\t~n", [
        escape_cell(<<"rank_none_order_by_key">>)
    ]),
    ok = io:format(File, "leveled\tsearch_opts\t~s\t\t\t\t\t\t~n", [
        escape_cell(list_to_binary(io_lib:format("~p", [
            search_opts(maps:get(limit, Opts), Opts)
        ])))
    ]),
    lists:foreach(fun(Result) -> write_query_result(File, Result) end, QueryResults),
    ok = file:close(File),
    ok.

cache_start_opts(Opts) ->
    CacheSize = maps:get(cache_size, Opts, default),
    CacheMultiple = maps:get(cache_multiple, Opts, default),
    MaxPencillerCacheSize = maps:get(max_pencillercachesize, Opts, default),
    lists:append([
        case CacheSize of
            default -> [];
            _ -> [{cache_size, CacheSize}]
        end,
        case CacheMultiple of
            default -> [];
            _ -> [{cache_multiple, CacheMultiple}]
        end,
        case MaxPencillerCacheSize of
            default -> [];
            _ -> [{max_pencillercachesize, MaxPencillerCacheSize}]
        end
    ]).

write_query_result(
    File,
    #{query := Query, count := Count, total_count := TotalCount, full_result_us := FullResultUs,
        runs_us := Runs, error := Error, keys := Keys, full_keys := FullKeys}
) ->
    RunsBin = join_integer_list(Runs),
    KeysCell = json_binary_list(Keys),
    FullKeysCell = json_binary_list(FullKeys),
    ok = io:format(
        File,
        "leveled\tfull_result_us\t~p\t~s\t\t~p\t\t~s\t~s~n",
        [FullResultUs, escape_cell(Query), TotalCount, term_cell(Error), escape_cell(FullKeysCell)]
    ),
    ok = io:format(
        File,
        "leveled\tquery_us\t\t~s\t~p\t~p\t~s\t~s\t~s~n",
        [escape_cell(Query), Count, TotalCount, RunsBin, term_cell(Error), escape_cell(KeysCell)]
    ).

term_cell(Term) ->
    escape_cell(iolist_to_binary(io_lib:format("~w", [Term]))).

prefix_execution_contract() ->
    <<"secondary_index_token_range_scan">>.

payload_visibility_contract() ->
    <<"no_hidden_objects_payload_in_index_metadata">>.

rank_none_snapshot_contract() ->
    <<"rank_none_payload_key_order">>.

query_results_sha256(QueryResults) ->
    QueryLines = [maps:get(query, Result) || Result <- QueryResults],
    sha256_hex(iolist_to_binary([[Query, <<"\n">>] || Query <- QueryLines])).

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

sha256_hex(Bin) ->
    hex_binary(crypto:hash(sha256, Bin)).

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
