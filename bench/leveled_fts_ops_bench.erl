-module(leveled_fts_ops_bench).

-include("leveled.hrl").
-include_lib("kernel/include/file.hrl").

-export([main/1]).

main(Args) ->
    case parse_args(Args) of
        {ok, Opts} ->
            run(Opts);
        {error, Reason} ->
            io:format(standard_error, "leveled_fts_ops_bench: ~p~n", [Reason]),
            {error, Reason}
    end.

parse_args(Args) ->
    Defaults = #{
        batch => 50,
        bucket => <<"bench">>,
        compression_method => native,
        index => <<"main">>,
        ledger_compression => as_store,
        limit => 2000,
        runs => 5,
        warmup => 1
    },
    parse_args(Args, Defaults).

parse_args([], Opts) ->
    Required = [ops, root, queries, result],
    case [K || K <- Required, not maps:is_key(K, Opts)] of
        [] -> {ok, Opts};
        Missing -> {error, {missing_args, Missing}}
    end;
parse_args(["--ops", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{ops => Path});
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
    ],
    {ok, Bookie} = leveled_bookie:book_start(StartOpts),
    LoadStart = erlang:monotonic_time(microsecond),
    LoadResult = load_ops(Bookie, Opts),
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

load_ops(Bookie, Opts) ->
    Ops = maps:get(ops, Opts),
    BatchSize = maps:get(batch, Opts),
    case file:open(Ops, [read, binary, {read_ahead, 1024 * 1024}]) of
        {ok, File} ->
            Result = load_loop(
                File,
                Bookie,
                Opts,
                BatchSize,
                [],
                0,
                #{},
                #{},
                #{},
                #{parse_us => 0, write_us => 0, batches => 0, ops_count => 0,
                    puts => 0, deletes => 0, collapsed_ops_count => 0}
            ),
            ok = file:close(File),
            Result;
        {error, Reason} ->
            {error, {open_ops, Reason}}
    end.

load_loop(
    File, Bookie, Opts, BatchSize, Batch, BatchCount, BatchKeys, SourceIdKeys, Active, Stats
) ->
    case file:read_line(File) of
        eof ->
            case timed_write_batch(Bookie, Batch, Stats) of
                {ok, FinalStats} ->
                    {ok, maps:size(Active), active_text_bytes(Active), FinalStats};
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, Line} ->
            ParseStart = erlang:monotonic_time(microsecond),
            ParseResult = parse_op(Line, Opts),
            ParseEnd = erlang:monotonic_time(microsecond),
            Stats1 = add_stat(parse_us, ParseEnd - ParseStart, Stats),
            case ParseResult of
                {ok, Op, SourceId, Key, Kind, TextBytes} ->
                    case stable_source_id_key(SourceId, Key, Kind, SourceIdKeys) of
                        {ok, SourceIdKeys1} ->
                            case
                                maybe_flush_repeated_batch_identity(
                                    Bookie, Batch, BatchCount, BatchKeys, Stats1, Op
                                )
                            of
                                {ok, Batch0, BatchCount0, BatchKeys0, Stats1A} ->
                                    Active1 = update_active(Kind, Key, TextBytes, Active),
                                    Stats2 = add_operation_stats(Kind, Stats1A),
                                    Batch1 = [Op | Batch0],
                                    BatchCount1 = BatchCount0 + 1,
                                    BatchKeys1 = add_batch_identity(Op, BatchKeys0),
                                    case BatchCount1 >= BatchSize of
                                        true ->
                                            case timed_write_batch(Bookie, Batch1, Stats2) of
                                                {ok, Stats3} ->
                                                    load_loop(
                                                        File,
                                                        Bookie,
                                                        Opts,
                                                        BatchSize,
                                                        [],
                                                        0,
                                                        #{},
                                                        SourceIdKeys1,
                                                        Active1,
                                                        Stats3
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
                                                Batch1,
                                                BatchCount1,
                                                BatchKeys1,
                                                SourceIdKeys1,
                                                Active1,
                                                Stats2
                                            )
                                    end;
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {read_ops, Reason}}
    end.

parse_op(Line0, Opts) ->
    Line = trim_newline(Line0),
    case binary:split(Line, <<"\t">>, [global]) of
        [<<"put">>, SourceId, Key, Title64, Body64] ->
            case parse_source_id(SourceId) of
                {ok, SourceIdInt} ->
                    try
                        Title = base64:decode(Title64),
                        Body = base64:decode(Body64),
                        Bucket = maps:get(bucket, Opts),
                        Op = {put, Bucket, Key, {Title, Body}, [], ?STD_TAG, infinity},
                        {ok, Op, SourceIdInt, Key, put, byte_size(Title) + byte_size(Body)}
                    catch
                        _:Reason ->
                            {error, {invalid_put_op, Reason}}
                    end;
                error ->
                    {error, {invalid_source_id, SourceId}}
            end;
        [<<"delete">>, SourceId, Key, <<>>, <<>>] ->
            case parse_source_id(SourceId) of
                {ok, SourceIdInt} ->
                    Bucket = maps:get(bucket, Opts),
                    {ok, {delete, Bucket, Key, [], ?STD_TAG, infinity}, SourceIdInt, Key,
                        delete, 0};
                error ->
                    {error, {invalid_source_id, SourceId}}
            end;
        Other ->
            {error, {invalid_ops_line, Other}}
    end.

parse_source_id(SourceId) ->
    try binary_to_integer(SourceId) of
        N when N > 0 -> {ok, N};
        _Other -> error
    catch
        _:_ -> error
    end.

stable_source_id_key(SourceId, Key, put, SourceIdKeys) ->
    case maps:get(SourceId, SourceIdKeys, undefined) of
        undefined -> {ok, maps:put(SourceId, Key, SourceIdKeys)};
        Key -> {ok, SourceIdKeys};
        ExistingKey -> {error, {unstable_source_id_key, SourceId, ExistingKey, Key}}
    end;
stable_source_id_key(SourceId, Key, delete, SourceIdKeys) ->
    case maps:get(SourceId, SourceIdKeys, undefined) of
        undefined -> {ok, SourceIdKeys};
        Key -> {ok, SourceIdKeys};
        ExistingKey -> {error, {unstable_source_id_key, SourceId, ExistingKey, Key}}
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

update_active(put, Key, TextBytes, Active) ->
    maps:put(Key, TextBytes, Active);
update_active(delete, Key, _TextBytes, Active) ->
    maps:remove(Key, Active).

active_text_bytes(Active) ->
    maps:fold(fun(_Key, Bytes, Acc) -> Acc + Bytes end, 0, Active).

add_operation_stats(put, Stats) ->
    add_stat(puts, 1, add_stat(ops_count, 1, Stats));
add_operation_stats(delete, Stats) ->
    add_stat(deletes, 1, add_stat(ops_count, 1, Stats)).

maybe_flush_repeated_batch_identity(_Bookie, [], 0, BatchKeys, Stats, _Op) ->
    {ok, [], 0, BatchKeys, Stats};
maybe_flush_repeated_batch_identity(Bookie, Batch, BatchCount, BatchKeys, Stats, Op) ->
    case batch_op_identity(Op) of
        {ok, Id} ->
            case maps:is_key(Id, BatchKeys) of
                true ->
                    case timed_write_batch(Bookie, Batch, Stats) of
                        {ok, Stats1} -> {ok, [], 0, #{}, Stats1};
                        {error, Reason} -> {error, Reason}
                    end;
                false ->
                    {ok, Batch, BatchCount, BatchKeys, Stats}
            end;
        error ->
            {ok, Batch, BatchCount, BatchKeys, Stats}
    end.

add_batch_identity(Op, BatchKeys) ->
    case batch_op_identity(Op) of
        {ok, Id} -> BatchKeys#{Id => true};
        error -> BatchKeys
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
    CollapsedOps = batch_collapsed_ops_count(Batch),
    Start = erlang:monotonic_time(microsecond),
    Result = write_batch(Bookie, Batch),
    Stop = erlang:monotonic_time(microsecond),
    Stats1 = add_stat(collapsed_ops_count, CollapsedOps, Stats),
    case Result of
        ok ->
            {ok, add_stat(batches, 1, add_stat(write_us, Stop - Start, Stats1))};
        pause ->
            {ok,
                add_stat(
                    pauses,
                    1,
                    add_stat(batches, 1, add_stat(write_us, Stop - Start, Stats1))
                )};
        {error, Reason} ->
            {error, Reason}
    end.

batch_collapsed_ops_count(Batch) ->
    Ids = [Id || Op <- Batch, {ok, Id} <- [batch_op_identity(Op)]],
    length(Ids) - maps:size(maps:from_keys(Ids, true)).

batch_op_identity({put, Bucket, Key, _Object, _IndexSpecs, Tag, _TTL}) ->
    {ok, {Bucket, Key, Tag}};
batch_op_identity({delete, Bucket, Key, _IndexSpecs, Tag, _TTL}) ->
    {ok, {Bucket, Key, Tag}};
batch_op_identity(_Op) ->
    error.

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
    ok = write_metric(File, <<"ops">>, unicode:characters_to_binary(maps:get(ops, Opts))),
    ok = write_metric(File, <<"queries">>, unicode:characters_to_binary(maps:get(queries, Opts))),
    ok = write_metric(File, <<"query_count">>, integer_to_binary(length(QueryResults))),
    ok = write_metric(File, <<"query_sha256">>, query_results_sha256(QueryResults)),
    ok = write_metric(File, <<"limit">>, integer_to_binary(maps:get(limit, Opts))),
    ok = write_metric(File, <<"runs">>, integer_to_binary(maps:get(runs, Opts))),
    ok = write_metric(File, <<"warmup">>, integer_to_binary(maps:get(warmup, Opts))),
    ok = write_metric(File, <<"sync_strategy">>, <<"none">>),
    ok = write_metric(File, <<"root">>, unicode:characters_to_binary(maps:get(root, Opts))),
    ok = write_metric(File, <<"docs">>, integer_to_binary(DocCount)),
    ok = write_metric(File, <<"text_bytes">>, integer_to_binary(TextBytes)),
    ok = write_metric(File, <<"load_us">>, integer_to_binary(LoadUs)),
    ok = write_metric(
        File,
        <<"load_finalized_us">>,
        integer_to_binary(LoadUs + maps:get(close_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_reopened_us">>,
        integer_to_binary(
            LoadUs + maps:get(close_us, LoadStats, 0) + maps:get(query_reopen_us, LoadStats, 0)
        )
    ),
    ok = write_metric(File, <<"load_parse_us">>, integer_to_binary(maps:get(parse_us, LoadStats, 0))),
    ok = write_metric(File, <<"load_write_us">>, integer_to_binary(maps:get(write_us, LoadStats, 0))),
    ok = write_metric(File, <<"load_batch_size">>, integer_to_binary(maps:get(batch, Opts))),
    ok = write_metric(File, <<"load_batches">>, integer_to_binary(maps:get(batches, LoadStats, 0))),
    ok = write_metric(File, <<"load_pauses">>, integer_to_binary(maps:get(pauses, LoadStats, 0))),
    ok = write_metric(
        File, <<"load_put_sample_count">>, integer_to_binary(maps:get(put_sample_count, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_do_batchput_ink_us">>,
        integer_to_binary(maps:get(load_do_batchput_ink_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_do_batchput_prep_us">>,
        integer_to_binary(maps:get(load_do_batchput_prep_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_do_batchput_mem_us">>,
        integer_to_binary(maps:get(load_do_batchput_mem_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_do_batchput_profiled_us">>,
        integer_to_binary(maps:get(load_do_batchput_profiled_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_write_profiled_us">>,
        integer_to_binary(maps:get(load_write_profiled_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_write_unprofiled_us">>,
        integer_to_binary(maps:get(load_write_unprofiled_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"collapsed_ops_count">>,
        integer_to_binary(maps:get(collapsed_ops_count, LoadStats, 0))
    ),
    ok = write_metric(File, <<"ops_count">>, integer_to_binary(maps:get(ops_count, LoadStats, 0))),
    ok = write_metric(File, <<"puts">>, integer_to_binary(maps:get(puts, LoadStats, 0))),
    ok = write_metric(File, <<"deletes">>, integer_to_binary(maps:get(deletes, LoadStats, 0))),
    ok = write_metric(File, <<"close_us">>, integer_to_binary(maps:get(close_us, LoadStats, 0))),
    ok = write_metric(
        File,
        <<"query_reopen_us">>,
        integer_to_binary(maps:get(query_reopen_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"query_close_us">>,
        integer_to_binary(maps:get(query_close_us, LoadStats, 0))
    ),
    ok = write_metric(
        File,
        <<"load_transaction_contract">>,
        <<"book_batchput with automatic fts_indexes every --batch operations">>
    ),
    ok = write_metric(
        File,
        <<"query_connection_contract">>,
        <<"close after load, reopen before timed queries">>
    ),
    ok = write_metric(File, <<"store_bytes">>, integer_to_binary(directory_size(maps:get(root, Opts)))),
    ok = write_metric(
        File, <<"compression_method">>, atom_to_binary(maps:get(compression_method, Opts), utf8)
    ),
    ok = write_metric(
        File, <<"ledger_compression">>, atom_to_binary(maps:get(ledger_compression, Opts), utf8)
    ),
    ok = write_metric(
        File,
        <<"fts_representation_contract">>,
        <<"secondary_index_payload_postings">>
    ),
    ok = write_metric(
        File,
        <<"prefix_execution_contract">>,
        prefix_execution_contract()
    ),
    ok = write_metric(File, <<"payload_visibility_contract">>, payload_visibility_contract()),
    ok = write_metric(File, <<"rank_none_snapshot_contract">>, rank_none_snapshot_contract()),
    ok = write_metric(File, <<"source_object_shape">>, <<"{Title, Body}">>),
    ok = write_metric(
        File,
        <<"mutation_contract">>,
        <<"book_batchput put/delete with automatic fts_indexes; book_ftssearch rank none">>
    ),
    ok = write_metric(File, <<"query_order_contract">>, <<"rank_none_order_by_key">>),
    ok = write_metric(
        File,
        <<"search_opts">>,
        list_to_binary(io_lib:format("~p", [search_opts(maps:get(limit, Opts), Opts)]))
    ),
    lists:foreach(fun(Result) -> write_query_result(File, Result) end, QueryResults),
    ok = file:close(File),
    ok.

write_metric(File, Metric, Value) ->
    ok = io:format(File, "leveled\t~s\t~s\t\t\t\t\t\t~n", [
        escape_cell(Metric), escape_cell(Value)
    ]).

prefix_execution_contract() ->
    <<"secondary_index_token_range_scan">>.

payload_visibility_contract() ->
    <<"no_hidden_objects_payload_in_index_metadata">>.

rank_none_snapshot_contract() ->
    <<"rank_none_payload_key_order">>.

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

query_results_sha256(QueryResults) ->
    QueryLines = [maps:get(query, Result) || Result <- QueryResults],
    sha256_hex(iolist_to_binary([[Query, <<"\n">>] || Query <- QueryLines])).

sha256_hex(Bin) ->
    Hex = [
        begin
            <<Hi:4, Lo:4>> = <<Byte>>,
            [hex_digit(Hi), hex_digit(Lo)]
        end
     || <<Byte>> <= crypto:hash(sha256, Bin)
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
