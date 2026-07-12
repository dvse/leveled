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
        max_journalobjectcount => 200000,
        max_journalsize => 1000000000,
        max_mergebelow => default,
        max_sstslots => default,
        rank => none,
        uncached => false,
        bust_mode => none,
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
parse_args(["--max-journalsize", N | Rest], Opts) ->
    parse_args(Rest, Opts#{max_journalsize => list_to_integer(N)});
parse_args(["--max-journalobjectcount", N | Rest], Opts) ->
    parse_args(Rest, Opts#{max_journalobjectcount => list_to_integer(N)});
parse_args(["--max-sstslots", N | Rest], Opts) ->
    parse_args(Rest, Opts#{max_sstslots => list_to_integer(N)});
parse_args(["--max-mergebelow", N | Rest], Opts) ->
    parse_args(Rest, Opts#{max_mergebelow => list_to_integer(N)});
parse_args(["--ledger-compression", Method | Rest], Opts) ->
    case ledger_compression(Method) of
        {ok, Atom} -> parse_args(Rest, Opts#{ledger_compression => Atom});
        error -> {error, {invalid_ledger_compression, Method}}
    end;
parse_args(["--rank", R | Rest], Opts) ->
    case rank_mode(R) of
        {ok, Atom} -> parse_args(Rest, Opts#{rank => Atom});
        error -> {error, {invalid_rank, R}}
    end;
parse_args(["--uncached" | Rest], Opts) ->
    parse_args(Rest, Opts#{uncached => true, bust_mode => always});
parse_args(["--amortized" | Rest], Opts) ->
    parse_args(Rest, Opts#{uncached => true, bust_mode => amortized});
parse_args(["--settle-ms", V | Rest], Opts) ->
    parse_args(Rest, Opts#{settle_ms => list_to_integer(V)});
parse_args(["--compact" | Rest], Opts) ->
    parse_args(Rest, Opts#{compact => true});
parse_args(["--stable" | Rest], Opts) ->
    parse_args(Rest, Opts#{bust_mode => stable});
parse_args(["--skip-load", DocCount | Rest], Opts) ->
    parse_args(Rest, Opts#{skip_load => list_to_integer(DocCount)});
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

run(Opts0) ->
    {ok, Schema} =
        leveled_fts:schema(#{
            index => maps:get(index, Opts0),
            columns => [title, body],
            prefixes => [5, 11],
            remove_diacritics => 2
        }),
    Opts = Opts0#{schema => Schema},
    Root = maps:get(root, Opts),
    ok =
        case maps:get(skip_load, Opts, undefined) of
            undefined -> reset_dir(Root);
            _Existing -> ok
        end,
    StartOpts = [
        {root_path, Root},
        {sync_strategy, none},
        {log_level, warn},
        {max_journalsize, maps:get(max_journalsize, Opts)},
        {max_journalobjectcount, maps:get(max_journalobjectcount, Opts)},
        {compression_method, maps:get(compression_method, Opts)},
        {ledger_compression, maps:get(ledger_compression, Opts)},
        {stats_percentage, 100},
        {monitor_loglist, []}
    ] ++ cache_start_opts(Opts) ++ sst_start_opts(Opts),
    {ok, Bookie} = leveled_bookie:book_start(StartOpts),
    LoadStart = erlang:monotonic_time(microsecond),
    %% --skip-load N: measure against an existing store (e.g. the store
    %% the other rank mode just built and compacted — identical state).
    LoadResult =
        case maps:get(skip_load, Opts, undefined) of
            undefined -> load_tsv(Bookie, Opts);
            SkipDocs -> {ok, SkipDocs, 0, #{}}
        end,
    LoadEnd = erlang:monotonic_time(microsecond),
    Result =
        case LoadResult of
            {ok, DocCount, TextBytes, LoadStats} ->
                %% skip-load stores are already compacted by the builder.
                SkipMaintenance = maps:get(skip_load, Opts, undefined) =/= undefined,
                %% --compact: converge the store to few posting batches
                %% before measuring, the steady state a maintained store
                %% runs at (SQLite's index is likewise fully merged after
                %% its build). Oldest-first selection makes merged
                %% batches (newest sequences) accumulate at the tail, so
                %% convergence rewrites each posting about twice.
                ok =
                    case SkipMaintenance of
                        true -> ok;
                        false -> maybe_compact(Bookie, Opts)
                    end,
                LoadStatus = leveled_bookie:book_status(Bookie),
                LoadMemory = memory_snapshot(),
                CloseStart = erlang:monotonic_time(microsecond),
                ok = leveled_bookie:book_close(Bookie),
                CloseEnd = erlang:monotonic_time(microsecond),
                erlang:garbage_collect(),
                ClosedMemory = memory_snapshot(),
                ReopenStart = erlang:monotonic_time(microsecond),
                {ok, QueryBookie} = leveled_bookie:book_start(StartOpts),
                ReopenEnd = erlang:monotonic_time(microsecond),
                Queries = read_queries(maps:get(queries, Opts)),
                QueryRowsPath = maps:get(result, Opts) ++ ".queries.tmp",
                Sentinel =
                    case maps:get(uncached, Opts, false) of
                        true -> read_first_doc(Opts);
                        false -> undefined
                    end,
                {ok, QueryStats} = run_queries_to_file(
                    QueryBookie,
                    Queries,
                    Opts#{doc_count => DocCount, sentinel => Sentinel},
                    QueryRowsPath
                ),
                QueryMemory = memory_snapshot(),
                QueryCloseStart = erlang:monotonic_time(microsecond),
                ok = leveled_bookie:book_close(QueryBookie),
                QueryCloseEnd = erlang:monotonic_time(microsecond),
                erlang:garbage_collect(),
                FinalMemory = memory_snapshot(),
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
                            query_close_us => QueryCloseEnd - QueryCloseStart,
                            memory_after_load => LoadMemory,
                            memory_after_load_close => ClosedMemory,
                            memory_after_queries => QueryMemory,
                            memory_after_query_close => FinalMemory,
                            query_memory_peak_total =>
                                maps:get(query_memory_peak_total, QueryStats, 0)
                        }
                    ),
                    length(Queries),
                    query_list_sha256(Queries),
                    QueryRowsPath,
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

rank_mode("none") -> {ok, none};
rank_mode("bm25") -> {ok, bm25};
rank_mode(_) -> error.

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
            case timed_write_batch(Bookie, Batch, Stats, Opts) of
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
                            case timed_write_batch(Bookie, NextBatch, Stats1, Opts) of
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
                Op = {doc, Key, #{title => Title, body => Body}},
                {ok, Op, byte_size(Title) + byte_size(Body)}
            catch
                _:Reason ->
                    {error, {invalid_doc_line, Reason}}
            end;
        Other ->
            {error, {invalid_doc_line, Other}}
    end.

%% Read the first TSV document as a sentinel for --uncached cache busting.
%% Re-putting this exact object is idempotent for query results (same key,
%% same content, same token count) but advances the FTS write sequence,
%% invalidating the per-sequence result and corpus-stats caches.
read_first_doc(Opts) ->
    Tsv = maps:get(tsv, Opts),
    {ok, File} = file:open(Tsv, [read, binary, {read_ahead, 1024 * 1024}]),
    try
        case file:read_line(File) of
            {ok, Line} ->
                case parse_doc(Line, Opts) of
                    {ok, {doc, Key, Object}, _Bytes} ->
                        {Key, Object};
                    _Other ->
                        undefined
                end;
            _Other ->
                undefined
        end
    after
        ok = file:close(File)
    end.

maybe_compact(Bookie, #{compact := true, schema := Schema}) ->
    Start = erlang:monotonic_time(microsecond),
    Result = leveled_fts:consolidate(Bookie, Schema, #{}),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    io:format("consolidated (~p) in ~.1f s~n", [Result, Elapsed / 1000000]),
    %% let the LSM digest the maintenance burst before the timed window:
    %% queries measured seconds after a bulk write measure the settling,
    %% not the store (observed as 10x floor noise on selective cells).
    %% Fast iteration loops pass --settle-ms 0.
    timer:sleep(maps:get(settle_ms, Opts, 30000)),
    ok;
maybe_compact(_Bookie, _Opts) ->
    ok.

maybe_bust_cache(_Bookie, #{sentinel := undefined}) ->
    ok;
maybe_bust_cache(Bookie, #{sentinel := {Key, Object}, schema := Schema}) ->
    {ok, Specs} = leveled_fts:derive(Schema, Key, Object),
    _ = leveled_bookie:book_mput(Bookie, Specs),
    ok;
maybe_bust_cache(_Bookie, _Opts) ->
    ok.

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

write_batch(_Bookie, [], _Opts) ->
    ok;
write_batch(Bookie, Batch, #{schema := Schema} = _Opts) ->
    %% Library model (docs/FTS.md): each doc's postings/manifest/epoch/
    %% stats rows come from pure derive/3; the source object rides the
    %% same batch as a head row; ONE book_mput commits the whole batch
    %% atomically under one SQN.
    Bucket = maps:get(index, Schema),
    Specs0 =
        lists:append(
            lists:map(
                fun({doc, Key, Object}) ->
                    {ok, DocSpecs} = leveled_fts:derive(Schema, Key, Object),
                    [{add, Bucket, <<"src">>, Key, Object} | DocSpecs]
                end,
                lists:reverse(Batch)
            )
        ),
    %% one epoch bump per touched shard per BATCH (not per doc): dedupe
    %% by row identity, last spec wins - identical values for epoch rows
    Specs =
        maps:values(
            lists:foldl(
                fun({_, B, K, SK, _} = Spec, Acc) -> Acc#{{B, K, SK} => Spec} end,
                #{},
                Specs0
            )
        ),
    case leveled_bookie:book_mput(Bookie, Specs) of
        ok ->
            ok;
        pause ->
            timer:sleep(50),
            pause;
        {error, Reason} ->
            {error, Reason}
    end.

timed_write_batch(_Bookie, [], Stats, _Opts) ->
    {ok, Stats};
timed_write_batch(Bookie, Batch, Stats, Opts) ->
    Start = erlang:monotonic_time(microsecond),
    Result = write_batch(Bookie, Batch, Opts),
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

memory_snapshot() ->
    maps:from_list(erlang:memory()).

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

run_queries_to_file(Bookie, Queries, Opts, Path) ->
    ok = filelib:ensure_dir(Path),
    {ok, File} = file:open(Path, [write, binary]),
    try
        QueryStats = lists:foldl(
            fun(Query, Stats) ->
                Result = run_query(Bookie, Query, Opts),
                ok = write_query_result(File, Result),
                erlang:garbage_collect(),
                Memory = memory_snapshot(),
                Stats#{
                    query_memory_peak_total =>
                        max(
                            maps:get(query_memory_peak_total, Stats, 0),
                            maps:get(total, Memory, 0)
                        )
                }
            end,
            #{},
            Queries
        ),
        {ok, QueryStats}
    after
        ok = file:close(File)
    end.

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
    Schema = maps:get(schema, Opts),
    SearchOpts = search_opts(Limit, Opts),
    _ = [search_once(Bookie, Schema, Query, SearchOpts) || _ <- lists:seq(1, Warmup)],
    Timed =
        [
            begin
                %% Cache-bust regimes under the LIBRARY model (docs/FTS.md):
                %% there is NO result cache - every query evaluates from
                %% per-shard cached states validated by epoch-row SQNs.
                %%   always    (--uncached): an idempotent re-derive+mput of
                %%     an existing document BEFORE the timer bumps the epoch
                %%     rows of every shard it touches, so the timed query
                %%     refolds those shards from a snapshot and then
                %%     evaluates. Write-per-query worst case.
                %%   amortized (--amortized): bump once, then run the SAME
                %%     query untimed to refold its shards; the timed run is
                %%     then epoch-check + evaluation - the steady-state cost
                %%     of a novel query between writes.
                %%   stable (--stable): NO writes - epoch-check + evaluation
                %%     against a write-quiescent store (equivalent to
                %%     amortized without the write; kept for ladder compat).
                _ = maybe_bust_cache(Bookie, Opts),
                BustMode = maps:get(bust_mode, Opts, none),
                _ =
                    case BustMode of
                        amortized ->
                            search_once(Bookie, Schema, Query, SearchOpts);
                        _ ->
                            ok
                    end,
                Start = erlang:monotonic_time(microsecond),
                Result = search_once(Bookie, Schema, Query, SearchOpts),
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
    TotalSearchOpts = SearchOpts#{limit => maps:get(total_limit, Opts, 1000000)},
    TotalStart = erlang:monotonic_time(microsecond),
    TotalResult = search_once(Bookie, Schema, Query, TotalSearchOpts),
    TotalStop = erlang:monotonic_time(microsecond),
    TotalCount = result_total_count(TotalResult),
    TotalError = result_error(total_count, TotalResult),
    Error = first_error([timed_error(Summaries), drift_error(Summaries), TotalError]),
    RankMode = maps:get(rank, Opts, none),
    EmitScores =
        case RankMode of
            bm25 -> summary_scores(LastSummary);
            _ -> []
        end,
    #{query => Query, count => summary_count(LastSummary), total_count => TotalCount,
        full_result_us => TotalStop - TotalStart, runs_us => Times, error => Error,
        keys => summary_keys(LastSummary), scores => EmitScores, rank_mode => RankMode,
        full_keys_sha256 => result_full_keys_sha256(TotalResult)}.

query_error_result(Query, Reason) ->
    #{query => Query, count => 0, total_count => 0, full_result_us => 0, runs_us => [],
        error => {query_worker, Reason}, keys => [], scores => [], full_keys => []}.

result_count({ok, Hits}) ->
    length(Hits);
result_count({error, _Reason}) ->
    0.

result_total_count({ok, #{total_count := TotalCount}}) ->
    TotalCount;
result_total_count(Result) ->
    result_count(Result).

result_full_keys_sha256({ok, #{full_keys_sha256 := Hash}}) ->
    Hash;
result_full_keys_sha256({ok, Hits}) ->
    keys_sha256(Hits);
result_full_keys_sha256({error, _Reason}) ->
    <<>>.

result_summary({ok, Hits}) ->
    Keys = [maps:get(key, Hit) || Hit <- Hits],
    Scores = [maps:get(score, Hit, 0.0) || Hit <- Hits],
    #{count => length(Keys), keys => Keys, scores => Scores, error => none,
        signature => {ok, Keys}};
result_summary({error, Reason}) ->
    #{count => 0, keys => [], scores => [], error => {query, Reason},
        signature => {error, Reason}}.

summary_count(Summary) ->
    maps:get(count, Summary).

summary_keys(Summary) ->
    maps:get(keys, Summary).

summary_scores(Summary) ->
    maps:get(scores, Summary, []).

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

search_once(Bookie, Schema, Query, SearchOpts) ->
    leveled_fts:search(Bookie, Schema, Query, SearchOpts).

search_opts(Limit, Opts) ->
    %% tokenizer/prefix settings are schema-level in the library model
    #{
        rank => maps:get(rank, Opts, none),
        limit => Limit,
        columns => [title, body]
    }.

write_results(Path, DocCount, TextBytes, LoadUs, LoadStats, QueryCount, QuerySha, QueryRowsPath, Opts) ->
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
        QueryCount
    ]),
    ok = io:format(File, "leveled\tquery_sha256\t~s\t\t\t\t\t\t~n", [
        escape_cell(QuerySha)
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
    ok = write_memory_metrics(File, LoadStats),
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
    Root = maps:get(root, Opts),
    RootBytes = directory_size(Root),
    StoreBytes = directory_size(Root, live),
    ok = io:format(File, "leveled\tstore_bytes\t~p\t\t\t\t\t\t~n", [StoreBytes]),
    ok = io:format(File, "leveled\troot_bytes\t~p\t\t\t\t\t\t~n", [RootBytes]),
    ok = io:format(File, "leveled\tarchived_bytes\t~p\t\t\t\t\t\t~n", [
        RootBytes - StoreBytes
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
    ok = io:format(File, "leveled\tmax_journalsize\t~p\t\t\t\t\t\t~n", [
        maps:get(max_journalsize, Opts)
    ]),
    ok = io:format(File, "leveled\tmax_journalobjectcount\t~p\t\t\t\t\t\t~n", [
        maps:get(max_journalobjectcount, Opts)
    ]),
    ok = io:format(File, "leveled\tmax_sstslots\t~p\t\t\t\t\t\t~n", [
        maps:get(max_sstslots, Opts, default)
    ]),
    ok = io:format(File, "leveled\tmax_mergebelow\t~p\t\t\t\t\t\t~n", [
        maps:get(max_mergebelow, Opts, default)
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
    ok = io:format(File, "leveled\trank_mode\t~p\t\t\t\t\t\t~n", [
        maps:get(rank, Opts, none)
    ]),
    ok = io:format(File, "leveled\tuncached\t~p\t\t\t\t\t\t~n", [
        maps:get(uncached, Opts, false)
    ]),
    ok = io:format(File, "leveled\tbust_mode\t~p\t\t\t\t\t\t~n", [
        maps:get(bust_mode, Opts, none)
    ]),
    ok = io:format(File, "leveled\tquery_order_contract\t~s\t\t\t\t\t\t~n", [
        escape_cell(query_order_contract(maps:get(rank, Opts, none)))
    ]),
    ok = io:format(File, "leveled\tsearch_opts\t~s\t\t\t\t\t\t~n", [
        escape_cell(list_to_binary(io_lib:format("~p", [
            search_opts(maps:get(limit, Opts), Opts)
        ])))
    ]),
    ok = append_query_rows(File, QueryRowsPath),
    ok = file:close(File),
    _ = file:delete(QueryRowsPath),
    ok.

write_memory_metrics(File, LoadStats) ->
    lists:foreach(
        fun({Metric, SnapshotKey}) ->
            Snapshot = maps:get(SnapshotKey, LoadStats, #{}),
            ok = io:format(File, "leveled\t~s_total\t~p\t\t\t\t\t\t~n", [
                atom_to_list(Metric),
                maps:get(total, Snapshot, 0)
            ]),
            ok = io:format(File, "leveled\t~s_processes\t~p\t\t\t\t\t\t~n", [
                atom_to_list(Metric),
                maps:get(processes, Snapshot, 0)
            ]),
            ok = io:format(File, "leveled\t~s_processes_used\t~p\t\t\t\t\t\t~n", [
                atom_to_list(Metric),
                maps:get(processes_used, Snapshot, 0)
            ]),
            ok = io:format(File, "leveled\t~s_system\t~p\t\t\t\t\t\t~n", [
                atom_to_list(Metric),
                maps:get(system, Snapshot, 0)
            ]),
            ok = io:format(File, "leveled\t~s_binary\t~p\t\t\t\t\t\t~n", [
                atom_to_list(Metric),
                maps:get(binary, Snapshot, 0)
            ])
        end,
        [
            {memory_after_load, memory_after_load},
            {memory_after_load_close, memory_after_load_close},
            {memory_after_queries, memory_after_queries},
            {memory_after_query_close, memory_after_query_close}
        ]
    ),
    ok = io:format(File, "leveled\tquery_memory_peak_total\t~p\t\t\t\t\t\t~n", [
        maps:get(query_memory_peak_total, LoadStats, 0)
    ]).

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

sst_start_opts(Opts) ->
    MaxSSTSlots = maps:get(max_sstslots, Opts, default),
    MaxMergeBelow = maps:get(max_mergebelow, Opts, default),
    lists:append([
        case MaxSSTSlots of
            default -> [];
            _ -> [{max_sstslots, MaxSSTSlots}]
        end,
        case MaxMergeBelow of
            default -> [];
            _ -> [{max_mergebelow, MaxMergeBelow}]
        end
    ]).

write_query_result(
    File,
    #{query := Query, count := Count, total_count := TotalCount, full_result_us := FullResultUs,
        runs_us := Runs, error := Error, keys := Keys, full_keys_sha256 := FullKeysSha256} = Result
) ->
    RunsBin = join_integer_list(Runs),
    KeysCell = json_binary_list(Keys),
    ok = io:format(
        File,
        "leveled\tfull_result_us\t~p\t~s\t\t~p\t\t~s\t~n",
        [FullResultUs, escape_cell(Query), TotalCount, term_cell(Error)]
    ),
    ok = io:format(
        File,
        "leveled\tfull_keys_sha256\t~s\t~s\t\t~p\t\t~s\t~n",
        [escape_cell(FullKeysSha256), escape_cell(Query), TotalCount, term_cell(Error)]
    ),
    ok = io:format(
        File,
        "leveled\tquery_us\t\t~s\t~p\t~p\t~s\t~s\t~s~n",
        [escape_cell(Query), Count, TotalCount, RunsBin, term_cell(Error), escape_cell(KeysCell)]
    ),
    case maps:get(rank_mode, Result, none) of
        bm25 ->
            ScoresCell = json_score_list(maps:get(scores, Result, [])),
            ok = io:format(
                File,
                "leveled\tquery_scores\t\t~s\t~p\t~p\t\t~s\t~s~n",
                [escape_cell(Query), Count, TotalCount, term_cell(Error), escape_cell(ScoresCell)]
            );
        _ ->
            ok
    end.

json_score_list(Scores) ->
    iolist_to_binary([$[, lists:join($,, [score_to_binary(S) || S <- Scores]), $]]).

score_to_binary(S) when is_integer(S) ->
    float_to_binary(float(S), [{scientific, 16}]);
score_to_binary(S) when is_float(S) ->
    float_to_binary(S, [{scientific, 16}]).

term_cell(Term) ->
    escape_cell(iolist_to_binary(io_lib:format("~w", [Term]))).

append_query_rows(OutFile, QueryRowsPath) ->
    {ok, InFile} = file:open(QueryRowsPath, [read, binary]),
    try append_query_rows_loop(InFile, OutFile) after ok = file:close(InFile) end.

append_query_rows_loop(InFile, OutFile) ->
    case file:read(InFile, 1024 * 1024) of
        eof ->
            ok;
        {ok, Bin} ->
            ok = file:write(OutFile, Bin),
            append_query_rows_loop(InFile, OutFile)
    end.

prefix_execution_contract() ->
    <<"secondary_index_token_range_scan">>.

payload_visibility_contract() ->
    <<"no_hidden_objects_payload_in_index_metadata">>.

rank_none_snapshot_contract() ->
    <<"rank_none_payload_key_order">>.

query_order_contract(bm25) ->
    <<"bm25_order_by_rank_key">>;
query_order_contract(_) ->
    <<"rank_none_order_by_key">>.

query_list_sha256(Queries) ->
    sha256_hex(iolist_to_binary([[Query, <<"\n">>] || Query <- Queries])).

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

keys_sha256(Hits) ->
    Keys = [maps:get(key, Hit) || Hit <- Hits],
    sha256_hex(json_binary_list(Keys)).

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
    directory_size(Path, all).

directory_size(Path, Mode) ->
    case filelib:is_dir(Path) of
        true -> directory_size([Path], Mode, 0);
        false -> 0
    end.

directory_size([], _Mode, Acc) ->
    Acc;
directory_size([Path | Rest], Mode, Acc) ->
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
                                case count_store_file(Name, Mode) of
                                    true -> {DirAcc, SizeAcc + Bytes};
                                    false -> {DirAcc, SizeAcc}
                                end;
                            {error, _Reason} ->
                                {DirAcc, SizeAcc}
                        end
                    end,
                    {[], Acc},
                    Names
                ),
            directory_size(Dirs ++ Rest, Mode, Size);
        {error, _Reason} ->
            directory_size(Rest, Mode, Acc)
    end.

count_store_file(_Name, all) ->
    true;
count_store_file(Name, live) ->
    filename:extension(Name) =/= ".bak".
