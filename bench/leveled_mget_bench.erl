%% Doc-retrieval benchmark: book_get loops vs book_mget batches.
%%
%% Loads a TSV corpus (key \t base64(title) \t base64(body), the
%% leveled_fts_bench corpus format) into a plain store - no FTS index -
%% then measures aggregate retrieval throughput for W concurrent
%% workers, each fetching scattered keys in batches:
%%
%%   get:  one book_get/4 call per key (every value serialises through
%%         the singleton inker)
%%   mget: one book_mget/4 call per batch (one inker grouping call per
%%         batch; journal reads and decodes fan out in the caller)
%%
%% The same seeded key sample is used for both modes at each worker
%% count. Typical use:
%%
%%   erl -noshell -pa <leveled ebin> -pa <bench ebin> \
%%       -run leveled_mget_bench main --tsv /tmp/leveled_fts_bench/docs.tsv \
%%       --root /tmp/leveled_mget_bench/store --workers 1,8 -run init stop
%%
%% Pass --skip-load to reuse a previously loaded store.
-module(leveled_mget_bench).

-include("leveled.hrl").

-export([main/1]).

main(Args) ->
    case parse_args(Args) of
        {ok, Opts} ->
            run(Opts);
        {error, Reason} ->
            io:format(standard_error, "leveled_mget_bench: ~p~n", [Reason]),
            {error, Reason}
    end.

parse_args(Args) ->
    Defaults = #{
        bucket => <<"bench">>,
        bytes => infinity,
        batch_size => 50,
        batches => 250,
        max_journalsize => 1000000000,
        max_journalobjectcount => 200000,
        skip_load => false,
        workers => [1, 8]
    },
    parse_args(Args, Defaults).

parse_args([], Opts) ->
    case [K || K <- [tsv, root], not maps:is_key(K, Opts)] of
        [] -> {ok, Opts};
        Missing -> {error, {missing_args, Missing}}
    end;
parse_args(["--tsv", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{tsv => Path});
parse_args(["--root", Path | Rest], Opts) ->
    parse_args(Rest, Opts#{root => Path});
parse_args(["--bytes", N | Rest], Opts) ->
    parse_args(Rest, Opts#{bytes => list_to_integer(N)});
parse_args(["--batch-size", N | Rest], Opts) ->
    parse_args(Rest, Opts#{batch_size => list_to_integer(N)});
parse_args(["--batches", N | Rest], Opts) ->
    parse_args(Rest, Opts#{batches => list_to_integer(N)});
parse_args(["--max-journalsize", N | Rest], Opts) ->
    parse_args(Rest, Opts#{max_journalsize => list_to_integer(N)});
parse_args(["--workers", Spec | Rest], Opts) ->
    Workers = [list_to_integer(W) || W <- string:tokens(Spec, ",")],
    parse_args(Rest, Opts#{workers => Workers});
parse_args(["--skip-load" | Rest], Opts) ->
    parse_args(Rest, Opts#{skip_load => true});
parse_args([Arg | _Rest], _Opts) ->
    {error, {unknown_arg, Arg}}.

run(Opts) ->
    #{root := Root, skip_load := SkipLoad} = Opts,
    ok =
        case SkipLoad of
            true -> ok;
            false -> load(Opts)
        end,
    Keys = read_keyfile(Root),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(Opts)),
    io:format(
        "store ~s: ~b docs, ~b journal files~n",
        [Root, length(Keys), journal_count(Root)]
    ),
    Results =
        [
            measure(Bookie, Workers, Keys, Opts)
         || Workers <- maps:get(workers, Opts)
        ],
    ok = leveled_bookie:book_close(Bookie),
    report(Results, Opts).

start_opts(Opts) ->
    [
        {root_path, maps:get(root, Opts)},
        {max_journalsize, maps:get(max_journalsize, Opts)},
        {max_journalobjectcount, maps:get(max_journalobjectcount, Opts)},
        {log_level, error},
        {compression_point, on_receipt}
    ].

%% Load the corpus and record the key list alongside the store, so
%% --skip-load runs can rebuild the sample without a fold.
load(Opts) ->
    #{tsv := Tsv, root := Root, bucket := Bucket, bytes := Budget} = Opts,
    os:cmd("rm -rf '" ++ Root ++ "'"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(Opts)),
    {ok, File} = file:open(Tsv, [read, raw, binary, {read_ahead, 1048576}]),
    T0 = erlang:monotonic_time(millisecond),
    {Count, Keys} =
        try
            load_loop(File, Bookie, Bucket, Budget, 0, 0, [])
        after
            ok = file:close(File)
        end,
    ok = leveled_bookie:book_close(Bookie),
    ok = write_keyfile(Root, lists:reverse(Keys)),
    io:format(
        "loaded ~b docs in ~.1f s~n",
        [Count, (erlang:monotonic_time(millisecond) - T0) / 1000]
    ),
    ok.

load_loop(File, Bookie, Bucket, Budget, Bytes, Count, Keys) ->
    case file:read_line(File) of
        eof ->
            {Count, Keys};
        {ok, Line} ->
            case binary:split(trim_newline(Line), <<"\t">>, [global]) of
                [Key, Title64, Body64] ->
                    Title = base64:decode(Title64),
                    Body = base64:decode(Body64),
                    Size = byte_size(Title) + byte_size(Body),
                    case Budget /= infinity andalso Bytes + Size > Budget of
                        true ->
                            {Count, Keys};
                        false ->
                            ok = leveled_bookie:book_put(
                                Bookie, Bucket, Key, {Title, Body}, []
                            ),
                            load_loop(
                                File,
                                Bookie,
                                Bucket,
                                Budget,
                                Bytes + Size,
                                Count + 1,
                                [Key | Keys]
                            )
                    end;
                _Other ->
                    load_loop(File, Bookie, Bucket, Budget, Bytes, Count, Keys)
            end
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

write_keyfile(Root, Keys) ->
    file:write_file(
        filename:join(Root, "mget_bench_keys.bin"),
        term_to_binary(Keys)
    ).

read_keyfile(Root) ->
    {ok, Bin} = file:read_file(filename:join(Root, "mget_bench_keys.bin")),
    binary_to_term(Bin).

journal_count(Root) ->
    length(
        filelib:wildcard(
            filename:join([Root, "journal", "journal_files", "*.cdb"])
        )
    ).

%% One measurement: for a worker count, sample per-worker batches once
%% (seeded, so re-runs and both modes see the same keys), warm the page
%% cache with an untimed pass, then time each mode.
measure(Bookie, Workers, Keys, Opts) ->
    #{
        bucket := Bucket,
        batches := Batches,
        batch_size := BatchSize
    } = Opts,
    KeyTuple = list_to_tuple(Keys),
    Plans =
        [
            sample_batches(
                rand:seed_s(exsss, {17, 29 * W, 43 + Workers}),
                KeyTuple,
                Batches,
                BatchSize
            )
         || W <- lists:seq(1, Workers)
        ],
    %% untimed warmup pass (page cache, cdb hash caches)
    _ = run_mode(mget, Bookie, Bucket, Plans),
    GetMicros = run_mode(get, Bookie, Bucket, Plans),
    MgetMicros = run_mode(mget, Bookie, Bucket, Plans),
    Values = Workers * Batches * BatchSize,
    {Workers, Values, GetMicros, MgetMicros}.

sample_batches(Seed, KeyTuple, Batches, BatchSize) ->
    N = tuple_size(KeyTuple),
    {Plan, _Seed} =
        lists:mapfoldl(
            fun(_B, SAcc0) ->
                lists:mapfoldl(
                    fun(_I, SAcc1) ->
                        {X, SAcc2} = rand:uniform_s(N, SAcc1),
                        {element(X, KeyTuple), SAcc2}
                    end,
                    SAcc0,
                    lists:seq(1, BatchSize)
                )
            end,
            Seed,
            lists:seq(1, Batches)
        ),
    Plan.

run_mode(Mode, Bookie, Bucket, Plans) ->
    Parent = self(),
    T0 = erlang:monotonic_time(microsecond),
    Pids =
        [
            spawn_link(
                fun() ->
                    ok = run_batches(Mode, Bookie, Bucket, Plan),
                    Parent ! {done, self()}
                end
            )
         || Plan <- Plans
        ],
    lists:foreach(
        fun(Pid) ->
            receive
                {done, Pid} -> ok
            end
        end,
        Pids
    ),
    erlang:monotonic_time(microsecond) - T0.

run_batches(_Mode, _Bookie, _Bucket, []) ->
    ok;
run_batches(get, Bookie, Bucket, [Batch | Rest]) ->
    lists:foreach(
        fun(Key) ->
            {ok, _Object} = leveled_bookie:book_get(Bookie, Bucket, Key)
        end,
        Batch
    ),
    run_batches(get, Bookie, Bucket, Rest);
run_batches(mget, Bookie, Bucket, [Batch | Rest]) ->
    Results = leveled_bookie:book_mget(Bookie, Bucket, Batch),
    true = length(Results) == length(Batch),
    lists:foreach(fun({_Key, {ok, _Object}}) -> ok end, Results),
    run_batches(mget, Bookie, Bucket, Rest).

report(Results, Opts) ->
    #{batches := Batches, batch_size := BatchSize} = Opts,
    io:format(
        "~nbatches=~b batch_size=~b~n"
        "workers  values     get_s    get_vps   mget_s   mget_vps  speedup~n",
        [Batches, BatchSize]
    ),
    lists:foreach(
        fun({Workers, Values, GetUs, MgetUs}) ->
            io:format(
                "~7b  ~9b  ~7.2f  ~9b  ~7.2f  ~9b  ~6.2fx~n",
                [
                    Workers,
                    Values,
                    GetUs / 1.0e6,
                    round(Values / (GetUs / 1.0e6)),
                    MgetUs / 1.0e6,
                    round(Values / (MgetUs / 1.0e6)),
                    GetUs / MgetUs
                ]
            )
        end,
        Results
    ),
    ok.
