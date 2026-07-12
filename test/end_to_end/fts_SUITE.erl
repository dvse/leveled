-module(fts_SUITE).

-include("leveled.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    search_shapes/1,
    update_delete_visibility/1,
    cache_admission/1,
    cross_shard_version_skew/1,
    consolidation_restart_equivalence/1,
    consolidation_write_race/1,
    sqlite_oracle_corpus/1
]).

all() ->
    [
        search_shapes,
        update_delete_visibility,
        cache_admission,
        cross_shard_version_skew,
        consolidation_restart_equivalence,
        consolidation_write_race,
        sqlite_oracle_corpus
    ].

init_per_suite(Config) ->
    testutil:init_per_suite([{suite, "fts"} | Config]),
    Config.

end_per_suite(Config) ->
    testutil:end_per_suite(Config).

search_shapes(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"shapes">>, [title, body], #{}),
        ok = put_doc(Bookie, Schema, <<"1">>,
            #{title => <<"Alpha">>, body => <<"quick brown fox">>}),
        ok = put_doc(Bookie, Schema, <<"2">>,
            #{title => <<"Beta">>, body => <<"quick blue hare">>}),
        ok = put_doc(Bookie, Schema, <<"3">>,
            #{title => <<"Gamma">>, body => <<"slow brown bear">>}),
        [<<"1">>, <<"2">>] = keys(search(Bookie, Schema, <<"quick">>, #{})),
        [<<"1">>] = keys(search(Bookie, Schema, <<"quick AND fox">>, #{})),
        [<<"1">>, <<"2">>, <<"3">>] =
            keys(search(Bookie, Schema, <<"quick OR bear">>, #{})),
        [<<"1">>] = keys(search(Bookie, Schema, <<"quick NOT blue">>, #{})),
        [<<"1">>] = keys(search(Bookie, Schema, <<"\"quick brown\"">>, #{})),
        [<<"1">>, <<"2">>] = keys(search(Bookie, Schema, <<"qui*">>, #{})),
        [<<"1">>] = keys(search(Bookie, Schema, <<"NEAR(quick fox, 2)">>, #{})),
        [<<"2">>] = keys(search(Bookie, Schema, <<"title:beta">>, #{})),
        [] = keys(search(Bookie, Schema, <<"title:quick">>, #{})),
        [Hit] = search(Bookie, Schema, <<"quick">>, #{return_positions => true,
            limit => 1}),
        true = maps:is_key(positions, Hit)
    end).

update_delete_visibility(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"updates">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"1">>, #{body => <<"red green">>}),
        {ok, Manifest1} = leveled_bookie:book_headonly(
            Bookie, <<"updates">>, <<"doc">>, <<"1">>
        ),
        {ok, UpdateSpecs} = leveled_fts:update(
            Schema, <<"1">>, #{body => <<"blue green">>}, Manifest1
        ),
        ok = leveled_bookie:book_mput(Bookie, UpdateSpecs),
        [] = keys(search(Bookie, Schema, <<"red">>, #{})),
        [<<"1">>] = keys(search(Bookie, Schema, <<"blue">>, #{})),
        {ok, Manifest2} = leveled_bookie:book_headonly(
            Bookie, <<"updates">>, <<"doc">>, <<"1">>
        ),
        ok = leveled_bookie:book_mput(
            Bookie, leveled_fts:remove(Schema, <<"1">>, Manifest2)
        ),
        [] = keys(search(Bookie, Schema, <<"blue">>, #{})),
        [] = keys(search(Bookie, Schema, all_docs, #{}))
    end).

cache_admission(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"cache">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"seed">>, #{body => <<"common">>}),
        Gate = atomics:new(1, []),
        Hook = fun({_Shard, _Epoch, _State}) ->
            case atomics:exchange(Gate, 1, 1) of
                0 -> put_doc(Bookie, Schema, <<"racer">>, #{body => <<"common">>});
                1 -> ok
            end
        end,
        [<<"racer">>, <<"seed">>] = keys(search(Bookie, Schema, <<"common">>,
            #{cache_fill_hook => Hook})),
        %% The stale snapshot was discarded; the installed entry has the
        %% current epoch and serves the same complete result.
        [<<"racer">>, <<"seed">>] = keys(search(Bookie, Schema, <<"common">>, #{}))
    end).

cross_shard_version_skew(_Config) ->
    %% A multi-shard AND must never assemble two versions of one doc
    %% into a false match (the doc-version stamp, FTS.md §2). Setup:
    %% v1 contains only <<"aa">> (shard 97), v2 only <<"zz">> (shard
    %% 122). Shard 97 is served from a cache validated BEFORE the
    %% update; shard 122 fills after it. Without the version stamp the
    %% merge sees aa (v1) + zz (v2) and "aa AND zz" false-matches.
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"skew">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"1">>, #{body => <<"aa">>}),
        %% warm the aa-shard cache at v1
        [<<"1">>] = keys(search(Bookie, Schema, <<"aa">>, #{})),
        Gate = atomics:new(1, []),
        Hook = fun({_Shard, _Epoch, _State}) ->
            case atomics:exchange(Gate, 1, 1) of
                0 ->
                    {ok, Manifest} = leveled_bookie:book_headonly(
                        Bookie, <<"skew">>, <<"doc">>, <<"1">>
                    ),
                    {ok, Specs} = leveled_fts:update(
                        Schema, <<"1">>, #{body => <<"zz">>}, Manifest
                    ),
                    ok = leveled_bookie:book_mput(Bookie, Specs);
                1 ->
                    ok
            end
        end,
        [] = keys(search(Bookie, Schema, <<"aa AND zz">>,
            #{cache_fill_hook => Hook})),
        %% post-update state is v2 exactly: zz matches, aa does not
        [<<"1">>] = keys(search(Bookie, Schema, <<"zz">>, #{})),
        [] = keys(search(Bookie, Schema, <<"aa">>, #{}))
    end).

consolidation_restart_equivalence(_Config) ->
    Root = testutil:reset_filestructure(),
    Opts = start_opts(Root),
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    Schema = schema(<<"consolidate">>, [body], #{}),
    ok = put_doc(Bookie1, Schema, <<"1">>, #{body => <<"alpha alpha beta">>}),
    ok = put_doc(Bookie1, Schema, <<"2">>, #{body => <<"alpha beta beta">>}),
    Before = search(Bookie1, Schema, <<"alpha OR beta">>, #{rank => bm25,
        return_positions => true}),
    {ok, #{skipped := []}} = leveled_fts:consolidate(Bookie1, Schema, #{}),
    Before = search(Bookie1, Schema, <<"alpha OR beta">>, #{rank => bm25,
        return_positions => true}),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    Before = search(Bookie2, Schema, <<"alpha OR beta">>, #{rank => bm25,
        return_positions => true}),
    ok = leveled_bookie:book_destroy(Bookie2).

consolidation_write_race(_Config) ->
    with_bookie(fun(Bookie, _Root) ->
        Schema = schema(<<"race">>, [body], #{}),
        ok = put_doc(Bookie, Schema, <<"seed">>, #{body => <<"alpha">>}),
        Shard = hd([S || {add, <<"race">>, <<S:16>>, <<"d:seed">>, _} <-
            element(2, leveled_fts:derive(Schema, <<"seed">>, #{body => <<"alpha">>}))]),
        Gate = atomics:new(1, []),
        Hook = fun({_S, _SQN}) ->
            case atomics:exchange(Gate, 1, 1) of
                0 -> put_doc(Bookie, Schema, <<"late">>, #{body => <<"apple">>});
                1 -> ok
            end
        end,
        {ok, #{skipped := [Shard], consolidated := []}} =
            leveled_fts:consolidate(Bookie, Schema,
                #{shards => [Shard], before_consolidate_commit => Hook}),
        [<<"late">>, <<"seed">>] = keys(search(Bookie, Schema, <<"alpha OR apple">>, #{}))
    end).

sqlite_oracle_corpus(_Config) ->
    LibDir = code:lib_dir(leveled),
    Oracle = filename:absname(filename:join(
        [LibDir, "..", "..", "..", "..", "test", "fts_sqlite_oracle_corpus.eterm"]
    )),
    {ok, [Cases]} = file:consult(Oracle),
    with_bookie(fun(Bookie, _Root) ->
        lists:foreach(fun(Case) -> oracle_case(Bookie, Case) end, Cases)
    end).

oracle_case(Bookie, #{id := Id, tokenizer_opts := TokOpts,
        doc := Doc, queries := Queries}) ->
    Index = atom_to_binary(Id, utf8),
    Schema = schema(Index, [body], TokOpts),
    ok = put_doc(Bookie, Schema, <<"doc">>, #{body => Doc}),
    lists:foreach(fun(#{q := Query, match := Expected}) ->
        Quoted = <<"\"", Query/binary, "\"">>,
        Actual = search(Bookie, Schema, Quoted, #{}) =/= [],
        case Actual =:= Expected of
            true -> ok;
            false -> ct:fail({sqlite_oracle_mismatch, Id, Query, Expected, Actual})
        end
    end, Queries).

schema(Index, Columns, Opts) ->
    {ok, Schema} = leveled_fts:schema(Opts#{index => Index, columns => Columns}),
    Schema.

put_doc(Bookie, Schema, Key, Object) ->
    {ok, Specs} = leveled_fts:derive(Schema, Key, Object),
    leveled_bookie:book_mput(Bookie, Specs).

search(Bookie, Schema, Query, Opts) ->
    {ok, Hits} = leveled_fts:search(Bookie, Schema, Query, Opts),
    Hits.

keys(Hits) -> [maps:get(key, Hit) || Hit <- Hits].

with_bookie(Fun) ->
    Root = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(Root)),
    try Fun(Bookie, Root)
    after
        try leveled_bookie:book_destroy(Bookie)
        catch _:_ -> ok
        end
    end.

start_opts(Root) ->
    [
        {root_path, Root},
        {sync_strategy, testutil:sync_strategy()},
        {compression_method, none},
        {ledger_compression, none},
        {log_level, warning}
    ].
