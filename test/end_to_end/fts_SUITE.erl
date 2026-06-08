-module(fts_SUITE).

-include("leveled.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    single_object_contract/1,
    batchput_contract/1,
    multi_token_phrase_contract/1,
    index_update_contract/1,
    concurrent_generation_contract/1,
    anchor_update_column_negative_contract/1,
    rejected_fast_path_regression_contract/1,
    segment_representation_contract/1,
    hot_term_segment_split_contract/1,
    segment_rank_none_limit_order_contract/1,
    segment_limited_pair_update_reopen_contract/1,
    invalid_write_inputs/1,
    private_snapshot_contract/1,
    regular_index_snapshot_contract/1,
    payload_index_contract/1,
    recovery_and_hotbackup/1,
    partial_tail_recovery_contract/1,
    recalc_reload_contract/1,
    sqlite_supported_ast_differential_contract/1,
    unicode61_supported_parity_corpus_contract/1,
    parse_errors/1
]).

all() ->
    [
        single_object_contract,
        batchput_contract,
        multi_token_phrase_contract,
        index_update_contract,
        concurrent_generation_contract,
        anchor_update_column_negative_contract,
        rejected_fast_path_regression_contract,
        segment_representation_contract,
        hot_term_segment_split_contract,
        segment_rank_none_limit_order_contract,
        segment_limited_pair_update_reopen_contract,
        invalid_write_inputs,
        private_snapshot_contract,
        regular_index_snapshot_contract,
        payload_index_contract,
        recovery_and_hotbackup,
        partial_tail_recovery_contract,
        recalc_reload_contract,
        sqlite_supported_ast_differential_contract,
        unicode61_supported_parity_corpus_contract,
        parse_errors
    ].

init_per_suite(Config) ->
    testutil:init_per_suite([{suite, "fts"} | Config]),
    Config.

end_per_suite(Config) ->
    testutil:end_per_suite(Config).

single_object_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"docs">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"quick brown fox quick">>, title => <<"Alpha">>},
            #{
                prefixes => [3],
                index_specs => [{add, <<"kind_bin">>, <<"guide">>}]
            }
        ),
    [<<"1">>] =
        segment_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"quick">>, <<"body">>),
    [<<"1">>] =
        segment_search_prefix_keys(
            Bookie, <<"docs">>, <<"main">>, 3, <<"qui">>, <<"quick">>, <<"body">>
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"docs">>,
            <<"2">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"quick blue hare">>, title => <<"Beta">>},
            #{prefixes => [3]}
        ),
    {error, {invalid_fts_contract_change, columns, [<<"body">>, <<"title">>], [<<"body">>]}} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"docs">>,
            <<"schema-column-drift">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"schema drift">>},
            #{columns => [body], prefixes => [3]}
        ),
    {error, {invalid_fts_contract_change, prefixes, [3], [4]}} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"docs">>,
            <<"schema-prefix-drift">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"schema drift">>, title => <<"Gamma">>},
            #{columns => [body, title], prefixes => [4]}
        ),
    {error, {invalid_fts_contract_change, tokenizer, _, _}} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"docs">>,
            <<"schema-tokenizer-drift">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"schema drift">>, title => <<"Gamma">>},
            #{columns => [body, title], prefixes => [3], remove_diacritics => 2}
        ),

    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"\"quick brown\"">>, #{})),
    [<<"1">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"qui* AND fox">>, #{
            prefixes => [3]
        })),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"brow*">>, #{})),
    SearchSchemaOpts = #{columns => [title, body]},
    {async, TitleNoOpts} =
        leveled_bookie:book_ftssearch(Bookie, <<"docs">>, <<"main">>, <<"title:beta">>, #{}),
    {ok, TitleNoOptsHits} = TitleNoOpts(),
    [<<"2">>] = keys(TitleNoOptsHits),
    [<<"2">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"title:beta">>, SearchSchemaOpts)),
    [] =
        keys(
            search(
                Bookie,
                <<"docs">>,
                <<"main">>,
                <<"title:(beta body:quick)">>,
                SearchSchemaOpts
            )
        ),
    [<<"1">>, <<"2">>] =
        keys(
            search(Bookie, <<"docs">>, <<"main">>, <<"{title body}:quick">>, SearchSchemaOpts)
        ),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"-title:quick">>, SearchSchemaOpts)),
    [] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"-body:quick">>, SearchSchemaOpts)),
    [<<"1">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"-{body}:alpha">>, SearchSchemaOpts)),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"quick NOT blue">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"NEAR(quick fox, 2)">>, #{})),
    {async, BM25Runner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{columns => [body], rank => bm25}
        ),
    {error, invalid_rank_option} = BM25Runner(),
    [<<"1">>, <<"2">>] =
        hit_keys(search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{rank => none})),
    [<<"2">>] =
        hit_keys(
            search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{
                rank => none,
                limit => 1,
                offset => 1
            })
        ),
    {async, StatsRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"docs">>,
            <<"main">>,
            <<"quick">>,
            #{columns => [body], stats => supplied, doc_count => 2, avgdl => 3.0}
        ),
    {error, invalid_fts_options} = StatsRunner(),
    [<<"1">>] = index_keys(Bookie, <<"docs">>, <<"kind_bin">>, <<"guide">>),

    [PositionHit | _] =
        search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{return_positions => true}),
    #{positions := Positions} = PositionHit,
    true = map_size(Positions) > 0,

    [RankNonePositionHit | _] =
        search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{
            rank => none,
            return_positions => true
        }),
    #{positions := RankNonePositions} = RankNonePositionHit,
    true = map_size(RankNonePositions) > 0,

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"docs">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"slow brown dog">>},
            #{
                columns => [body, title],
                prefixes => [3],
                index_specs => [
                    {remove, <<"kind_bin">>, <<"guide">>},
                    {add, <<"kind_bin">>, <<"article">>}
                ]
            }
        ),
    [<<"2">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"slow">>, #{})),
    [] = segment_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"fox">>, <<"body">>),
    [] =
        segment_search_prefix_keys(
            Bookie, <<"docs">>, <<"main">>, 3, <<"fox">>, <<"fox">>, <<"body">>
        ),
    [<<"1">>] =
        segment_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"slow">>, <<"body">>),
    [<<"1">>] =
        segment_search_prefix_keys(
            Bookie, <<"docs">>, <<"main">>, 3, <<"slo">>, <<"slow">>, <<"body">>
        ),
    [] = index_keys(Bookie, <<"docs">>, <<"kind_bin">>, <<"guide">>),
    [<<"1">>] = index_keys(Bookie, <<"docs">>, <<"kind_bin">>, <<"article">>),
    {ok, <<"obj1b">>} = leveled_bookie:book_get(Bookie, <<"docs">>, <<"1">>),

    ok = leveled_bookie:book_ftsdelete(Bookie, <<"docs">>, <<"2">>, <<"main">>, #{}),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, all_docs, #{})),
    [] = segment_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"quick">>, <<"body">>),
    not_found = leveled_bookie:book_get(Bookie, <<"docs">>, <<"2">>),

    ok = leveled_bookie:book_close(Bookie).

batchput_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"1">>, <<"obj1">>, <<"main">>,
                    #{body => <<"red apple">>}, #{prefixes => [3]}},
                {fts_put, <<"batch">>, <<"2">>, <<"obj2">>, <<"main">>,
                    #{body => <<"green apple">>},
                    #{
                        prefixes => [3],
                        index_specs => [{add, <<"batch_kind_bin">>, <<"fruit">>}]
                    }}
            ],
            true
        ),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"apple">>, #{})),
    [<<"2">>] =
        segment_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"green">>, <<"body">>),
    [<<"2">>] =
        segment_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"gre">>, <<"green">>, <<"body">>
        ),
    [<<"2">>] = index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"fruit">>),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"2">>, <<"obj2b">>, <<"main">>,
                    #{body => <<"yellow pear">>},
                    #{
                        prefixes => [3],
                        index_specs => [
                            {remove, <<"batch_kind_bin">>, <<"fruit">>},
                            {add, <<"batch_kind_bin">>, <<"dessert">>}
                        ]
                    }}
            ]
        ),
    [<<"1">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"apple">>, #{})),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"green">>, #{})),
    [] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"gre*">>, #{prefixes => [3]})),
    [] = segment_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"green">>, <<"body">>),
    [] =
        segment_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"gre">>, <<"green">>, <<"body">>
        ),
    [<<"2">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"pear">>, #{})),
    [<<"2">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"pea*">>, #{prefixes => [3]})),
    [<<"2">>] =
        segment_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"pea">>, <<"pear">>, <<"body">>
        ),
    [<<"2">>] =
        segment_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"pear">>, <<"body">>),
    [] = index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"fruit">>),
    [<<"2">>] =
        index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"dessert">>),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, all_docs, #{})),
    {ok, <<"obj2b">>} = leveled_bookie:book_get(Bookie, <<"batch">>, <<"2">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"batch">>,
            <<"idx">>,
            <<"idx-main">>,
            <<"main">>,
            #{body => <<"mainonly">>},
            #{}
        ),
    {error, {conflicting_fts_index, <<"main">>, <<"alt">>}} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"batch">>,
            <<"idx">>,
            <<"idx-alt">>,
            <<"alt">>,
            #{body => <<"altonly">>},
            #{}
        ),
    {error, missing_fts_schema} =
        leveled_bookie:book_ftsdelete(Bookie, <<"batch">>, <<"idx">>, <<"alt">>, #{}),
    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),
    assert_missing_fts_schema(Bookie, <<"batch">>, <<"alt">>, <<"altonly">>, #{}),
    {ok, <<"idx-main">>} = leveled_bookie:book_get(Bookie, <<"batch">>, <<"idx">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"batch">>,
            <<"idx-normalised">>,
            <<"idx-normalised-main">>,
            <<"main">>,
            #{body => <<"normalised old">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"batch">>,
            <<"idx-normalised">>,
            <<"idx-normalised-atom">>,
            main,
            #{body => <<"normalised new">>},
            #{}
        ),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"old">>, #{})),
    [<<"idx-normalised">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"new">>, #{})),
    ok =
        leveled_bookie:book_ftsdelete(
            Bookie, <<"batch">>, <<"idx-normalised">>, main, #{}
        ),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"new">>, #{})),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"batch">>,
            <<"idx-batch">>,
            <<"idx-batch-main">>,
            <<"main">>,
            #{body => <<"batchmainonly">>},
            #{}
        ),
    {error, {conflicting_fts_index, <<"main">>, <<"alt">>}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"idx-batch">>, <<"idx-batch-alt">>, <<"alt">>,
                    #{body => <<"batchaltonly">>}, #{}}
            ]
        ),
    {error, missing_fts_schema} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_delete, <<"batch">>, <<"idx-batch">>, <<"alt">>, #{}}
            ]
        ),
    [<<"idx-batch">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"batchmainonly">>, #{})),
    assert_missing_fts_schema(Bookie, <<"batch">>, <<"alt">>, <<"batchaltonly">>, #{}),
    {ok, <<"idx-batch-main">>} =
        leveled_bookie:book_get(Bookie, <<"batch">>, <<"idx-batch">>),

    {error, invalid_index_specs} =
        leveled_bookie:book_batchput(
            Bookie,
            [
                {put, <<"raw">>, <<"payload-spec">>, <<"obj">>,
                    [{add, <<"ordinary_idx">>, <<"term">>, <<"payload">>}],
                    ?STD_TAG, infinity}
            ]
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"raw">>, <<"payload-spec">>),
    RawPayloadSpec =
        {idx_payload, add, {fts_segment, <<"main">>}, <<"term">>, <<"payload">>},
    {error, invalid_index_specs} =
        leveled_bookie:book_put(
            Bookie,
            <<"raw">>,
            <<"idx-payload-direct">>,
            <<"obj">>,
            [RawPayloadSpec],
            ?STD_TAG
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"raw">>, <<"idx-payload-direct">>),
    {error, invalid_index_specs} =
        leveled_bookie:book_batchput(
            Bookie,
            [
                {put, <<"raw">>, <<"idx-payload-batch">>, <<"obj">>,
                    [RawPayloadSpec], ?STD_TAG, infinity}
            ]
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"raw">>, <<"idx-payload-batch">>),

    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {put, <<"batch">>, <<"idx">>, <<"raw-over-fts">>, [],
                    ?STD_TAG, infinity}
            ]
        ),
	    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),
	    {error, {invalid_batch_object_spec, {delete, _, _, _, _, _}}} =
	        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {delete, <<"batch">>, <<"idx">>, [], ?STD_TAG, infinity}
            ]
	        ),
	    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),

	    ok =
	        leveled_bookie:book_ftsput(
		            Bookie, <<"raw-contract">>, <<"raw-direct">>, <<"fts-object">>, <<"main">>,
	            #{body => <<"rawdirect">>}, #{}
	        ),
		    ok = leveled_bookie:book_put(Bookie, <<"raw-contract">>, <<"raw-direct">>, <<"raw-object">>, []),
		    {ok, <<"raw-object">>} = leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-direct">>),
		    ok = leveled_bookie:book_delete(Bookie, <<"raw-contract">>, <<"raw-direct">>, []),
		    not_found = leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-direct">>),
	    ok =
	        leveled_bookie:book_ftsput(
		            Bookie, <<"raw-contract">>, <<"raw-batch-put">>, <<"fts-object">>, <<"main">>,
	            #{body => <<"rawbatchput">>}, #{}
	        ),
	    ok =
	        leveled_bookie:book_batchput(
	            Bookie,
	            [
		                {put, <<"raw-contract">>, <<"raw-batch-put">>, <<"raw-normal-batch">>, [],
	                    ?STD_TAG, infinity}
	            ]
	        ),
	    {ok, <<"raw-normal-batch">>} =
	        leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-batch-put">>),
	    ok =
	        leveled_bookie:book_ftsput(
	            Bookie, <<"raw-contract">>, <<"raw-batch-delete">>, <<"fts-object">>, <<"main">>,
	            #{body => <<"rawbatchdelete">>}, #{}
	        ),
	    ok =
	        leveled_bookie:book_batchput(
	            Bookie,
	            [
	                {delete, <<"raw-contract">>, <<"raw-batch-delete">>, [], ?STD_TAG, infinity}
	            ]
	        ),
	    not_found = leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-batch-delete">>),
	    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),

	    {error, {duplicate_fts_batch_key, _}} =
	        leveled_bookie:book_ftsbatchput(
	            Bookie,
	            [
	                {fts_put, <<"batch">>, <<"3">>, <<"obj3">>, <<"main">>,
	                    #{body => <<"one">>}, #{}},
	                {fts_put, <<"batch">>, <<"3">>, <<"obj3">>, <<"main">>,
	                    #{body => <<"two">>}, #{}}
	            ]
	        ),
	    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"one">>, #{})),
	    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"two">>, #{})),
	    ok =
	        leveled_bookie:book_ftsbatchput(
	            Bookie,
	            [
	                {fts_put, <<"batch">>, <<"3">>, <<"obj3">>, <<"main">>,
	                    #{body => <<"two">>}, #{}}
	            ]
	        ),
	    [<<"3">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"two">>, #{})),

    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {put, <<"batch">>, <<"4">>, <<"raw">>, [], ?STD_TAG, infinity},
                {fts_put, <<"batch">>, <<"4">>, <<"obj4">>, <<"main">>,
                    #{body => <<"four">>}, #{}}
            ]
        ),

    {error, {duplicate_fts_batch_key, _}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"5">>, <<"obj5">>, <<"main">>,
                    #{body => <<"five">>}, #{}},
                {fts_put, <<"batch">>, <<"5">>, <<"obj5">>, <<"alt">>,
                    #{body => <<"five">>}, #{}}
            ]
        ),

    {error, {invalid_batch_object_spec, bogus}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"6">>, <<"obj6">>, <<"main">>,
                    #{body => <<"uniquetoken">>}, #{}},
                bogus
            ]
        ),
    [] = search(Bookie, <<"batch">>, <<"main">>, <<"uniquetoken">>, #{}),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [{fts_delete, <<"batch">>, <<"1">>, <<"main">>, #{}}]
        ),
    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {put, <<"batch">>, <<"1">>, <<"raw-over-fts-tombstone">>, [],
                    ?STD_TAG, infinity}
            ]
        ),
    [<<"2">>, <<"3">>, <<"idx">>, <<"idx-batch">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, all_docs, #{})),
    [] = segment_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"red">>, <<"body">>),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_delete, <<"batch">>, <<"2">>, <<"main">>,
                    #{index_specs => [{remove, <<"batch_kind_bin">>, <<"dessert">>}]}}
            ]
        ),
    [<<"3">>, <<"idx">>, <<"idx-batch">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, all_docs, #{})),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"pear">>, #{})),
    [] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"pea*">>, #{prefixes => [3]})),
    [] = segment_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"pear">>, <<"body">>),
    [] =
        segment_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"pea">>, <<"pear">>, <<"body">>
        ),
    [] = index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"dessert">>),
    [<<"1">>, <<"2">>, <<"3">>, <<"idx">>, <<"idx-batch">>, <<"idx-normalised">>] =
        index_keys(Bookie, <<"batch">>, {fts_doc, <<"main">>}, doc),
    not_found = leveled_bookie:book_get(Bookie, <<"batch">>, <<"2">>),

    ok = leveled_bookie:book_close(Bookie).

multi_token_phrase_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"phrase">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body], prefixes => [3]},
    SearchOpts = #{columns => [title, body], prefixes => [3], rank => none},

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, Bucket, <<"exact">>, <<"exact-object">>, Index,
                    #{
                        body => <<"alpha beta gamma delta repeat repeat gamma">>,
                        title => <<"alpha beta gamma">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"gap">>, <<"gap-object">>, Index,
                    #{
                        body => <<"alpha beta filler gamma delta">>,
                        title => <<"gamma beta alpha">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"column">>, <<"column-object">>, Index,
                    #{
                        body => <<"alpha beta miss">>,
                        title => <<"alpha beta gamma">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"repeat">>, <<"repeat-object">>, Index,
                    #{
                        body => <<"repeat repeat gamma">>,
                        title => <<"other">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"stale">>, <<"stale-object">>, Index,
                    #{
                        body => <<"alpha beta gamma stale">>,
                        title => <<"alpha beta gamma">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"delete">>, <<"delete-object">>, Index,
                    #{
                        body => <<"alpha beta gamma delete">>,
                        title => <<"other">>
                    },
                    WriteOpts}
            ]
        ),

    [<<"column">>, <<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gamma\"">>, SearchOpts)),
    [<<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"title:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta\" + gamma">>, SearchOpts)),
    [<<"exact">>, <<"repeat">>] =
        keys(search(Bookie, Bucket, Index, <<"\"repeat repeat gamma\"">>, SearchOpts)),
    [<<"column">>, <<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gam\"*">>, SearchOpts)),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"stale">>,
            <<"stale-object-new">>,
            Index,
            #{
                body => <<"alpha beta filler gamma stale">>,
                title => <<"stale title">>
            },
            WriteOpts
        ),
    ok =
        leveled_bookie:book_ftsdelete(
            Bookie,
            Bucket,
            <<"delete">>,
            Index,
            #{columns => [title, body]}
        ),

    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gamma\"">>, SearchOpts)),
    [<<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"title:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta\" + gamma">>, SearchOpts)),
    [<<"exact">>, <<"repeat">>] =
        keys(search(Bookie, Bucket, Index, <<"\"repeat repeat gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gam\"*">>, SearchOpts)),
    [] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma delete\"">>, SearchOpts)),
    [] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma stale\"">>, SearchOpts)),

    ok = leveled_bookie:book_close(Bookie).

index_update_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    SearchColumns = [title, body],
    BodyColumns = [body],
    SearchOpts = #{columns => SearchColumns},
    SegmentSearchOpts = #{columns => BodyColumns},

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"doc-old">>,
            <<"main">>,
            #{body => <<"alpha beta">>, title => <<"first">>},
            #{
                columns => SearchColumns,
                prefixes => [2, 3],
                index_specs => [{add, <<"kind_bin">>, <<"old">>}]
            }
        ),
    [<<"doc">>] =
        segment_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"alpha">>, <<"body">>),
    [<<"doc">>] =
        segment_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 2, <<"al">>, <<"alpha">>, <<"body">>
        ),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, {fts_doc, <<"main">>}, doc),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"old">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"doc-new">>,
            <<"main">>,
            #{body => <<"gamma beta">>, title => <<"second">>},
            #{
                columns => SearchColumns,
                prefixes => [2, 3],
                index_specs => [
                    {remove, <<"kind_bin">>, <<"old">>},
                    {add, <<"kind_bin">>, <<"new">>}
                ]
            }
        ),
    PrefixRankOpts = #{prefixes => [2, 3], rank => none, columns => BodyColumns},
    PrefixRankMixedOpts = #{prefixes => [2, 3], rank => none, columns => BodyColumns},
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"alpha">>, SearchOpts)),
    [<<"doc">>] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, SearchOpts)),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:al*">>, PrefixRankOpts)),
    [<<"doc">>] =
        keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankOpts)),
    [<<"doc">>] =
        keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankMixedOpts)),
    [] = segment_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"alpha">>, <<"body">>),
    [] =
        segment_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 2, <<"al">>, <<"alpha">>, <<"body">>
        ),
    [<<"doc">>] =
        segment_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, <<"body">>),
    [<<"doc">>] =
        segment_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"gam">>, <<"gamma">>, <<"body">>
        ),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, {fts_doc, <<"main">>}, doc),
    [] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"old">>),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"new">>),

    ok =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"main">>,
            #{columns => SearchColumns, index_specs => [{remove, <<"kind_bin">>, <<"new">>}]}
        ),
    [] = search(Bookie, <<"idx-maint">>, <<"main">>, all_docs, SearchOpts),
    [] = segment_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, <<"body">>),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankOpts)),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankMixedOpts)),
    [] =
        segment_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"gam">>, <<"gamma">>, <<"body">>
        ),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, {fts_doc, <<"main">>}, doc),
    [] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"new">>),
    not_found = leveled_bookie:book_get(Bookie, <<"idx-maint">>, <<"doc">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"doc-reinsert">>,
            <<"main">>,
            #{body => <<"delta beta">>, title => <<"third">>},
            #{
                columns => SearchColumns,
                prefixes => [2, 3],
                index_specs => [{add, <<"kind_bin">>, <<"again">>}]
            }
        ),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, SearchOpts)),
    [<<"doc">>] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"delta">>, SearchOpts)),
    [] = segment_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, <<"body">>),
    [] =
        segment_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"gam">>, <<"gamma">>, <<"body">>
        ),
    [<<"doc">>] =
        segment_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"delta">>, <<"body">>),
    [<<"doc">>] =
        segment_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"del">>, <<"delta">>, <<"body">>
        ),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, {fts_doc, <<"main">>}, doc),
    [] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"new">>),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"again">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint-segment">>,
            <<"seg">>,
            <<"seg-old">>,
            <<"main">>,
            #{body => <<"red apple">>},
            #{
                columns => BodyColumns,
                index_specs => [{add, <<"seg_kind_bin">>, <<"old">>}]
            }
        ),
    true =
        segment_term_present(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"red">>, <<"body">>
        ),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, {fts_doc, <<"main">>}, doc),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, <<"seg_kind_bin">>, <<"old">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint-segment">>,
            <<"seg">>,
            <<"seg-new">>,
            <<"main">>,
            #{body => <<"blue grape">>},
            #{
                columns => BodyColumns,
                index_specs => [
                    {remove, <<"seg_kind_bin">>, <<"old">>},
                    {add, <<"seg_kind_bin">>, <<"new">>}
                ]
            }
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"red">>, SegmentSearchOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"body:red">>, #{
                    rank => none,
                    columns => [body]
                }
            )
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue">>, SegmentSearchOpts
            )
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"body:blue">>, #{
                    rank => none,
                    columns => [body]
                }
            )
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue NOT red">>, #{
                    rank => none,
                    limit => 20,
                    columns => BodyColumns
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"red NOT blue">>, #{
                    rank => none,
                    limit => 20,
                    columns => BodyColumns
                }
            )
        ),
    true =
        segment_term_present(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue">>, <<"body">>
        ),
    true =
        segment_doc_delete_present(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"red">>, <<"body">>, <<"seg">>
        ),
    false =
        segment_doc_delete_present(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue">>, <<"body">>, <<"seg">>
        ),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, {fts_doc, <<"main">>}, doc),
    [] =
        index_keys(Bookie, <<"idx-maint-segment">>, <<"seg_kind_bin">>, <<"old">>),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, <<"seg_kind_bin">>, <<"new">>),

    ok =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"idx-maint-segment">>,
            <<"seg">>,
            <<"main">>,
            #{columns => BodyColumns, index_specs => [{remove, <<"seg_kind_bin">>, <<"new">>}]}
        ),
    [] =
        search(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue">>, SegmentSearchOpts
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue NOT red">>, #{
                    rank => none,
                    limit => 20,
                    columns => BodyColumns
                }
            )
        ),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, {fts_doc, <<"main">>}, doc),
    true =
        segment_doc_delete_present(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue">>, <<"body">>, <<"seg">>
        ),
    [] =
        index_keys(Bookie, <<"idx-maint-segment">>, <<"seg_kind_bin">>, <<"new">>),
    not_found = leveled_bookie:book_get(Bookie, <<"idx-maint-segment">>, <<"seg">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint-segment">>,
            <<"seg">>,
            <<"seg-alt">>,
            <<"alt">>,
            #{body => <<"alternate melon">>},
            #{columns => BodyColumns}
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"alt">>, <<"alternate">>, SegmentSearchOpts
            )
        ),
    ok =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"idx-maint-segment">>,
            <<"seg">>,
            <<"alt">>,
            #{columns => BodyColumns}
        ),
    [] =
        search(
            Bookie, <<"idx-maint-segment">>, <<"alt">>, <<"alternate">>, SegmentSearchOpts
        ),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"idx-maint-segment">>,
            <<"seg">>,
            <<"seg-fresh">>,
            <<"main">>,
            #{body => <<"fresh melon">>},
            #{
                columns => BodyColumns,
                index_specs => [{add, <<"seg_kind_bin">>, <<"fresh">>}]
            }
        ),
    [] =
        search(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"red">>, SegmentSearchOpts
        ),
    [] =
        search(
            Bookie, <<"idx-maint-segment">>, <<"main">>, <<"blue">>, SegmentSearchOpts
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-segment">>, <<"main">>, <<"fresh">>, SegmentSearchOpts
            )
        ),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, {fts_doc, <<"main">>}, doc),
    [] =
        index_keys(Bookie, <<"idx-maint-segment">>, <<"seg_kind_bin">>, <<"new">>),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-segment">>, <<"seg_kind_bin">>, <<"fresh">>),

    ok = leveled_bookie:book_close(Bookie).

concurrent_generation_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_concurrent_generation"),
    Bucket = <<"concurrent-generation">>,
    Index = <<"main">>,
    Key = <<"same-key">>,
    WriterCount = 20,
    Parent = self(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    [
        spawn(fun() ->
            Term = concurrent_generation_term(N),
            Result =
                leveled_bookie:book_ftsput(
                    Bookie,
                    Bucket,
                    Key,
                    Term,
                    Index,
                    #{body => <<Term/binary, " stable">>},
                    #{}
                ),
            Parent ! {concurrent_ftsput, N, Term, Result}
        end)
     || N <- lists:seq(1, WriterCount)
    ],
    Results = collect_concurrent_ftsputs(WriterCount, []),
    [] = [{N, Result} || {N, _Term, Result} <- Results, Result =/= ok],
    Terms = [Term || {_N, Term, ok} <- Results],
    {ok, FinalTerm} = leveled_bookie:book_get(Bookie, Bucket, Key),
    true = lists:member(FinalTerm, Terms),
    [Key] = keys(search(Bookie, Bucket, Index, FinalTerm, #{})),
    lists:foreach(
        fun(Term) ->
            [] = keys(search(Bookie, Bucket, Index, Term, #{}))
        end,
        Terms -- [FinalTerm]
    ),
    {active, WriterCount, false} = doc_marker_payload_state(Bookie, Bucket, Index, Key),
    ok = leveled_bookie:book_close(Bookie).

anchor_update_column_negative_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_anchor_update_column_negative"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"anchor">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body]},
    SearchOpts = #{columns => [title, body], rank => none},

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"doc-title-start">>,
            <<"obj-title-start">>,
            Index,
            #{title => <<"anchortitle first">>, body => <<"intro anchorbody">>},
            WriteOpts
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"doc-body-start">>,
            <<"obj-body-start">>,
            Index,
            #{title => <<"intro anchortitle">>, body => <<"anchorbody first">>},
            WriteOpts
        ),

    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"title:^anchortitle">>, SearchOpts)),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"^anchorbody">>, SearchOpts)),
    [] = keys(search(Bookie, Bucket, Index, <<"title:^anchorbody">>, SearchOpts)),
    [] = keys(search(Bookie, Bucket, Index, <<"body:^anchortitle">>, SearchOpts)),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"doc-title-start">>,
            <<"obj-title-moved">>,
            Index,
            #{title => <<"intro anchortitle">>, body => <<"anchorbody now">>},
            WriteOpts
        ),
    [] = keys(search(Bookie, Bucket, Index, <<"title:^anchortitle">>, SearchOpts)),
    [<<"doc-body-start">>, <<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    [] = keys(search(Bookie, Bucket, Index, <<"^anchortitle">>, SearchOpts)),

    ok =
        leveled_bookie:book_ftsdelete(
            Bookie, Bucket, <<"doc-body-start">>, Index, WriteOpts
        ),
    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    not_found = leveled_bookie:book_get(Bookie, Bucket, <<"doc-body-start">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"doc-body-start">>,
            <<"obj-title-reinserted">>,
            Index,
            #{title => <<"anchortitle rebound">>, body => <<"later anchorbody">>},
            WriteOpts
        ),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"title:^anchortitle">>, SearchOpts)),
    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"^anchortitle">>, SearchOpts)),
    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"^anchorbody">>, SearchOpts)),

    ok = leveled_bookie:book_close(Bookie).

rejected_fast_path_regression_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_rejected_fast_path_regression"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Index = <<"main">>,
    SearchOpts = #{columns => [title, body], prefixes => [5, 11], rank => none},

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"fast-anchor">>,
            <<"a1">>,
            <<"a1">>,
            Index,
            #{
                title => <<"equivtitleupdate">>,
                body => <<"equivupdate equivbodyupdate">>
            },
            #{prefixes => [5, 11]}
        ),
    [<<"a1">>] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"equivupdate">>, SearchOpts)),
    [<<"a1">>] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"^equivupdate">>, SearchOpts)),
    [<<"a1">>] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"title:^equivtitleupdate">>, SearchOpts)),
    [] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"^equivbodyupdate">>, SearchOpts)),

    PhraseOpts = #{columns => [body], prefixes => [5, 11], rank => none},
    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"fast-phrase-prefix">>, <<"p1">>, <<"p1">>,
                    Index, #{body => <<"equivnearone galaxy equivneartwo">>},
                    #{prefixes => [5, 11]}},
                {fts_put, <<"fast-phrase-prefix">>, <<"p2">>, <<"p2">>,
                    Index,
                    #{body => <<"equivnearone gap distant distant distant equivneartwo">>},
                    #{prefixes => [5, 11]}},
                {fts_put, <<"fast-phrase-prefix">>, <<"p3">>, <<"p3-old">>,
                    Index, #{body => <<"equivnearone gamma equivneartwo">>},
                    #{prefixes => [5, 11]}}
            ]
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"fast-phrase-prefix">>,
            <<"p3">>,
            <<"p3-new">>,
            Index,
            #{body => <<"equivnearone zeta equivneartwo">>},
            #{prefixes => [5, 11]}
        ),
    [<<"p1">>, <<"p2">>] =
        keys(
            search(
                Bookie,
                <<"fast-phrase-prefix">>,
                Index,
                <<"\"equivnearone ga\"*">>,
                PhraseOpts
            )
        ),
    [<<"p1">>] =
        keys(
            search(
                Bookie,
                <<"fast-phrase-prefix">>,
                Index,
                <<"NEAR(\"equivnearone ga\"* equivneartwo, 1)">>,
                PhraseOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"fast-phrase-prefix">>,
                Index,
                <<"\"equivnearone gam\"*">>,
                PhraseOpts
            )
        ),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"fast-near">>, <<"n1">>, <<"n1-old">>,
                    Index, #{body => <<"oldnearone gap oldneartwo">>}, #{}},
                {fts_put, <<"fast-near">>, <<"n2">>, <<"n2-old">>,
                    Index, #{body => <<"deletenearone gap deleteneartwo">>}, #{}}
            ]
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"fast-near">>,
            <<"n1">>,
            <<"n1-new">>,
            Index,
            #{body => <<"nearone gap neartwo">>},
            #{}
        ),
    ok = leveled_bookie:book_ftsdelete(Bookie, <<"fast-near">>, <<"n2">>, Index, #{}),
    [] =
        keys(
            search(
                Bookie,
                <<"fast-near">>,
                Index,
                <<"NEAR(oldnearone oldneartwo, 5)">>,
                #{rank => none}
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"fast-near">>,
                Index,
                <<"NEAR(deletenearone deleteneartwo, 5)">>,
                #{rank => none}
            )
        ),
    [<<"n1">>] =
        keys(search(Bookie, <<"fast-near">>, Index, <<"NEAR(nearone neartwo, 5)">>, #{rank => none})),

    ok = leveled_bookie:book_close(Bookie).

segment_representation_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"segment">>, <<"1">>, <<"obj1">>, <<"main">>,
                    #{body => <<"red apple oldnearone gap oldneartwo">>},
                    #{prefixes => [3]}},
                {fts_put, <<"segment">>, <<"2">>, <<"obj2">>, <<"main">>,
                    #{body => <<"green apple deletenearone gap deleteneartwo">>},
                    #{}}
            ]
        ),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"segment">>, <<"main">>, <<"apple">>, #{})),
    true =
        segment_term_present(
            Bookie, <<"segment">>, <<"main">>, <<"red">>, <<"body">>
        ),
    true =
        segment_term_present(
            Bookie, <<"segment">>, <<"main">>, <<"green">>, <<"body">>
        ),
    [<<"1">>, <<"2">>] = index_keys(Bookie, <<"segment">>, {fts_doc, <<"main">>}, doc),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"segment_a">>, <<"1">>, <<"obj-a1">>, <<"main">>,
                    #{body => <<"shared bucket token">>}, #{}},
                {fts_put, <<"segment_b">>, <<"1">>, <<"obj-b1">>, <<"main">>,
                    #{body => <<"shared bucket token">>}, #{}}
            ]
        ),
    [<<"1">>] = keys(search(Bookie, <<"segment_a">>, <<"main">>, <<"shared">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"segment_b">>, <<"main">>, <<"shared">>, #{})),
    true =
        segment_term_present(
            Bookie, <<"segment_a">>, <<"main">>, <<"shared">>, <<"body">>
        ),
    true =
        segment_term_present(
            Bookie, <<"segment_b">>, <<"main">>, <<"shared">>, <<"body">>
        ),

    HighKey = binary:copy(<<255>>, 33),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-high-key">>,
            HighKey,
            <<"obj-high">>,
            <<"main">>,
            #{body => <<"highmarker">>},
            #{}
        ),
    [HighKey] =
        keys(
            search(
                Bookie, <<"segment-high-key">>, <<"main">>, all_docs, #{}
            )
        ),
    [HighKey] =
        keys(
            search(
                Bookie, <<"segment-high-key">>, <<"main">>, <<"highmarker">>, #{}
            )
        ),

    Cafe = <<"caf", 195, 169>>,
    CafeBody = <<Cafe/binary, " oldtoken">>,
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-diacritic">>,
            <<"diacritic">>,
            <<"obj-diacritic-1">>,
            <<"main">>,
            #{body => CafeBody},
            #{remove_diacritics => false}
        ),
    [<<"diacritic">>] =
        keys(
            search(
                Bookie, <<"segment-diacritic">>, <<"main">>, Cafe, #{
                    rank => none,
                    remove_diacritics => false
                }
            )
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-diacritic">>,
            <<"diacritic">>,
            <<"obj-diacritic-2">>,
            <<"main">>,
            #{body => <<"plain replacement">>},
            #{remove_diacritics => false}
        ),
    [] =
        keys(
            search(
                Bookie, <<"segment-diacritic">>, <<"main">>, Cafe, #{
                    rank => none,
                    remove_diacritics => false
                }
            )
        ),
    [<<"diacritic">>] =
        keys(
            search(
                Bookie, <<"segment-diacritic">>, <<"main">>, <<"plain">>, #{
                    rank => none,
                    remove_diacritics => false
                }
            )
        ),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"blue grape nearone gap neartwo">>},
            #{}
        ),
    [<<"2">>] = keys(search(Bookie, <<"segment">>, <<"main">>, <<"apple">>, #{})),
    [#{key := <<"2">>, doc_length := 5}] =
        search(
            Bookie,
            <<"segment">>,
            <<"main">>,
            <<"apple AND green">>,
            #{rank => none}
        ),
    [<<"1">>] = keys(search(Bookie, <<"segment">>, <<"main">>, <<"grape">>, #{})),
    [<<"1">>] =
        keys(search(Bookie, <<"segment">>, <<"main">>, <<"gra*">>, #{prefixes => [3]})),
    [<<"1">>] =
        keys(
            search(
                Bookie,
                <<"segment">>,
                <<"main">>,
                <<"gra*">>,
                #{prefixes => [3], rank => none}
            )
        ),
    [<<"1">>] =
        keys(
            search(Bookie, <<"segment">>, <<"main">>, <<"body:grape">>, #{
                rank => none,
                columns => [body]
            })
        ),
    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"segment-cross-column-and">>, <<"1">>, <<"cross-1">>,
                    <<"main">>, #{title => <<"crossalpha">>, body => <<"crossbeta">>}, #{}},
                {fts_put, <<"segment-cross-column-and">>, <<"2">>, <<"cross-2">>,
                    <<"main">>, #{title => <<"crossalpha">>, body => <<"filler">>}, #{}},
                {fts_put, <<"segment-cross-column-and">>, <<"3">>, <<"cross-3">>,
                    <<"main">>, #{title => <<"filler">>, body => <<"crossbeta">>}, #{}},
                {fts_put, <<"segment-cross-column-and">>, <<"4">>, <<"cross-4">>,
                    <<"main">>, #{title => <<"crossbeta">>, body => <<"crossalpha">>}, #{}},
                {fts_put, <<"segment-cross-column-and">>, <<"5">>, <<"cross-5">>,
                    <<"main">>, #{body => <<"crossalpha crossbeta">>}, #{}}
            ]
        ),
    [<<"1">>, <<"4">>, <<"5">>] =
        keys(
            search(
                Bookie, <<"segment-cross-column-and">>, <<"main">>,
                <<"crossalpha AND crossbeta">>, #{rank => none, columns => [title, body]}
            )
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie, <<"segment-cross-column-and">>, <<"main">>,
                <<"title:crossalpha AND body:crossbeta">>, #{
                    rank => none, columns => [title, body]
                }
            )
        ),
    [<<"4">>] =
        keys(
            search(
                Bookie, <<"segment-cross-column-and">>, <<"main">>,
                <<"body:crossalpha AND title:crossbeta">>, #{
                    rank => none, columns => [title, body]
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"segment-cross-column-and">>, <<"main">>,
                <<"title:crossalpha AND title:crossbeta">>, #{
                    rank => none, columns => [title, body]
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"segment">>, <<"main">>, <<"\"red apple\"">>, #{
                    rank => none
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"segment">>, <<"main">>, <<"NEAR(oldnearone oldneartwo, 2)">>, #{
                    rank => none
                }
            )
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie, <<"segment">>, <<"main">>, <<"\"blue grape\"">>, #{
                    rank => none
                }
            )
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie, <<"segment">>, <<"main">>, <<"NEAR(nearone neartwo, 2)">>, #{
                    rank => none
                }
            )
        ),
    [] = search(Bookie, <<"segment">>, <<"main">>, <<"red">>, #{}),
    true =
        segment_term_present(
            Bookie, <<"segment">>, <<"main">>, <<"blue">>, <<"body">>
        ),
    [<<"1">>, <<"2">>] = index_keys(Bookie, <<"segment">>, {fts_doc, <<"main">>}, doc),

    ok = leveled_bookie:book_ftsdelete(Bookie, <<"segment">>, <<"2">>, <<"main">>, #{}),
    [] = search(Bookie, <<"segment">>, <<"main">>, <<"apple">>, #{}),
    [] = search(Bookie, <<"segment">>, <<"main">>, <<"green">>, #{}),
    [] =
        keys(
            search(
                Bookie, <<"segment">>, <<"main">>, <<"\"green apple\"">>, #{
                    rank => none
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"segment">>,
                <<"main">>,
                <<"NEAR(deletenearone deleteneartwo, 2)">>,
                #{
                    rank => none
                }
            )
        ),
    [<<"1">>] = keys(search(Bookie, <<"segment">>, <<"main">>, all_docs, #{})),
    [<<"1">>, <<"2">>] = index_keys(Bookie, <<"segment">>, {fts_doc, <<"main">>}, doc),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"segment-shared-block">>, <<"1">>, <<"shared-1">>,
                    <<"main">>, #{body => <<"sharedblock keepalive">>},
                    #{}},
                {fts_put, <<"segment-shared-block">>, <<"2">>, <<"shared-2">>,
                    <<"main">>, #{body => <<"sharedblock keepalive">>},
                    #{}}
            ]
        ),
    [<<"1">>, <<"2">>] =
        keys(
            search(
                Bookie, <<"segment-shared-block">>, <<"main">>, <<"sharedblock">>, #{
                    rank => none
                }
            )
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-shared-block">>,
            <<"1">>,
            <<"shared-1b">>,
            <<"main">>,
            #{body => <<"replacementonly">>},
            #{}
        ),
    [<<"2">>] =
        keys(
            search(
                Bookie, <<"segment-shared-block">>, <<"main">>, <<"sharedblock">>, #{
                    rank => none
                }
            )
        ),

    ScaleRootPath = testutil:reset_filestructure("fts_segment_scale"),
    ScaleOpts =
        lists:ukeysort(
            1,
            [
                {max_pencillercachesize, 401},
                {max_journalobjectcount, 100},
                {max_journalsize, 200000}
                | start_opts(ScaleRootPath)
            ]
        ),
    {ok, ScaleBookie} = leveled_bookie:book_start(ScaleOpts),
    ScaleMax = 1000,
    ok =
        write_fts_batches(
            ScaleBookie,
            [
                {fts_put, <<"segment-scale">>, scaled_doc_key(N),
                    {<<"equivtitlebaseline">>, <<"equivbaseline">>}, <<"main">>,
                    #{
                        title => <<"equivtitlebaseline">>,
                        body => <<"equivbaseline equivnearone middle equivneartwo new york">>
                    },
                    #{prefixes => [5, 11]}}
             || N <- lists:seq(1, ScaleMax)
            ],
            50
        ),
    ok =
        write_fts_batches(
            ScaleBookie,
            [
                {fts_put, <<"segment-scale">>, scaled_doc_key(N),
                    {<<"equivtitleupdate">>, <<"equivupdate">>}, <<"main">>,
                    #{
                        title => <<"equivtitleupdate">>,
                        body =>
                            <<"equivupdate equivbodyupdate equivnearone gap equivneartwo new york">>
                    },
                    #{prefixes => [5, 11]}}
             || N <- lists:seq(10, ScaleMax, 10)
            ],
            50
        ),
    ok =
        write_fts_batches(
            ScaleBookie,
            [
                {fts_delete, <<"segment-scale">>, scaled_doc_key(N), <<"main">>,
                    #{prefixes => [5, 11]}}
             || N <- lists:seq(17, ScaleMax, 17)
            ],
            50
        ),
	    ScaleExpected =
	        [
	            scaled_doc_key(N)
	         || N <- lists:seq(1, ScaleMax),
	            N rem 10 =/= 0,
	            N rem 17 =/= 0
	        ],
	    UpdatedDocKey = scaled_doc_key(10),
	    DeletedDocKey = scaled_doc_key(17),
	    true =
	        segment_doc_delete_present(
	            ScaleBookie, <<"segment-scale">>, <<"main">>, <<"equivbaseline">>, <<"body">>,
	            UpdatedDocKey
	        ),
	    true =
	        segment_doc_delete_present(
	            ScaleBookie, <<"segment-scale">>, <<"main">>, <<"equivbaseline">>, <<"body">>,
	            DeletedDocKey
	        ),
		    [UpdatedDocKey, DeletedDocKey] =
		        lists:filter(
	            fun(Key) ->
	                lists:member(
	                    Key,
	                    index_keys(ScaleBookie, <<"segment-scale">>, {fts_doc, <<"main">>}, doc)
	                )
	            end,
	            [UpdatedDocKey, DeletedDocKey]
	        ),
	    ScaleExpected =
	        keys(
            search(
                ScaleBookie, <<"segment-scale">>, <<"main">>, <<"equivbaseline">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ok = leveled_bookie:book_close(ScaleBookie),
    {ok, ReopenedScaleBookie} = leveled_bookie:book_start(ScaleOpts),
    ScaleExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"segment-scale">>,
                <<"main">>,
                <<"equivbaseline">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"segment-scale">>,
                <<"main">>,
                <<"title:equivtitlebaseline">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleActiveExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(1, ScaleMax),
            N rem 17 =/= 0
        ],
    ScaleActiveExpected =
        keys(
            search(
                ReopenedScaleBookie, <<"segment-scale">>, <<"main">>, <<"\"new york\"">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleActiveExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"segment-scale">>,
                <<"main">>,
                <<"NEAR(equivnearone equivneartwo, 5)">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleUpdateExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(10, ScaleMax, 10),
            N rem 17 =/= 0
        ],
    ScaleUpdateExpected =
        keys(
            search(
                ReopenedScaleBookie, <<"segment-scale">>, <<"main">>, <<"equivupdate">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleUpdateExpected =
        keys(
            search(
                ReopenedScaleBookie, <<"segment-scale">>, <<"main">>, <<"^equivupdate">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleUpdateExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"segment-scale">>,
                <<"main">>,
                <<"title:^equivtitleupdate">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    [] =
        keys(
            search(
                ReopenedScaleBookie,
                <<"segment-scale">>,
                <<"main">>,
                <<"^equivbodyupdate">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ok = leveled_bookie:book_close(ReopenedScaleBookie),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-prefix-dirty">>,
            <<"pref">>,
            <<"pref-old">>,
            <<"main">>,
            #{title => <<"prefix stale">>, body => <<"prefix stale">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-prefix-dirty">>,
            <<"pref">>,
            <<"pref-new">>,
            <<"main">>,
            #{title => <<"prefix live">>, body => <<"prefix live">>},
            #{}
        ),
    [] =
        keys(
            search(
                Bookie, <<"segment-prefix-dirty">>, <<"main">>, <<"sta*">>, #{
                    columns => [title, body],
                    rank => none
                }
            )
        ),
    [<<"pref">>] =
        keys(
            search(
                Bookie, <<"segment-prefix-dirty">>, <<"main">>, <<"liv*">>, #{
                    columns => [title, body],
                    rank => none
                }
            )
        ),

    SegmentPhrasePrefixOpts = #{
        columns => [body],
        rank => none
    },
    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"segment-phrase-prefix">>, <<"p1">>, <<"p1">>,
                    <<"main">>, #{body => <<"equivnearone galaxy equivneartwo">>},
                    #{}},
                {fts_put, <<"segment-phrase-prefix">>, <<"p2">>, <<"p2">>,
                    <<"main">>,
                    #{body =>
                        <<"equivnearone gap distant distant distant equivneartwo">>},
                    #{}},
                {fts_put, <<"segment-phrase-prefix">>, <<"p3">>, <<"p3-old">>,
                    <<"main">>, #{body => <<"equivnearone gamma equivneartwo">>},
                    #{}},
                {fts_put, <<"segment-phrase-prefix">>, <<"p4">>, <<"p4">>,
                    <<"main">>, #{body => <<"alpha beta gamma omega">>},
                    #{}}
            ]
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-phrase-prefix">>,
            <<"p3">>,
            <<"p3-new">>,
            <<"main">>,
            #{body => <<"equivnearone zeta equivneartwo">>},
            #{}
        ),
    [<<"p1">>, <<"p2">>] =
        keys(
            search(
                Bookie,
                <<"segment-phrase-prefix">>,
                <<"main">>,
                <<"\"equivnearone ga\"*">>,
                SegmentPhrasePrefixOpts
            )
        ),
    [<<"p1">>] =
        keys(
            search(
                Bookie,
                <<"segment-phrase-prefix">>,
                <<"main">>,
                <<"NEAR(\"equivnearone ga\"* equivneartwo, 1)">>,
                SegmentPhrasePrefixOpts
            )
        ),
    [<<"p4">>] =
        keys(
            search(
                Bookie,
                <<"segment-phrase-prefix">>,
                <<"main">>,
                <<"\"alpha beta ga\"*">>,
                SegmentPhrasePrefixOpts
            )
        ),
    [<<"p4">>] =
        keys(
            search(
                Bookie,
                <<"segment-phrase-prefix">>,
                <<"main">>,
                <<"NEAR(\"alpha beta ga\"* \"beta gamma om\"*, 1)">>,
                SegmentPhrasePrefixOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"segment-phrase-prefix">>,
                <<"main">>,
                <<"\"equivnearone gam\"*">>,
                SegmentPhrasePrefixOpts
            )
        ),
    [SegmentPhrasePositionHit] =
        search(
            Bookie,
            <<"segment-phrase-prefix">>,
            <<"main">>,
            <<"\"equivnearone gal\"*">>,
            SegmentPhrasePrefixOpts#{return_positions => true}
        ),
    #{positions := SegmentPhrasePositions} = SegmentPhrasePositionHit,
    #{{phrase, [{<<"equivnearone">>, false, 0}, {<<"gal">>, true, 1}]} :=
        #{<<"body">> := [0]}} = SegmentPhrasePositions,

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-transition">>,
            <<"mix">>,
            <<"seg-old">>,
            <<"main">>,
            #{body => <<"transition red">>},
            #{}
        ),
    [<<"mix">>] =
        keys(
            search(
                Bookie, <<"segment-transition">>, <<"main">>, <<"red">>, #{
                    rank => none
                }
            )
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-transition">>,
            <<"mix">>,
            <<"seg-fresh">>,
            <<"main">>,
            #{body => <<"transition fresh">>},
            #{}
        ),
    [] =
        search(
            Bookie, <<"segment-transition">>, <<"main">>, <<"red">>, #{
                rank => none
            }
        ),
    [<<"mix">>] =
        keys(
            search(
                Bookie, <<"segment-transition">>, <<"main">>, <<"fresh">>, #{
                    rank => none
                }
            )
        ),

    ok = leveled_bookie:book_close(Bookie).

hot_term_segment_split_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_hot_term_segment_split"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"hot-segment">>,
    Index = <<"main">>,
    Count = 1300,
    ExpectedKeys = [hot_doc_key(N) || N <- lists:seq(1, Count)],
    Ops =
        [
            {fts_put, Bucket, hot_doc_key(N), hot_doc_key(N), Index,
                #{body => <<"hotterm uniquehot", (integer_to_binary(N))/binary>>}, #{}}
         || N <- lists:seq(1, Count)
        ],
    ok = leveled_bookie:book_ftsbatchput(Bookie, Ops),
    HotTerms =
        index_terms(
            Bookie,
            Bucket,
            {fts_segment, Index},
            {<<"hotterm">>, {0, <<"body">>}, {0, 0, 0, 0}},
            {<<"hotterm">>, {0, <<"body">>}, {1, 0, 0, 0}}
        ),
    true = length(HotTerms) > 1,
    true =
        lists:all(
            fun({_Term, ObjKey}) -> is_hidden_segment_carrier_key(ObjKey) end,
            HotTerms
        ),
    ExpectedKeys = keys(search(Bookie, Bucket, Index, <<"hotterm">>, #{rank => none})),
    ExpectedUniqueKey = hot_doc_key(777),
    [ExpectedUniqueKey] =
        keys(search(Bookie, Bucket, Index, <<"uniquehot777">>, #{rank => none})),
    ok = leveled_bookie:book_close(Bookie).

segment_rank_none_limit_order_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"segment-order">>, <<"010">>, <<"obj-010">>, <<"main">>,
                    #{body => <<"a x">>}, #{}},
                {fts_put, <<"segment-order">>, <<"100">>, <<"obj-100">>, <<"main">>,
                    #{body => <<"a b">>}, #{}}
            ]
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"segment-order">>,
            <<"050">>,
            <<"obj-050">>,
            <<"main">>,
            #{body => <<"a b">>},
            #{}
        ),
    [<<"050">>] =
        hit_keys(
            search(
                Bookie, <<"segment-order">>, <<"main">>, <<"\"a b\"">>, #{
                    rank => none,
                    limit => 1
                }
            )
        ),
    [<<"050">>, <<"100">>] =
        hit_keys(
            search(
                Bookie, <<"segment-order">>, <<"main">>, <<"NEAR(a b, 1)">>, #{
                    rank => none,
                    limit => 2
                }
            )
        ),
    [<<"010">>, <<"050">>, <<"100">>] =
        hit_keys(
            search(
                Bookie, <<"segment-order">>, <<"main">>, <<"a OR x">>, #{
                    rank => none,
                    limit => 3
                }
            )
        ),
    [<<"050">>, <<"100">>] =
        hit_keys(
            search(
                Bookie, <<"segment-order">>, <<"main">>, <<"a NOT x">>, #{
                    rank => none,
                    limit => 3
                }
            )
        ),
    ok = leveled_bookie:book_close(Bookie).

segment_limited_pair_update_reopen_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_segment_limited_pair_update"),
    Opts =
        lists:ukeysort(
            1,
            [
                {max_pencillercachesize, 401},
                {max_journalobjectcount, 100},
                {max_journalsize, 200000}
                | start_opts(RootPath)
            ]
        ),
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    Bucket = <<"segment-limited-pair-update">>,
    Index = <<"main">>,
    MaxDocs = 80,
    Limit = 20,

    ok =
        write_fts_batches(
            Bookie,
            [
                {fts_put, Bucket, scaled_doc_key(N), scaled_doc_key(N), Index,
                    #{body => <<"limitedold history culture archive">>},
                    #{columns => [body]}}
             || N <- lists:seq(1, MaxDocs)
            ],
            20
        ),
    ok =
        write_fts_batches(
            Bookie,
            [
                {fts_put, Bucket, scaled_doc_key(N), {updated, scaled_doc_key(N)}, Index,
                    #{body => <<"limitednew history culture archive">>},
                    #{columns => [body]}}
             || N <- lists:seq(10, MaxDocs, 10)
            ],
            20
        ),
    ok =
        write_fts_batches(
            Bookie,
            [
                {fts_delete, Bucket, scaled_doc_key(N), Index, #{
                    columns => [body]
                }}
             || N <- lists:seq(17, MaxDocs, 17)
            ],
            20
        ),
    ok = leveled_bookie:book_close(Bookie),

    {ok, ReopenedBookie} = leveled_bookie:book_start(Opts),
    PairOpts = #{
        rank => none,
        columns => [body],
        limit => Limit
    },
    ActiveExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(1, MaxDocs),
            N rem 17 =/= 0
        ],
    OldExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(1, MaxDocs),
            N rem 10 =/= 0,
            N rem 17 =/= 0
        ],
    ActiveLimitedExpected = lists:sublist(ActiveExpected, Limit),
    OldLimitedExpected = lists:sublist(OldExpected, Limit),
    ActiveLimitedExpected =
        hit_keys(
            search(ReopenedBookie, Bucket, Index, <<"\"history culture\"">>, PairOpts)
        ),
    ActiveLimitedExpected =
        hit_keys(
            search(ReopenedBookie, Bucket, Index, <<"NEAR(history culture, 10)">>, PairOpts)
        ),
    OldLimitedExpected =
        hit_keys(
            search(ReopenedBookie, Bucket, Index, <<"\"limitedold history\"">>, PairOpts)
        ),
    ok = leveled_bookie:book_close(ReopenedBookie).

invalid_write_inputs(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    {error, invalid_fts_fields} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"bad-fields">>,
            <<"obj">>,
            <<"main">>,
            [bad],
            #{}
        ),
    not_written(Bookie, <<"invalid">>, <<"bad-fields">>, <<"main">>, <<"obj">>),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"bad-options">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"bad option body">>},
            [bad]
        ),
    not_written(Bookie, <<"invalid">>, <<"bad-options">>, <<"main">>, <<"obj">>),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"store-fields-option">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"stored fields are deferred">>},
            #{store_fields => true}
        ),
    not_written(
        Bookie, <<"invalid">>, <<"store-fields-option">>, <<"main">>, <<"obj">>
    ),

    {async, ReturnFieldsRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"invalid">>,
            <<"main">>,
            <<"stored">>,
            #{columns => [body], return_fields => true}
        ),
    {error, missing_fts_schema} = ReturnFieldsRunner(),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"bad-index-specs">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"bad index body">>},
            #{index_specs => bad}
        ),
    not_written(
        Bookie, <<"invalid">>, <<"bad-index-specs">>, <<"main">>, <<"obj">>
    ),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"reserved-index-spec">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"reserved fts field">>},
            #{index_specs => [
                {add, {fts_segment, <<"main">>}, <<"reserved">>}
            ]}
        ),
    not_written(
        Bookie, <<"invalid">>, <<"reserved-index-spec">>, <<"main">>, <<"obj">>
    ),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"reserved-schema-index-spec">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"reserved fts schema field">>},
            #{index_specs => [
                {add, {fts_schema, <<"main">>}, schema}
            ]}
        ),
    not_written(
        Bookie, <<"invalid">>, <<"reserved-schema-index-spec">>, <<"main">>, <<"obj">>
    ),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            <<"bad-options-term">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"bad option term">>},
            bogus
        ),
    not_written(
        Bookie, <<"invalid">>, <<"bad-options-term">>, <<"main">>, <<"obj">>
    ),

    {error, {invalid_batch_object_spec, {fts_put, _, _, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, <<"head-tag">>, <<"obj">>, <<"main">>,
                    #{body => <<"head tag">>}, #{}, ?HEAD_TAG, infinity}
            ]
        ),
    not_written(Bookie, <<"invalid">>, <<"head-tag">>, <<"main">>, <<"obj">>),

    {error, invalid_fts_key} =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid">>,
            {<<"tuple">>, <<"key">>},
            <<"obj">>,
            <<"main">>,
            #{body => <<"tuple key">>},
            #{}
        ),
    {error, invalid_fts_key} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, {<<"tuple">>, <<"batch">>}, <<"obj">>,
                    <<"main">>, #{body => <<"tuple batch">>}, #{}}
            ]
        ),
    {error, invalid_fts_key} =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"invalid">>,
            {<<"tuple">>, <<"delete">>},
            <<"main">>,
            #{}
        ),

    {error, invalid_fts_options} =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"invalid">>,
            <<"bad-delete-options">>,
            <<"main">>,
            [bad]
        ),
    not_written(
        Bookie, <<"invalid">>, <<"bad-delete-options">>, <<"main">>, <<"obj">>
    ),
    {error, missing_fts_schema} =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"invalid">>,
            <<"bad-delete">>,
            <<"main">>,
            #{index_specs => [{add, <<"extra">>, <<"bad">>}]}
        ),
    not_written(Bookie, <<"invalid">>, <<"bad-delete">>, <<"main">>, <<"obj">>),
    assert_missing_fts_schema(Bookie, <<"invalid">>, <<"main">>, all_docs, #{}),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"invalid-delete">>,
            <<"reserved-schema-delete">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"delete reserved schema">>},
            #{}
        ),
    {error, invalid_fts_options} =
        leveled_bookie:book_ftsdelete(
            Bookie,
            <<"invalid-delete">>,
            <<"reserved-schema-delete">>,
            <<"main">>,
            #{index_specs => [
                {remove, {fts_schema, <<"main">>}, schema}
            ]}
        ),
    [<<"reserved-schema-delete">>] =
        keys(search(Bookie, <<"invalid-delete">>, <<"main">>, <<"reserved">>, #{})),

    {error, invalid_fts_fields} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, <<"bad-batch">>, <<"obj">>, <<"main">>,
                    [bad], #{}}
            ]
        ),
    not_written(Bookie, <<"invalid">>, <<"bad-batch">>, <<"main">>, <<"obj">>),

    {error, invalid_fts_fields} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, <<"bad-batch-duplicate">>, <<"bad">>,
                    <<"main">>, [bad], #{}},
                {fts_put, <<"invalid">>, <<"bad-batch-duplicate">>, <<"good">>,
                    <<"main">>, #{body => <<"collapsevalid">>}, #{}}
            ]
        ),
    not_written(
        Bookie, <<"invalid">>, <<"bad-batch-duplicate">>, <<"main">>, <<"good">>
    ),
    assert_missing_fts_schema(Bookie, <<"invalid">>, <<"main">>, <<"collapsevalid">>, #{}),

    {error, {invalid_batch_object_spec, {fts_put, _, _, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, <<"bad-batch-unsupported">>, <<"bad">>,
                    <<"main">>, #{body => <<"unsupported">>}, #{}, ?STD_TAG, invalid_ttl},
                {fts_put, <<"invalid">>, <<"bad-batch-unsupported">>, <<"good">>,
                    <<"main">>, #{body => <<"valid">>}, #{}}
            ]
        ),
    not_written(Bookie, <<"invalid">>, <<"bad-batch-unsupported">>, <<"main">>, <<"good">>),
    assert_missing_fts_schema(Bookie, <<"invalid">>, <<"main">>, <<"valid">>, #{}),

    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {put, <<"invalid-raw">>, <<"1">>, <<"raw">>,
                    [{add, <<"raw_leak_bin">>, <<"fields">>}], ?STD_TAG, infinity},
                {fts_put, <<"invalid">>, <<"bad-batch">>, <<"obj">>, <<"main">>,
                    [bad], #{}}
            ]
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"invalid-raw">>, <<"1">>),
    [] = index_keys(Bookie, <<"invalid-raw">>, <<"raw_leak_bin">>, <<"fields">>),

    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {put, <<"invalid-raw">>, <<"2">>, <<"raw">>,
                    [{add, <<"raw_leak_bin">>, <<"options">>}], ?STD_TAG, infinity},
                {fts_put, <<"invalid">>, <<"bad-batch-index-specs">>, <<"obj">>,
                    <<"main">>, #{body => <<"bad batch index specs">>},
                    #{index_specs => bad}}
            ]
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"invalid-raw">>, <<"2">>),
    [] = index_keys(Bookie, <<"invalid-raw">>, <<"raw_leak_bin">>, <<"options">>),
    not_written(
        Bookie, <<"invalid">>, <<"bad-batch-index-specs">>, <<"main">>, <<"obj">>
    ),

    ok =
        leveled_bookie:book_batchput(
            Bookie,
            [
                {put, <<"invalid-raw">>, <<"remove-seed">>, <<"raw-remain">>,
                    [{add, <<"raw_leak_bin">>, <<"remove">>}], ?STD_TAG, infinity}
            ]
        ),
    [<<"remove-seed">>] =
        index_keys(Bookie, <<"invalid-raw">>, <<"raw_leak_bin">>, <<"remove">>),
    {error, {invalid_batch_object_spec, {delete, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {delete, <<"invalid-raw">>, <<"remove-seed">>,
                    [{remove, <<"raw_leak_bin">>, <<"remove">>}], ?STD_TAG, infinity},
                {fts_put, <<"invalid">>, <<"bad-batch-raw-remove">>, <<"obj">>,
                    <<"main">>, [bad], #{}}
            ]
        ),
    {ok, <<"raw-remain">>} =
        leveled_bookie:book_get(Bookie, <<"invalid-raw">>, <<"remove-seed">>),
    [<<"remove-seed">>] =
        index_keys(Bookie, <<"invalid-raw">>, <<"raw_leak_bin">>, <<"remove">>),
    not_written(
        Bookie, <<"invalid">>, <<"bad-batch-raw-remove">>, <<"main">>, <<"obj">>
    ),

    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, <<"bad-batch-raw-put">>, <<"obj">>,
                    <<"main">>, #{body => <<"bad raw put index specs">>}, #{}},
                {put, <<"invalid-raw">>, <<"3">>, <<"raw">>, [bad], ?STD_TAG, infinity}
            ]
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"invalid-raw">>, <<"3">>),
    not_written(
        Bookie, <<"invalid">>, <<"bad-batch-raw-put">>, <<"main">>, <<"obj">>
    ),

    {error, {invalid_batch_object_spec, {delete, _, _, _, _, _}}} =
        leveled_bookie:book_ftsbatchput(
            Bookie,
            [
                {fts_put, <<"invalid">>, <<"bad-batch-raw-delete">>, <<"obj">>,
                    <<"main">>, #{body => <<"bad raw delete index specs">>}, #{}},
                {delete, <<"invalid-raw">>, <<"4">>, [bad], ?STD_TAG, infinity}
            ]
        ),
    not_written(
        Bookie, <<"invalid">>, <<"bad-batch-raw-delete">>, <<"main">>, <<"obj">>
    ),

    {error, fts_segment_mode_requires_infinity_ttl} =
        leveled_fts:put_batch_specs(
            <<"invalid">>,
            <<"finite-ttl">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"finite ttl">>},
            #{},
            ?STD_TAG,
            leveled_util:integer_now() + 60,
            not_found
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"invalid">>, <<"finite-ttl">>),

    ok = leveled_bookie:book_close(Bookie),
    {ok, ReopenedBookie} = leveled_bookie:book_start(start_opts(RootPath)),
    not_found = leveled_bookie:book_get(ReopenedBookie, <<"invalid">>, <<"finite-ttl">>),
    ok = leveled_bookie:book_close(ReopenedBookie).

private_snapshot_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"private-snapshot">>,
            <<"1">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"history culture">>},
            #{}
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie,
                <<"private-snapshot">>,
                <<"main">>,
                <<"NEAR(history culture, 5)">>,
                #{}
            )
        ),

    ok = leveled_bookie:book_close(Bookie).

regular_index_snapshot_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"001">>,
            <<"obj-001">>,
            [{add, <<"kind_bin">>, <<"alpha">>}]
        ),
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"002">>,
            <<"obj-002">>,
            [{add, <<"kind_bin">>, <<"alpha">>}]
        ),
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"003">>,
            <<"obj-003">>,
            [{add, <<"kind_bin">>, <<"beta">>}]
        ),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),

    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"000">>,
            <<"obj-000">>,
            [{add, <<"kind_bin">>, <<"aardvark">>}]
        ),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),

    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"999">>,
            <<"obj-999">>,
            [{add, <<"kind_bin">>, <<"zulu">>}]
        ),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),

    ok = leveled_bookie:book_close(Bookie),
    {ok, Bookie2} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),

    ok =
        leveled_bookie:book_put(
            Bookie2,
            <<"regular-snapshot">>,
            <<"004">>,
            <<"obj-004">>,
            [{add, <<"kind_bin">>, <<"alpha">>}]
        ),
    ok =
        leveled_bookie:book_put(
            Bookie2,
            <<"regular-snapshot">>,
            <<"998">>,
            <<"obj-998">>,
            [{add, <<"kind_bin">>, <<"omega">>}]
        ),
    [<<"001">>, <<"002">>, <<"004">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),
    ok = leveled_bookie:book_compactjournal(Bookie2, 30000),
    [<<"001">>, <<"002">>, <<"004">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),

    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie3} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"000">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"aardvark">>),
    [<<"001">>, <<"002">>, <<"004">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),
    [<<"998">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"omega">>),
    [<<"999">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"zulu">>),

    ok = leveled_bookie:book_close(Bookie3).

payload_index_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_put(
            Bookie, <<"payload">>, <<"direct-normal">>, <<"obj">>, [], ?STD_TAG
        ),
    {ok, <<"obj">>} = leveled_bookie:book_get(Bookie, <<"payload">>, <<"direct-normal">>),
    ok = leveled_bookie:book_close(Bookie),

    {ok, ReplayedBookie} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        leveled_bookie:book_ftsput(
            ReplayedBookie,
            <<"payload">>,
            <<"direct-payload">>,
            <<"legit">>,
            <<"directidx">>,
            #{body => <<"honest">>},
            #{}
        ),
    [<<"direct-payload">>] =
        keys(search(ReplayedBookie, <<"payload">>, <<"directidx">>, <<"honest">>, #{})),
    [] = search(ReplayedBookie, <<"payload">>, <<"directidx">>, <<"leak">>, #{}),
    assert_user_change_has_doc_marker_only(
        ReplayedBookie, <<"payload">>, <<"direct-payload">>, <<"directidx">>
    ),
    assert_hidden_segment_carrier(
        ReplayedBookie,
        <<"payload">>,
        <<"direct-payload">>,
        <<"directidx">>,
        <<"honest">>,
        <<"body">>
    ),
    {async, HashlistRunner} =
        leveled_bookie:book_returnfolder(
            ReplayedBookie, {hashlist_query, ?IDX_TAG, false}
        ),
    {error, unsupported_idx_hashlist_query} = HashlistRunner(),
    lists:foreach(
        fun(Field) ->
            {async, TicTacRunner} =
                leveled_bookie:book_returnfolder(
                    ReplayedBookie,
                    {tictactree_idx, {<<"payload">>, Field, null, null}, 1024,
                        fun(_B, _K) -> accumulate end}
                ),
            {error, {unsupported_fts_payload_tictactree_idx, Field}} = TicTacRunner()
        end,
        [
            {fts_doc, <<"directidx">>},
            {fts_schema, <<"directidx">>},
            {fts_segment, <<"directidx">>},
            {fts_segment_doc_delete, <<"directidx">>}
        ]
    ),
    ok =
        leveled_bookie:book_ftsdelete(
            ReplayedBookie, <<"payload">>, <<"direct-payload">>, <<"directidx">>, #{}
        ),
    assert_hidden_delete_segment_carrier(
        ReplayedBookie,
        <<"payload">>,
        <<"direct-payload">>,
        <<"directidx">>,
        <<"honest">>,
        <<"body">>
    ),
    [] = search(ReplayedBookie, <<"payload">>, <<"directidx">>, <<"honest">>, #{}),
    ok = leveled_bookie:book_close(ReplayedBookie),

    {ok, ReopenedBookie} = leveled_bookie:book_start(start_opts(RootPath)),
    {ok, <<"obj">>} =
        leveled_bookie:book_get(ReopenedBookie, <<"payload">>, <<"direct-normal">>),
    ok = leveled_bookie:book_close(ReopenedBookie),

    assert_retain_compacted_fts_replay(),
    assert_doc_marker_latest_wins().

recovery_and_hotbackup(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BackupPath = testutil:reset_filestructure("fts_backup"),
    {ok, Bookie1} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"recover">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"old durable search">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"recover">>,
            <<"2">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"delete durable search">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"recover">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"new durable search">>},
            #{}
        ),
    ok = leveled_bookie:book_ftsdelete(Bookie1, <<"recover">>, <<"2">>, <<"main">>, #{}),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, Bookie2} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"1">>] = keys(search(Bookie2, <<"recover">>, <<"main">>, <<"new">>, #{})),
    [] = search(Bookie2, <<"recover">>, <<"main">>, <<"old">>, #{}),
    [<<"1">>] = keys(search(Bookie2, <<"recover">>, <<"main">>, all_docs, #{})),
    not_found = leveled_bookie:book_get(Bookie2, <<"recover">>, <<"2">>),
    ok = leveled_bookie:book_compactjournal(Bookie2, 30000),
    testutil:wait_for_compaction(Bookie2),
    [<<"1">>] = keys(search(Bookie2, <<"recover">>, <<"main">>, <<"new">>, #{})),
    {async, BackupFun} = leveled_bookie:book_hotbackup(Bookie2),
    ok = BackupFun(BackupPath),
    ok = leveled_bookie:book_close(Bookie2),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, BookieReplay} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"1">>] =
        keys(search(BookieReplay, <<"recover">>, <<"main">>, <<"new">>, #{})),
    [] = search(BookieReplay, <<"recover">>, <<"main">>, <<"delete">>, #{}),
    assert_segment_manifest_update(
        BookieReplay,
        <<"recover">>,
        <<"main">>,
        <<"1">>,
        <<"new">>,
        <<"replayed">>
    ),
    ok = leveled_bookie:book_close(BookieReplay),

    {ok, Bookie3} = leveled_bookie:book_start(start_opts(BackupPath)),
    [<<"1">>] = keys(search(Bookie3, <<"recover">>, <<"main">>, <<"new">>, #{})),
    [] = search(Bookie3, <<"recover">>, <<"main">>, <<"delete">>, #{}),
    assert_segment_manifest_update(
        Bookie3,
        <<"recover">>,
        <<"main">>,
        <<"1">>,
        <<"new">>,
        <<"backup">>
    ),
    ok = leveled_bookie:book_destroy(Bookie3).

partial_tail_recovery_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_partial_tail"),
    Opts =
        [
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {sync_strategy, riak_sync},
            {compression_method, none},
            {ledger_compression, none},
            {log_level, warning}
        ],
    Bucket = <<"partial-tail">>,
    Index = <<"main">>,
    {ok, Bookie1} = leveled_bookie:book_plainstart(Opts),
    ok =
        leveled_bookie:book_ftsbatchput(Bookie1, [
            {fts_put, Bucket, <<"1">>, <<"obj1">>, Index, #{body => <<"tail one">>}, #{}},
            {fts_put, Bucket, <<"2">>, <<"obj2">>, Index, #{body => <<"tail two">>}, #{}}
        ]),
    [<<"1">>] = keys(search(Bookie1, Bucket, Index, <<"one">>, #{})),
    [<<"2">>] = keys(search(Bookie1, Bucket, Index, <<"two">>, #{})),

    {ok, Inker, Penciller} = leveled_bookie:book_returnactors(Bookie1),
    [ActiveJournal | _] = leveled_inker:ink_getcdbpids(Inker),
    JournalFile = leveled_cdb:cdb_filename(ActiveJournal),
    BookieRef = erlang:monitor(process, Bookie1),
    InkerRef = erlang:monitor(process, Inker),
    PencillerRef = erlang:monitor(process, Penciller),
    exit(Bookie1, kill),
    wait_down(BookieRef, Bookie1, bookie),
    wait_down(InkerRef, Inker, inker),
    wait_down(PencillerRef, Penciller, penciller),
    truncate_after_first_cdb_record(JournalFile),

    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    not_found = leveled_bookie:book_get(Bookie2, Bucket, <<"1">>),
    not_found = leveled_bookie:book_get(Bookie2, Bucket, <<"2">>),
    [] = index_keys(Bookie2, Bucket, {fts_doc, Index}, doc),
    [] = index_terms(
        Bookie2,
        Bucket,
        {fts_segment, Index},
        {<<"tail">>, {0, <<"body">>}, {0, 0, 0, 0}},
        {<<"tail">>, {0, <<"body">>}, {1, 0, 0, 0}}
    ),
    assert_missing_fts_schema(Bookie2, Bucket, Index, <<"tail">>, #{columns => [body]}),
    ok = leveled_bookie:book_destroy(Bookie2).

recalc_reload_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    Opts = start_opts(RootPath) ++ [{reload_strategy, [{?STD_TAG, recalc}]}],
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    WriteOpts = #{
        prefixes => [3],
        index_specs => [{add, <<"recalc_kind_bin">>, <<"before">>}]
    },
    SearchOpts = #{},
    PrefixSearchOpts = #{prefixes => [3]},

    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"recalc">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"survives recalc ledger rebuild">>},
            WriteOpts
        ),
    [<<"1">>] =
        keys(search(Bookie1, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts)),
    [<<"1">>] =
        keys(search(Bookie1, <<"recalc">>, <<"main">>, <<"sur*">>, PrefixSearchOpts)),
    [<<"1">>] =
        index_keys(Bookie1, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),
    ok = leveled_bookie:book_close(Bookie1),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    [<<"1">>] =
        keys(search(Bookie2, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts)),
    [<<"1">>] =
        keys(search(Bookie2, <<"recalc">>, <<"main">>, <<"sur*">>, PrefixSearchOpts)),
    [<<"1">>] =
        keys(search(Bookie2, <<"recalc">>, <<"main">>, all_docs, SearchOpts)),
    [<<"1">>] = index_keys(Bookie2, <<"recalc">>, {fts_doc, <<"main">>}, doc),
    [<<"1">>] =
        index_keys(Bookie2, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),

    ok =
        leveled_bookie:book_ftsput(
            Bookie2,
            <<"recalc">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"after recalc rebuild">>},
            #{
                prefixes => [3],
                index_specs => [
                    {remove, <<"recalc_kind_bin">>, <<"before">>},
                    {add, <<"recalc_kind_bin">>, <<"after">>}
                ]
            }
        ),
    [] = search(Bookie2, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts),
    [] = search(Bookie2, <<"recalc">>, <<"main">>, <<"sur*">>, PrefixSearchOpts),
    [<<"1">>] =
        keys(search(Bookie2, <<"recalc">>, <<"main">>, <<"after">>, SearchOpts)),
    [] = index_keys(Bookie2, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),
    [<<"1">>] =
        index_keys(Bookie2, <<"recalc">>, <<"recalc_kind_bin">>, <<"after">>),
    ok = leveled_bookie:book_close(Bookie2),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, Bookie3} = leveled_bookie:book_start(Opts),
    [] = search(Bookie3, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts),
    [<<"1">>] =
        keys(search(Bookie3, <<"recalc">>, <<"main">>, <<"after">>, SearchOpts)),
    [] = index_keys(Bookie3, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),
    [<<"1">>] =
        index_keys(Bookie3, <<"recalc">>, <<"recalc_kind_bin">>, <<"after">>),
    ok = leveled_bookie:book_close(Bookie3).

sqlite_supported_ast_differential_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_sqlite_ast_differential"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"sqlite-ast">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body], prefixes => [2, 3, 5], remove_diacritics => 2},
    SearchOpts = WriteOpts#{rank => none, limit => 100},
    Docs = [
        {<<"d01">>, <<"alpha title">>, <<"bodyonly gamma new york nearone middle neartwo">>},
        {<<"d02">>, <<"beta title">>, <<"alpha bodyonly new york nearone gap gap gap neartwo">>},
        {<<"d03">>, <<"gamma title">>, <<"one two three">>},
        {<<"d04">>, <<"delta">>, <<"world alpha">>},
        {<<"d05">>, <<"prefix">>, <<"alphabet alphanumeric">>},
        {<<"d06">>, <<"cafe">>, <<"caf", 195, 169, " e", 204, 129, "clair london_distance">>}
    ],
    ValidQueries = [
        <<"alpha">>,
        <<"title:alpha">>,
        <<"body:alpha">>,
        <<"-title:alpha">>,
        <<"{title body}:alpha">>,
        <<"-{title}:alpha">>,
        <<"\"new york\"">>,
        <<"new + york">>,
        <<"alph*">>,
        <<"^alpha">>,
        <<"NEAR(nearone neartwo, 2)">>,
        <<"alpha OR beta gamma">>,
        <<"alpha NOT bodyonly">>,
        <<"one NOT two three">>,
        <<"cafe">>,
        <<"london_distance">>,
        <<"london distance">>,
        <<"one OR two three">>,
        <<"NEAR(\"new york\" neartwo, 5)">>
    ],
    InvalidQueries = [
        <<"(alpha OR beta) gamma">>,
        <<"one (two OR three)">>,
        <<"^title:alpha">>,
        <<"^NEAR(alpha beta)">>,
        <<"bogus:alpha">>
    ],
    Queries = ValidQueries ++ InvalidQueries,
    write_sqlite_diff_docs(Bookie, Bucket, Index, Docs, WriteOpts),
    {error, {invalid_fts_contract_change, columns, _, _}} =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"schema-column-mismatch">>,
            <<"schema-column-mismatch">>,
            Index,
            #{body => <<"schema mismatch">>},
            #{columns => [body], prefixes => [2, 3, 5], remove_diacritics => 2}
        ),
    {error, {invalid_fts_contract_change, prefixes, [2, 3, 5], [4]}} =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            <<"schema-prefix-mismatch">>,
            <<"schema-prefix-mismatch">>,
            Index,
            #{title => <<"schema">>, body => <<"mismatch">>},
            #{columns => [title, body], prefixes => [4], remove_diacritics => 2}
        ),
    Expected = sqlite_expected_results(RootPath, Docs, Queries),
    lists:foreach(fun(Query) -> error = maps:get(Query, Expected) end, InvalidQueries),
    lists:foreach(
        fun(Query) ->
            assert_sqlite_diff_query(Bookie, Bucket, Index, Query, SearchOpts, Expected)
        end,
        Queries
    ),
    ok = leveled_bookie:book_close(Bookie).

unicode61_supported_parity_corpus_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_unicode61_parity"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"sqlite-unicode">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body], prefixes => [2, 3, 5], remove_diacritics => 2},
    SearchOpts = WriteOpts#{rank => none, limit => 100},
    PrivateUse = <<238, 128, 128>>,
    Acute = <<204, 129>>,
    Docs = [
        {<<"u01">>, <<"Case">>, <<"CAF", 195, 137, " e", 204, 129, "clair ", 196, 176,
            "STANBUL ", 199, 141, " ", 225, 184, 131>>},
        {<<"u02">>, <<"Private">>, <<PrivateUse/binary, "x marker">>},
        {<<"u03">>, <<"Combining">>, <<Acute/binary, "alpha beta">>},
        {<<"u04">>, <<"Separators">>,
            <<"london_distance koji", 240, 159, 152, 128, "grains koji", 226, 128, 147,
                "grains">>},
        {<<"u05">>, <<"Malformed">>, <<"malformed ", 255, " utf8tail">>}
    ],
    Queries = [
        <<"cafe">>,
        <<"eclair">>,
        <<"istanbul">>,
        <<"a">>,
        <<"b">>,
        <<PrivateUse/binary, "x">>,
        <<"alpha">>,
        <<"london_distance">>,
        <<"\"london distance\"">>,
        <<"\"koji grains\"">>,
        <<"koji grains">>,
        <<"malformed">>,
        <<"utf8tail">>
    ],
    write_sqlite_diff_docs(Bookie, Bucket, Index, Docs, WriteOpts),
    Expected = sqlite_expected_results(RootPath, Docs, Queries),
    lists:foreach(
        fun(Query) ->
            assert_sqlite_diff_query(Bookie, Bucket, Index, Query, SearchOpts, Expected)
        end,
        Queries
    ),
    ok = leveled_bookie:book_close(Bookie).

parse_errors(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"valid content">>},
            #{columns => [title, body]}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"stop">>,
            <<"stop-object">>,
            <<"stop">>,
            #{body => <<"quick the fox">>},
            #{stopwords => [<<"the">>]}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"column">>,
            <<"column-object">>,
            <<"main">>,
            #{<<"Title">> => <<"History">>, body => <<"plain body">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"not1">>,
            <<"not1-object">>,
            <<"main">>,
            #{body => <<"one">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"not2">>,
            <<"not2-object">>,
            <<"main">>,
            #{body => <<"one two">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"not3">>,
            <<"not3-object">>,
            <<"main">>,
            #{body => <<"one two three">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"sep1">>,
            <<"sep1-object">>,
            <<"main">>,
            #{body => <<"koji grains">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"sep2">>,
            <<"sep2-object">>,
            <<"main">>,
            #{body => <<"koji x grains">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"sep3">>,
            <<"sep3-object">>,
            <<"main">>,
            #{body => <<"grains koji">>},
            #{}
        ),
    LongPositionText =
        iolist_to_binary(lists:join(<<" ">>, lists:duplicate(4097, <<"repeatcap">>))),
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            <<"parse">>,
            <<"position-cap">>,
            <<"position-cap-object">>,
            <<"main">>,
            #{body => LongPositionText},
            #{}
        ),
    StopOpts = #{stopwords => [<<"the">>]},
    [] = search(Bookie, <<"parse">>, <<"stop">>, <<"the">>, StopOpts),
    [<<"stop">>] = keys(search(Bookie, <<"parse">>, <<"stop">>, <<"the quick">>, StopOpts)),
    [<<"stop">>] =
        keys(search(Bookie, <<"parse">>, <<"stop">>, <<"\"quick the fox\"">>, StopOpts)),
    [] = search(Bookie, <<"parse">>, <<"stop">>, <<"\"quick fox\"">>, StopOpts),
    [<<"column">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"title:history">>, #{
                columns => [title, body]
            })
        ),
    [<<"column">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"TITLE:history">>, #{
                columns => [title, body]
            })
        ),
    {async, TitleNoSchemaOpts} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, <<"title:history">>, #{}),
    {ok, TitleNoSchemaOptsHits} = TitleNoSchemaOpts(),
    [<<"column">>] = keys(TitleNoSchemaOptsHits),
    [] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"title:(history body)">>, #{
                columns => [title, body]
            })
        ),
    [<<"sep1">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"koji", 226, 128, 147, "grains">>, #{})),
    [<<"sep1">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"koji", 240, 159, 152, 128, "grains">>, #{})),
    [<<"sep1">>, <<"sep2">>, <<"sep3">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"koji ", 226, 128, 147, " grains">>, #{})),
    [<<"not1">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"one NOT two NOT three">>, #{})),

    {async, BlankQuery} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<" \t">>, #{columns => [body]}
        ),
    {error, {fts_parse, empty_query}} = BlankQuery(),
    {async, BadQuery} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"(">>, #{columns => [body]}
        ),
    {error, {fts_parse, unexpected_end}} = BadQuery(),
    {async, UnknownColumn} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"parse">>,
            <<"main">>,
            <<"bogus:history">>,
            #{columns => [title, body]}
        ),
    {error, {fts_parse, unknown_column, <<"bogus">>}} = UnknownColumn(),
    {async, UnknownColumnNoSchema} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, <<"bogus:history">>, #{}),
    {error, {fts_parse, unknown_column, <<"bogus">>}} = UnknownColumnNoSchema(),
    {async, BadRank} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"valid">>, #{columns => [body], rank => bogus}
        ),
    {error, invalid_rank_option} = BadRank(),
    LongQuery = binary:copy(<<"a">>, 4097),
    {async, LongQueryRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, LongQuery, #{}),
    {error, fts_query_too_large} = LongQueryRunner(),
    LongQueryList = lists:duplicate(4097, $a),
    {async, LongQueryListRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, LongQueryList, #{}),
    {error, fts_query_too_large} = LongQueryListRunner(),
    TooManyTokens =
        iolist_to_binary(lists:join(<<" ">>, lists:duplicate(129, <<"valid">>))),
    {async, TooManyTokensRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, TooManyTokens, #{}),
    {error, fts_query_too_many_tokens} = TooManyTokensRunner(),
    DeepAnd =
        iolist_to_binary(lists:join(<<" AND ">>, lists:duplicate(40, <<"valid">>))),
    {async, DeepAndRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, DeepAnd, #{}),
    {error, fts_query_ast_too_deep} = DeepAndRunner(),
    {async, NearCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"NEAR(valid content, 65)">>, #{}
        ),
    {error, fts_query_near_distance_exceeded} = NearCapRunner(),
    LongPrefixToken = binary:copy(<<"a">>, 65),
    LongPrefixQuery = <<LongPrefixToken/binary, "*">>,
    {async, PrefixCapRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, LongPrefixQuery, #{}),
    {error, fts_query_prefix_too_large} = PrefixCapRunner(),
    {async, LimitCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"valid">>, #{limit => 20001}
        ),
    {error, fts_query_limit_exceeded} = LimitCapRunner(),
    {async, WindowCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"valid">>, #{limit => 20, offset => 19990}
        ),
    {error, fts_query_limit_exceeded} = WindowCapRunner(),
    {async, PositionCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"repeatcap">>, #{return_positions => true}
        ),
    {error, fts_query_positions_limit_exceeded} = PositionCapRunner(),
    ok = leveled_bookie:book_close(Bookie).

start_opts(RootPath) ->
    [
        {root_path, RootPath},
        {sync_strategy, testutil:sync_strategy()},
        {compression_method, none},
        {ledger_compression, none},
        {log_level, warning}
    ].

search(Bookie, Bucket, Index, Query, Opts) ->
    SearchOpts =
        case maps:is_key(columns, Opts) of
            true -> Opts;
            false -> Opts#{columns => [body]}
        end,
    {async, Runner} = leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, SearchOpts),
    {ok, Hits} = Runner(),
    Hits.

assert_missing_fts_schema(Bookie, Bucket, Index, Query, Opts) ->
    {async, Runner} = leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, Opts),
    {error, missing_fts_schema} = Runner().

keys(Hits) ->
    lists:sort([maps:get(key, Hit) || Hit <- Hits]).

concurrent_generation_term(N) ->
    <<"concurrentterm", (integer_to_binary(N))/binary>>.

collect_concurrent_ftsputs(0, Acc) ->
    lists:sort(Acc);
collect_concurrent_ftsputs(Count, Acc) ->
    receive
        {concurrent_ftsput, N, Term, Result} ->
            collect_concurrent_ftsputs(Count - 1, [{N, Term, Result} | Acc])
    after 30000 ->
        erlang:error({timeout, concurrent_ftsput, Count})
    end.

wait_down(Ref, Pid, Label) ->
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after 5000 ->
        error({process_still_alive, Label})
    end.

truncate_after_first_cdb_record(JournalFile) ->
    {ok, Handle} = file:open(JournalFile, [read, write, binary]),
    {ok, 2048} = file:position(Handle, {bof, 2048}),
    {ok, <<KeyLength:32/little-integer, ValueLength:32/little-integer>>} =
        file:read(Handle, 8),
    FirstRecordEnd = 2048 + 8 + KeyLength + ValueLength,
    {ok, _} = file:position(Handle, {bof, FirstRecordEnd + 4}),
    ok = file:truncate(Handle),
    ok = file:close(Handle).

write_sqlite_diff_docs(Bookie, Bucket, Index, Docs, WriteOpts) ->
    Ops =
        [
            {fts_put, Bucket, Key, Key, Index, #{title => Title, body => Body}, WriteOpts}
         || {Key, Title, Body} <- Docs
        ],
    ok = leveled_bookie:book_ftsbatchput(Bookie, Ops).

assert_sqlite_diff_query(Bookie, Bucket, Index, Query, SearchOpts, Expected) ->
    ExpectedResult = maps:get(Query, Expected),
    ActualResult =
        case leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, SearchOpts) of
            {async, Runner} ->
                case Runner() of
                    {ok, Hits} -> {ok, keys(Hits)};
                    {error, _Reason} -> error
                end;
            {error, _Reason} ->
                error
        end,
    case ActualResult of
        ExpectedResult ->
            ok;
        _ ->
            erlang:error({sqlite_diff_mismatch, Query, ExpectedResult, ActualResult})
    end.

sqlite_expected_results(RootPath, Docs, Queries) ->
    ScriptPath = filename:join(RootPath, "sqlite_fts_diff.py"),
    ok = file:write_file(ScriptPath, sqlite_diff_script(Docs, Queries)),
    parse_sqlite_diff_output(os:cmd("python3 " ++ ScriptPath)).

sqlite_diff_script(Docs, Queries) ->
    DocRows =
        lists:join(
            ",\n",
            [
                io_lib:format(
                    "    (s('~s'), b('~s'), b('~s'))",
                    [b64(Key), b64(Title), b64(Body)]
                )
             || {Key, Title, Body} <- Docs
            ]
        ),
    QueryRows =
        lists:join(
            ",\n",
            [io_lib:format("    s('~s')", [b64(Query)]) || Query <- Queries]
        ),
    iolist_to_binary([
        "import base64\n"
        "import sqlite3\n"
        "\n"
        "def s(value):\n"
        "    return base64.b64decode(value).decode('utf-8')\n"
        "\n"
        "def b(value):\n"
        "    return base64.b64decode(value)\n"
        "\n"
        "def enc(value):\n"
        "    return base64.b64encode(value.encode('utf-8')).decode('ascii')\n"
        "\n"
        "docs = [\n",
        DocRows,
        "\n]\n"
        "queries = [\n",
        QueryRows,
        "\n]\n"
        "conn = sqlite3.connect(':memory:')\n"
        "conn.execute(\"CREATE VIRTUAL TABLE docs USING fts5(key UNINDEXED, title, body, "
        "tokenize='unicode61 remove_diacritics 2', prefix='2 3 5')\")\n"
        "conn.executemany('INSERT INTO docs(key, title, body) VALUES(?, ?, ?)', docs)\n"
        "for query in queries:\n"
        "    try:\n"
        "        keys = [row[0] for row in conn.execute("
        "'SELECT key FROM docs WHERE docs MATCH ? ORDER BY key', (query,))]\n"
        "        print('OK\\t%s\\t%s' % (enc(query), ','.join(enc(key) for key in keys)))\n"
        "    except Exception as exc:\n"
        "        print('ERR\\t%s\\t%s' % (enc(query), enc(str(exc))))\n"
    ]).

b64(Bin) ->
    binary_to_list(base64:encode(Bin)).

parse_sqlite_diff_output(Output0) ->
    Output = unicode:characters_to_binary(Output0, utf8),
    Lines =
        [
            Line
         || Line <- binary:split(Output, <<"\n">>, [global]),
            Line =/= <<>>
        ],
    maps:from_list([parse_sqlite_diff_line(Line) || Line <- Lines]).

parse_sqlite_diff_line(Line) ->
    case binary:split(Line, <<"\t">>, [global]) of
        [<<"OK">>, EncodedQuery, EncodedKeys] ->
            {base64:decode(EncodedQuery), {ok, parse_sqlite_diff_keys(EncodedKeys)}};
        [<<"ERR">>, EncodedQuery, _EncodedError] ->
            {base64:decode(EncodedQuery), error};
        _Other ->
            erlang:error({bad_sqlite_diff_output, Line})
    end.

parse_sqlite_diff_keys(<<>>) ->
    [];
parse_sqlite_diff_keys(EncodedKeys) ->
    [base64:decode(EncodedKey) || EncodedKey <- binary:split(EncodedKeys, <<",">>, [global])].

hit_keys(Hits) ->
    [maps:get(key, Hit) || Hit <- Hits].

write_fts_batches(Bookie, Ops, BatchSize) ->
    lists:foreach(
        fun(Batch) ->
            ok = leveled_bookie:book_ftsbatchput(Bookie, Batch)
        end,
        chunks(Ops, BatchSize)
    ).

chunks([], _BatchSize) ->
    [];
chunks(Ops, BatchSize) ->
    {Batch, Rest} = lists:split(min(BatchSize, length(Ops)), Ops),
    [Batch | chunks(Rest, BatchSize)].

scaled_doc_key(N) ->
    iolist_to_binary(io_lib:format("doc-~12..0B", [N])).

hot_doc_key(N) ->
    iolist_to_binary(io_lib:format("hot-~4..0B", [N])).

index_keys(Bookie, Bucket, Field, Term) ->
    Fold =
        fun(_Bucket, Key, Acc) ->
            [Key | Acc]
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Bookie,
            {Bucket, null},
            {Fold, []},
            {Field, Term, Term},
            {false, undefined}
        ),
    lists:sort(Runner()).

segment_search_term_keys(Bookie, Bucket, Index, Token, Column) ->
    keys(search(Bookie, Bucket, Index, Token, #{columns => [Column], rank => none})).

segment_search_prefix_keys(Bookie, Bucket, Index, PrefixLen, Prefix, _Token, Column) ->
    keys(
        search(
            Bookie,
            Bucket,
            Index,
            <<Prefix/binary, "*">>,
            #{columns => [Column], prefixes => [PrefixLen], rank => none}
        )
    ).

segment_term_present(Bookie, Bucket, Index, Token, Column) ->
    [] =/= index_terms(
            Bookie,
            Bucket,
            {fts_segment, Index},
            {Token, {0, Column}, {0, 0, 0, 0}},
            {Token, {0, Column}, {1, 0, 0, 0}}
        ).

segment_doc_delete_present(Bookie, Bucket, Index, ExpectedToken, ExpectedColumn, Key) ->
    lists:any(
        fun
            ({{TermToken, {0, TermColumn}, {doc_range, {0, FirstDoc, LastDoc, PayloadId}}},
                _ObjKey}) when
                TermToken =:= ExpectedToken,
                TermColumn =:= ExpectedColumn,
                is_binary(FirstDoc),
                is_binary(LastDoc),
                is_binary(PayloadId)
            ->
                FirstDoc =< Key andalso Key =< LastDoc;
            (_Other) ->
                false
        end,
        index_terms(
            Bookie,
            Bucket,
            {fts_segment_doc_delete, Index},
            {ExpectedToken, {0, ExpectedColumn}, {doc_range, {0, 0, 0, 0}}},
            {ExpectedToken, {0, ExpectedColumn}, {doc_range, {1, 0, 0, 0}}}
        )
    ).

assert_user_change_has_doc_marker_only(Bookie, Bucket, Key, Index) ->
    IndexSpecs = current_user_indexspecs(Bookie, Bucket, Key),
    true =
        lists:any(
            fun
                ({idx_payload, add, {fts_doc, SpecIndex}, doc, _Payload}) when
                    SpecIndex =:= Index
                ->
                    true;
                (_Other) -> false
            end,
            IndexSpecs
        ),
    false = lists:any(fun is_fts_segment_payload_indexspec/1, IndexSpecs).

assert_hidden_segment_carrier(Bookie, Bucket, UserKey, Index, Token, Column) ->
    SegmentTerms =
        index_terms(
            Bookie,
            Bucket,
            {fts_segment, Index},
            {Token, {0, Column}, {0, 0, 0, 0}},
            {Token, {0, Column}, {1, 0, 0, 0}}
        ),
    true = SegmentTerms =/= [],
    true =
        lists:all(
            fun({_Term, ObjKey}) ->
                ObjKey =/= UserKey andalso is_hidden_segment_carrier_key(ObjKey)
            end,
            SegmentTerms
        ).

assert_hidden_delete_segment_carrier(Bookie, Bucket, UserKey, Index, Token, Column) ->
    DeleteTerms =
        index_terms(
            Bookie,
            Bucket,
            {fts_segment_doc_delete, Index},
            {Token, {0, Column}, {doc_range, {0, 0, 0, 0}}},
            {Token, {0, Column}, {doc_range, {1, 0, 0, 0}}}
        ),
    true = DeleteTerms =/= [],
    true =
        lists:all(
            fun({_Term, ObjKey}) ->
                ObjKey =/= UserKey andalso is_hidden_delete_segment_carrier_key(ObjKey)
            end,
            DeleteTerms
        ).

current_user_indexspecs(Bookie, Bucket, Key) ->
    {ok, _Object, SQN} = leveled_bookie:book_get_sqn(Bookie, Bucket, Key),
    {ok, Inker, _Penciller} = leveled_bookie:book_returnactors(Bookie),
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, ?STD_TAG),
    {{SQN, LedgerKey}, {_ObjectFromJournal, KeyChanges0}} =
        leveled_inker:ink_get(Inker, LedgerKey, SQN),
    {IndexSpecs, _TTL} = leveled_codec:unwrap_batch_keychanges(KeyChanges0),
    IndexSpecs.

is_fts_segment_payload_indexspec({idx_payload, _Op, {fts_segment, _Index}, _Term, _Payload}) ->
    true;
is_fts_segment_payload_indexspec(
    {idx_payload, _Op, {fts_segment_doc_delete, _Index}, _Term, _Payload}
) ->
    true;
is_fts_segment_payload_indexspec(_Spec) ->
    false.

is_hidden_segment_carrier_key(<<0, "$leveled_fts/seg/", _Rest/binary>>) ->
    true;
is_hidden_segment_carrier_key(_Key) ->
    false.

is_hidden_delete_segment_carrier_key(<<0, "$leveled_fts/del/", _Rest/binary>>) ->
    true;
is_hidden_delete_segment_carrier_key(_Key) ->
    false.

index_terms(Bookie, Bucket, Field, Start, End) ->
    Fold =
        fun(_Bucket, {Term, Key}, Acc) ->
            [{Term, Key} | Acc]
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Bookie,
            {Bucket, null},
            {Fold, []},
            {Field, Start, End},
            {true, undefined}
        ),
    lists:sort(Runner()).

not_written(Bookie, Bucket, Key, Index, Object) ->
    not_found = leveled_bookie:book_get(Bookie, Bucket, Key),
    assert_missing_fts_schema(Bookie, Bucket, Index, Object, #{}).

assert_segment_manifest_update(Bookie, Bucket, Index, Key, OldTerm, NewTerm) ->
    ok =
        leveled_bookie:book_ftsput(
            Bookie,
            Bucket,
            Key,
            <<"updated">>,
            Index,
            #{body => <<NewTerm/binary, " durable search">>},
            #{}
        ),
    [] = search(Bookie, Bucket, Index, OldTerm, #{}),
    [Key] = keys(search(Bookie, Bucket, Index, NewTerm, #{})).

assert_doc_marker_latest_wins() ->
    RootPath = testutil:reset_filestructure("fts_doc_marker_latest"),
    Bucket = <<"doc-marker-latest">>,
    Index = <<"main">>,
    Key = <<"same-key">>,
    {ok, Bookie1} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1, Bucket, Key, <<"obj1">>, Index, #{body => <<"old marker">>}, #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1, Bucket, Key, <<"obj2">>, Index, #{body => <<"new marker">>}, #{}
        ),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, Bookie2} = leveled_bookie:book_start(start_opts(RootPath)),
    {active, 2, false} = doc_marker_payload_state(Bookie2, Bucket, Index, Key),
    [] = search(Bookie2, Bucket, Index, <<"old">>, #{}),
    [Key] = keys(search(Bookie2, Bucket, Index, <<"new">>, #{})),
    ok = leveled_bookie:book_ftsdelete(Bookie2, Bucket, Key, Index, #{}),
    ok = leveled_bookie:book_close(Bookie2),

    {ok, Bookie3} = leveled_bookie:book_start(start_opts(RootPath)),
    not_found = doc_marker_payload_state(Bookie3, Bucket, Index, Key),
    [] = search(Bookie3, Bucket, Index, <<"new">>, #{}),
    ok =
        leveled_bookie:book_ftsput(
            Bookie3, Bucket, Key, <<"obj3">>, Index, #{body => <<"again marker">>}, #{}
        ),
    ok = leveled_bookie:book_close(Bookie3),

    {ok, Bookie4} = leveled_bookie:book_start(start_opts(RootPath)),
    {active, 3, false} = doc_marker_payload_state(Bookie4, Bucket, Index, Key),
    [] = search(Bookie4, Bucket, Index, <<"old">>, #{}),
    [] = search(Bookie4, Bucket, Index, <<"new">>, #{}),
    [Key] = keys(search(Bookie4, Bucket, Index, <<"again">>, #{})),
    ok = leveled_bookie:book_close(Bookie4).

doc_marker_payload_state(Bookie, Bucket, Index, Key) ->
    case leveled_bookie:book_get_sqn(Bookie, Bucket, Key) of
        {ok, _Object, SQN} ->
            {ok, Inker, _Penciller} = leveled_bookie:book_returnactors(Bookie),
            LedgerKey = leveled_codec:to_objectkey(Bucket, Key, ?STD_TAG),
            case leveled_inker:ink_get(Inker, LedgerKey, SQN) of
                {{SQN, LedgerKey}, {_ObjectFromJournal, KeyChanges0}} ->
                    case leveled_fts:manifest_from_keychanges(KeyChanges0) of
                        {ok, Manifest} ->
                            Index = maps:get(index, Manifest),
                            {
                                active,
                                maps:get(generation, Manifest),
                                maps:get(manifest_deleted, Manifest, false)
                            };
                        not_found ->
                            not_found;
                        {error, Reason} ->
                            {error, Reason}
                    end;
                _Other ->
                    not_found
            end;
        not_found ->
            not_found
    end.

assert_retain_compacted_fts_replay() ->
    RootPath = testutil:reset_filestructure("fts_retain_compacted"),
    Opts =
        start_opts(RootPath) ++
            [
                {max_journalsize, 1000000},
                {max_run_length, 1},
                {cache_size, 1},
                {reload_strategy, [{?STD_TAG, retain}]},
                {journalcompaction_scoreonein, 1},
                {singlefile_compactionpercentage, 100.0},
                {maxrunlength_compactionpercentage, 100.0}
            ],
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"retain-compact">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"old retained payload">>},
            #{}
        ),
    write_retain_compact_fillers(Bookie1),
    {ok, Inker1, _Penciller1} = leveled_bookie:book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker1),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"retain-compact">>,
            <<"2">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"deleted retained payload">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsput(
            Bookie1,
            <<"retain-compact">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"live retained payload">>},
            #{}
        ),
    ok =
        leveled_bookie:book_ftsdelete(
            Bookie1, <<"retain-compact">>, <<"2">>, <<"main">>, #{}
        ),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, BookieCompactor} = leveled_bookie:book_start(Opts),
    ok = leveled_bookie:book_compactjournal(BookieCompactor, 30000),
    testutil:wait_for_compaction(BookieCompactor),
    true = fts_payload_keydelta_retained(BookieCompactor),
    [<<"1">>] =
        keys(search(BookieCompactor, <<"retain-compact">>, <<"main">>, <<"live">>, #{})),
    [] = search(BookieCompactor, <<"retain-compact">>, <<"main">>, <<"old">>, #{}),
    [] = search(BookieCompactor, <<"retain-compact">>, <<"main">>, <<"deleted">>, #{}),
    ok = leveled_bookie:book_close(BookieCompactor),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    [<<"1">>] =
        keys(search(Bookie2, <<"retain-compact">>, <<"main">>, <<"live">>, #{})),
    [] = search(Bookie2, <<"retain-compact">>, <<"main">>, <<"old">>, #{}),
    [] = search(Bookie2, <<"retain-compact">>, <<"main">>, <<"deleted">>, #{}),
    assert_segment_manifest_update(
        Bookie2,
        <<"retain-compact">>,
        <<"main">>,
        <<"1">>,
        <<"live">>,
        <<"replayed">>
    ),
    ok = leveled_bookie:book_close(Bookie2).

write_retain_compact_fillers(Bookie) ->
    lists:foreach(
        fun(N) ->
            Key = integer_to_binary(N),
            ok =
                leveled_bookie:book_put(
                    Bookie,
                    <<"retain-compact-fill">>,
                    Key,
                    <<"filler">>,
                    [],
                    ?STD_TAG
                )
        end,
        lists:seq(1, 160)
    ).

fts_payload_keydelta_retained(Bookie) ->
    {ok, Inker, _Penciller} = leveled_bookie:book_returnactors(Bookie),
    Manifest = leveled_inker:ink_getmanifest(Inker),
    lists:any(
        fun
            ({_LowSQN, _Filename, _JournalPid, empty}) ->
                false;
            ({_LowSQN, _Filename, JournalPid, _LastKey}) ->
                Positions = leveled_cdb:cdb_getpositions(JournalPid, all),
                Entries = leveled_cdb:cdb_directfetch(
                    JournalPid, Positions, key_value_check
                ),
                lists:any(fun retained_fts_keydelta_entry/1, Entries)
        end,
        Manifest
    ).

retained_fts_keydelta_entry({{_SQN, ?INKT_KEYD, _LedgerKey}, JournalBin, _Check})
    when is_binary(JournalBin)
->
    case leveled_codec:revert_value_from_journal(JournalBin) of
        {null, KeyChanges0} -> keychanges_have_fts_payload(KeyChanges0);
        {_Object, KeyChanges0} -> keychanges_have_fts_payload(KeyChanges0)
    end;
retained_fts_keydelta_entry({{_SQN, ?INKT_KEYD, _LedgerKey}, {null, KeyChanges0}, _Check}) ->
    keychanges_have_fts_payload(KeyChanges0);
retained_fts_keydelta_entry(_Other) ->
    false.

keychanges_have_fts_payload(KeyChanges0) ->
    {IndexSpecs, _TTL} = leveled_codec:unwrap_batch_keychanges(KeyChanges0),
    lists:any(fun is_fts_payload_indexspec/1, IndexSpecs).

is_fts_payload_indexspec({idx_payload, _IdxOp, {fts_doc, _Index}, _IdxTerm, _Payload}) ->
    true;
is_fts_payload_indexspec({idx_payload, _IdxOp, {fts_schema, _Index}, _IdxTerm, _Payload}) ->
    true;
is_fts_payload_indexspec({idx_payload, _IdxOp, {fts_segment, _Index}, _IdxTerm, _Payload}) ->
    true;
is_fts_payload_indexspec({idx_payload, _IdxOp, {fts_segment_doc_delete, _Index}, _IdxTerm, _Payload}) ->
    true;
is_fts_payload_indexspec(_Other) ->
    false.
