%% -------- Full-text search: a pure client library ---------
%%
%% leveled_fts consumes ONLY the public store surface (docs/FTS.md):
%% book_mput/book_casmput/book_sqn/book_headonly/folds/snapshots. The
%% store carries no FTS hooks; all index state is ordinary HEAD_TAG
%% object-spec rows committed in the caller's own batches. Mutable document
%% deltas are consolidated into immutable, generation-qualified FTS2 planes.

-module(leveled_fts).

-include("leveled.hrl").


-export([
    schema/1,
    capacities/0,
    tokenize_with_offsets/2,
    derive/3,
    remove/3,
    update/4,
    prepare/3,
    search/4,
    posting_read/5,
    text_blocks/5,
    text_blocks_batch/3,
    text_blocks_with_offsets_batch/3,
    document_text/3,
    document_text_batch/3,
    record_fetch/4,
    candidate_fetch/4,
    hydrate_page/3,
    consolidate/3
]).

-ifdef(TEST).
-export([
    tokenize_fold_for_test/4,
    fts2_available/2,
    fts2_codec_term_key/3,
    fts2_codec_identity_key/1,
    fts2_codec_encode_plane/1,
    fts2_codec_decode_plane/1,
    fts2_codec_encode_positions/1,
    fts2_codec_decode_positions/1,
    fts2_codec_encode_identity_page/1,
    fts2_codec_decode_identity_page/2,
    fts2_phrase_fast/7,
    fts2_build_parallel_for_test/4,
    fts2_build_parallel_for_test/5,
    fts2_build_parallel_for_test/6,
    fts2_outer_bound_for_test/3,
    fts2_legacy_outer_assemble_for_test/2,
    fts2_consolidate_with_heap_for_test/4,
    fts2_consolidate_with_heap_for_test/5,
    fts2_consolidate_with_shards_for_test/6
]).
-endif.

-define(DEFAULT_LIMIT, 10000).
-define(MAX_LIMIT, 20000).
-define(MAX_WINDOW, 20000).
-define(MAX_QUERY_BYTES, 4096).
-define(MAX_QUERY_TOKENS, 128).
-define(MAX_AST_DEPTH, 32).
-define(MAX_NEAR_DISTANCE, 64).
-define(MAX_PREFIX_BYTES, 64).
-define(DEFAULT_NEAR, 10).
-define(TERM_BLOOM_BITS, (1 bsl 20)).
-define(TERM_BLOOM_WORDS, (?TERM_BLOOM_BITS div 64)).
-define(TERM_BLOOM_SHARDS, 256).
-define(TERM_BLOOM_SHARD_BITS, (1 bsl 13)).
-define(TERM_BLOOM_SHARD_WORDS, (?TERM_BLOOM_SHARD_BITS div 64)).
-define(BIGRAM_BLOOM_SHARDS, 256).
-define(BIGRAM_BLOOM_SHARD_BITS, (1 bsl 16)).
-define(BIGRAM_BLOOM_SHARD_WORDS, (?BIGRAM_BLOOM_SHARD_BITS div 64)).

%% Search candidates stay in one flat tuple until the final requested page is
%% hydrated.  The two association lists are query-sized (not candidate maps):
%% term_stats holds {Term, Tf, Source, Df}, while columns and match_positions
%% hold {Key, Positions} pairs.
-record(fts2_match, {
    chunk_id,
    group_id = undefined,
    source_id,
    doc_length = 0,
    tf = 0,
    term_stats = [],
    terms = [],
    columns = [],
    match_positions = [],
    match_count = undefined,
    logical_group = undefined,
    group_version = 0,
    group_match_count = undefined,
    delta_document = undefined,
    score = 0.0
}).

%% Client-library wire format capacities.  These are deliberately exported by
%% capacities/0 and consumed by schema/1.  Every fixed-width writer below also
%% checks the same bound immediately before constructing a bit syntax.
-define(MANIFEST_VERSION, 5).
-define(LEGACY_TEXT_BLOCK_VERSION, 2).
-define(PREVIOUS_TEXT_BLOCK_VERSION, 3).
-define(VARINT_TEXT_BLOCK_VERSION, 4).
-define(TEXT_BLOCK_VERSION, 5).
-define(TEXT_BLOCK_OFFSET_CHUNK, 32).
-define(TEXT_BLOCK_OFFSET_DIR_STRIDE, 8).
% "TBP"
-define(TEXT_PACK_MAGIC, 16#544250).
-define(PREVIOUS_TEXT_PACK_VERSION, 1).
-define(TEXT_PACK_VERSION, 2).
-define(TEXT_PACK_DIR_STRIDE, 16).
-define(TEXT_BLOCK_BYTES, 4096).
-define(MAX_HIT_RECORD_BYTES, 1280).
-define(TRANSIENT_DOC_ID_BIT, 16#8000000000000000).
-define(MAX_COLUMNS, 255).
-define(MAX_COLUMN_ID, 254).
-define(MAX_TOKEN_BYTES, 65535).
-define(MAX_POSITION_BYTES, 65535).
-define(MAX_DOC_KEY_BYTES, 65533).
-define(MAX_SHARDS, 65536).
-define(DEFAULT_SHARDS, 256).
-define(MAX_U16, 16#FFFF).
-define(MAX_U32, 16#FFFFFFFF).
-define(MAX_U64, 16#FFFFFFFFFFFFFFFF).
-define(MAX_DOC_ID, ?MAX_U64).

%% ---------------------------------------------------------------------------
%% Pure client API (docs/FTS.md).
%%
%% All persisted state is made from ordinary HEAD_TAG object specs.  The store
%% has no FTS callback, configuration, or privileged payload.  The logical
%% Search resolves the current FTS2 root and merges its immutable planes with
%% the exhaustive document-major delta set. A configured store may hold one
%% immutable generation resident through leveled_fts_residency; that state is
%% built as a bounded unit, never populated by serving reads, and is discarded
%% at the same FTS state/root transition that supersedes the generation.
%% ---------------------------------------------------------------------------

-spec capacities() -> map().
capacities() ->
    #{
        columns => ?MAX_COLUMNS,
        column_id => ?MAX_COLUMN_ID,
        token_bytes => ?MAX_TOKEN_BYTES,
        position_bytes => ?MAX_POSITION_BYTES,
        doc_key_bytes => ?MAX_DOC_KEY_BYTES,
        doc_id => ?MAX_DOC_ID,
        shards => ?MAX_SHARDS,
        true_occurrences => ?MAX_U64
    }.

-spec schema(map()) -> {ok, map()} | {error, term()}.
schema(Definition) when is_map(Definition) ->
    try
        Index = normalise_index(maps:get(index, Definition)),
        true = is_binary(Index) andalso Index =/= <<>>,
        {ok, ColumnSpecs} = normalise_column_specs(
            maps:get(columns, Definition)
        ),
        ColumnCount = length(ColumnSpecs),
        client_guard(columns, ColumnCount, maps:get(columns, capacities())),
        Shards = maps:get(shards, Definition, ?DEFAULT_SHARDS),
        true =
            is_integer(Shards) andalso Shards > 0 andalso
                Shards =< maps:get(shards, capacities()) andalso
                (Shards band (Shards - 1)) =:= 0,
        Opts0 = maps:with(
            [
                tokenizer,
                remove_diacritics,
                tokenchars,
                separators,
                stopwords,
                decode,
                prefixes
            ],
            Definition
        ),
        true = valid_tokenizer(maps:get(tokenizer, Opts0, unicode61)),
        true = valid_remove_diacritics(
            maps:get(remove_diacritics, Opts0, 1)
        ),
        true = valid_char_option(maps:get(tokenchars, Opts0, [])),
        true = valid_char_option(maps:get(separators, Opts0, [])),
        Opts1 = normalise_options(
            Opts0#{
                remove_diacritics => client_rd_mode(
                    maps:get(remove_diacritics, Opts0, 1)
                )
            }
        ),
        Columns = [Column || {Column, _Path, _Mode} <- ColumnSpecs],
        HitFields = normalise_hit_fields(
            maps:get(hit_fields, Definition, [])
        ),
        CandidateFields = normalise_hit_fields(
            maps:get(candidate_fields, Definition, [])
        ),
        CandidateFilterFields = normalise_candidate_filter_fields(
            maps:get(candidate_filter_fields, Definition, [])
        ),
        CandidateGroupFields = normalise_field_names(
            maps:get(candidate_group_fields, Definition, [])
        ),
        CandidateVersionField = normalise_optional_field_name(
            maps:get(candidate_version_field, Definition, undefined)
        ),
        CandidateNames = [Name || {Name, _Path} <- CandidateFields],
        true = lists:all(
            fun(Name) -> lists:member(Name, CandidateNames) end,
            CandidateGroupFields ++
                case CandidateVersionField of
                    undefined -> [];
                    Name -> [Name]
                end
        ),
        TextPath = normalise_text_path(
            maps:get(text_field, Definition, undefined)
        ),
        Canonical = #{
            index => Index,
            columns => Columns,
            column_specs => ColumnSpecs,
            column_modes => maps:from_list([
                {Column, Mode}
             || {Column, _Path, Mode} <- ColumnSpecs
            ]),
            options => Opts1,
            tokenizer => tokenizer_description(Opts1),
            prefixes => maps:get(prefixes, Opts1, []),
            hit_fields => HitFields,
            candidate_fields => CandidateFields,
            candidate_filter_fields => CandidateFilterFields,
            candidate_group_fields => CandidateGroupFields,
            candidate_version_field => CandidateVersionField,
            text_path => TextPath,
            text_block_bytes => ?TEXT_BLOCK_BYTES,
            shards => Shards
        },
        Fingerprint = crypto:hash(
            sha256, term_to_binary(Canonical, [deterministic])
        ),
        {ok, Canonical#{fingerprint => Fingerprint}}
    catch
        error:{fts_capacity_exceeded, _, _, _} = Reason -> {error, Reason};
        _:_ -> {error, invalid_fts_schema}
    end;
schema(_Definition) ->
    {error, invalid_fts_schema}.

client_rd_mode(false) -> 0;
client_rd_mode(true) -> 1;
client_rd_mode(Mode) -> Mode.

-spec derive(map(), binary(), term()) -> {ok, [leveled_codec:object_spec()]}.
derive(Schema, DocKey, Object) ->
    client_derive(Schema, DocKey, Object, none, none).

client_derive(
    #{fingerprint := Fingerprint} = Schema,
    DocKey,
    Object,
    BaseLength,
    OldManifest
) when
    is_binary(DocKey), is_binary(Fingerprint)
->
    client_guard(
        doc_key_bytes, byte_size(DocKey), maps:get(doc_key_bytes, capacities())
    ),
    Decoded = maybe_decode_object(Object, Schema),
    Fields = extract_fields(Decoded, maps:get(column_specs, Schema)),
    ColTerms = build_column_terms(Fields, maps:get(options, Schema)),
    Text = client_extract_text(Decoded, Schema),
    Blocks = client_text_blocks(Text),
    {BlockDirectory, BlockOffsets} = client_text_structure(
        Text, maps:get(options, Schema)
    ),
    HitRecord = client_hit_record(
        DocKey, Decoded, Schema, BlockDirectory, byte_size(Text)
    ),
    CandidateRecord = client_candidate_record(Decoded, Schema),
    ResidentCandidate = CandidateRecord#{
        '$fts_text_blocks' => BlockDirectory,
        '$fts_text_bytes' => byte_size(Text)
    },
    {ByShard, DocLength} = client_group_postings(ColTerms, Schema),
    Touched = lists:sort(maps:keys(ByShard)),
    Bucket = maps:get(index, Schema),
    %% content-derived version stamp: pure, idempotent (an identical
    %% reindex is version-stable), shared by every row of this batch
    DocVersion =
        binary:part(
            erlang:md5(
                term_to_binary(
                    {
                        ByShard,
                        DocLength,
                        HitRecord,
                        CandidateRecord,
                        crypto:hash(sha256, Text)
                    },
                    [deterministic]
                )
            ),
            0,
            8
        ),
    DocId = client_doc_id(DocKey, DocVersion),
    RetiredIds =
        case OldManifest of
            #{doc_id := DocId} -> [];
            #{doc_id := OldDocId} -> [OldDocId];
            _ -> []
        end,
    Manifest =
        client_encode_manifest(
            DocVersion,
            DocId,
            Touched,
            DocLength,
            BaseLength,
            Fingerprint,
            length(Blocks)
        ),
    BlockSpecs = client_text_block_specs(Bucket, DocId, Blocks, BlockOffsets),
    WriteShards = client_write_shards(Touched),
    {ok,
        BlockSpecs ++
            client_tail_presence_specs(Bucket, ByShard, RetiredIds) ++
            [
                {add, Bucket, <<"doc">>, DocKey, Manifest},
                client_fts2_delta_spec(
                    Bucket,
                    DocId,
                    #{
                        status => live,
                        source_id => DocId,
                        retired_ids => RetiredIds,
                        doc_key => DocKey,
                        doc_version => DocVersion,
                        doc_length => DocLength,
                        base_length => BaseLength,
                        posting => client_full_posting(ByShard),
                        candidate_record => ResidentCandidate,
                        hit_record => HitRecord
                    }
                ),
                client_fts2_state_dirty_spec(Bucket),
                client_record_tail_dirty_spec(Bucket),
                client_stats_dirty_spec(Bucket)
            ] ++
            [client_epoch_spec(Bucket, Shard, DocId) || Shard <- WriteShards]};
client_derive(_Schema, DocKey, _Object, _BaseLength, _OldManifest) ->
    erlang:error({invalid_fts_derive, DocKey}).

-spec remove(map(), binary(), binary() | map()) ->
    [leveled_codec:object_spec()].
remove(#{index := Bucket, fingerprint := Fingerprint}, DocKey, Manifest0) when
    is_binary(DocKey)
->
    #{
        version := DocVersion,
        doc_id := DocId,
        shards := Shards,
        doc_length := DocLength,
        fingerprint := Fingerprint
    } =
        Manifest = client_decode_manifest_value(Manifest0),
    BaseLength = maps:get(base_length, Manifest, none),
    BlockCount = maps:get(block_count, Manifest, 0),
    [
            {remove, Bucket, <<"doc">>, DocKey, <<>>},
            client_fts2_delta_spec(
                Bucket,
                DocId,
                #{
                    status => remove,
                    source_id => DocId,
                    retired_ids => [],
                    doc_key => DocKey,
                    doc_version => DocVersion,
                    doc_length => DocLength,
                    base_length => BaseLength
                }
            ),
            client_fts2_state_dirty_spec(Bucket),
            client_record_tail_dirty_spec(Bucket),
            client_stats_dirty_spec(Bucket),
            client_tail_mutation_spec(Bucket)
        ] ++
        client_remove_text_block_specs(Bucket, DocId, BlockCount) ++
        [
            client_epoch_spec(Bucket, Shard, DocId)
         || Shard <- client_write_shards(Shards)
        ].

-spec update(map(), binary(), term(), binary() | map()) ->
    {ok, [leveled_codec:object_spec()]}.
update(Schema, DocKey, Object, OldManifest) ->
    Old = client_decode_manifest_value(OldManifest),
    BaseLength = maps:get(base_length, Old, none),
    {ok, NewSpecs} = client_derive(
        Schema, DocKey, Object, BaseLength, Old
    ),
    {ok, client_dedupe_specs(remove(Schema, DocKey, OldManifest) ++ NewSpecs)}.

client_dedupe_specs(Specs) ->
    {_Seen, Kept} = lists:foldl(
        fun({_, B, K, SK, _} = Spec, {Seen, Acc}) ->
            Id = {B, K, SK},
            case sets:is_element(Id, Seen) of
                true -> {Seen, Acc};
                false -> {sets:add_element(Id, Seen), [Spec | Acc]}
            end
        end,
        {sets:new(), []},
        lists:reverse(Specs)
    ),
    Kept.

client_full_posting(ByShard) ->
    maps:fold(
        fun(_Shard, ByColumn, Acc) ->
            maps:fold(
                fun(Column, Tokens, Inner) ->
                    Inner#{Column => maps:merge(maps:get(Column, Inner, #{}), Tokens)}
                end,
                Acc,
                ByColumn
            )
        end,
        #{},
        ByShard
    ).

client_group_postings(ColTerms, Schema) ->
    lists:foldl(
        fun({ColId, {_Column, TokenPositions}}, {ShardAcc, LengthAcc}) ->
            client_guard(column_id, ColId, maps:get(column_id, capacities())),
            lists:foldl(
                fun({Token, Positions}, {SA, LA}) ->
                    client_guard(
                        token_bytes,
                        byte_size(Token),
                        maps:get(token_bytes, capacities())
                    ),
                    Count = length(Positions),
                    client_guard(
                        true_occurrences,
                        Count,
                        maps:get(true_occurrences, capacities())
                    ),
                    Shard = client_shard_id(Token, maps:get(shards, Schema)),
                    ByCol = maps:get(Shard, SA, #{}),
                    ByToken = maps:get(ColId, ByCol, #{}),
                    Entry = #{count => Count, positions => Positions},
                    {
                        SA#{Shard => ByCol#{ColId => ByToken#{Token => Entry}}},
                        LA + Count
                    }
                end,
                {ShardAcc, LengthAcc},
                TokenPositions
            )
        end,
        {#{}, 0},
        lists:zip(lists:seq(0, length(ColTerms) - 1), ColTerms)
    ).

client_doc_id_subkey(DocId) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    <<DocId:64/unsigned-big>>.

client_doc_id(DocKey, DocVersion) ->
    <<RawId:64/unsigned-big, _/binary>> = crypto:hash(
        sha256,
        <<
            (byte_size(DocKey)):16/unsigned-big,
            DocKey/binary,
            DocVersion/binary
        >>
    ),
    RawId bor ?TRANSIENT_DOC_ID_BIT.

client_transient_doc_id(DocId) ->
    (DocId band ?TRANSIENT_DOC_ID_BIT) =/= 0.

client_fts2_delta_spec(Bucket, DocId, Delta) ->
    {add, Bucket, <<"f2:d">>, client_doc_id_subkey(DocId),
        fts2_codec_encode(delta, Delta)}.

%% A dirty tail remains document-major, but a compact union of 16-bit term
%% hashes proves the overwhelmingly common zero-tail-match case without a
%% fold.  Collisions only cause a conservative full scan.  Markers are never
%% removed by document updates, so they cannot introduce false negatives.
client_tail_presence_specs(Bucket, ByShard, RetiredIds) ->
    Posting = client_full_posting(ByShard),
    Hashes = lists:usort(lists:append([
        [erlang:crc32(Token) band 16#FFFF || Token <- maps:keys(Tokens)]
     || Tokens <- maps:values(Posting)
    ])),
    [{add, Bucket, <<"f2:p">>, <<"ready">>, <<1>>}] ++
        case RetiredIds of
            [] -> [];
            _ -> [client_tail_mutation_spec(Bucket)]
        end ++
        [
            {add, Bucket, <<"f2:p">>, <<Hash:16/unsigned-big>>, <<1>>}
         || Hash <- Hashes
        ].

client_tail_mutation_spec(Bucket) ->
    {add, Bucket, <<"f2:p">>, <<"mutated">>, <<1>>}.

client_fts2_state_dirty_spec(Bucket) ->
    {add, Bucket, <<"f2:state">>, <<"current">>, <<0>>}.

client_record_tail_dirty_spec(Bucket) ->
    {add, Bucket, <<"record-tail">>, <<"dirty">>, <<1>>}.

client_text_block_key(DocId) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    <<"b:", DocId:64/unsigned-big>>.

client_text_block_specs(Bucket, DocId, Blocks, BlockOffsets) ->
    Key = client_text_block_key(DocId),
    case Blocks of
        [] ->
            [];
        _ ->
            [
                {add, Bucket, Key, <<0:32/unsigned-big>>,
                    client_encode_text_pack(Blocks, BlockOffsets)}
            ]
    end.

client_remove_text_block_specs(Bucket, DocId, BlockCount) ->
    Key = client_text_block_key(DocId),
    [
        {remove, Bucket, Key, <<BlockNo:32/unsigned-big>>, <<>>}
     || BlockNo <- lists:seq(0, erlang:max(BlockCount - 1, 0))
    ].

client_encode_text_pack(Blocks, BlockOffsets) ->
    {Directory0, Payload0, _Offset} = lists:foldl(
        fun({BlockNo, Block}, {Directory, Payload, Offset}) ->
            {OffsetDirectory, EncodedOffsets} =
                client_encode_seekable_text_block_offsets(
                    maps:get(BlockNo, BlockOffsets, [])
                ),
            Raw = <<
                (byte_size(Block)):32/unsigned-big,
                Block/binary,
                (byte_size(OffsetDirectory)):32/unsigned-big,
                OffsetDirectory/binary,
                EncodedOffsets/binary
            >>,
            Compressed = client_zstd_compress(Raw),
            Row =
                <<BlockNo:32/unsigned-big, Offset:32/unsigned-big,
                    (byte_size(Compressed)):32/unsigned-big,
                    (byte_size(Raw)):32/unsigned-big>>,
            {
                [Row | Directory],
                [Compressed | Payload],
                Offset + byte_size(Compressed)
            }
        end,
        {[], [], 0},
        Blocks
    ),
    Directory = iolist_to_binary(lists:reverse(Directory0)),
    Payload = iolist_to_binary(lists:reverse(Payload0)),
    <<?TEXT_PACK_MAGIC:24/unsigned-big, ?TEXT_PACK_VERSION:8,
        (length(Blocks)):32/unsigned-big,
        (byte_size(Directory)):32/unsigned-big, Directory/binary,
        Payload/binary>>.

client_decode_text_block_with_offsets(
    <<?TEXT_BLOCK_VERSION:8, _RawBytes:32/unsigned-big, Compressed/binary>>
) ->
    case client_zstd_decompress(Compressed) of
        <<BlockBytes:32/unsigned-big, Block:BlockBytes/binary,
            OffsetDirectoryBytes:32/unsigned-big,
            OffsetDirectory:OffsetDirectoryBytes/binary,
            EncodedOffsets/binary>> when
            OffsetDirectoryBytes rem ?TEXT_BLOCK_OFFSET_DIR_STRIDE =:= 0
        ->
            {Block,
                client_decode_seekable_text_block_offsets(
                    OffsetDirectory, EncodedOffsets, all
                )};
        Encoded when is_binary(Encoded) ->
            client_decode_term_text_block(Encoded, offsets);
        error ->
            erlang:error({invalid_fts_text_block, compressed})
    end;
client_decode_text_block_with_offsets(
    <<?VARINT_TEXT_BLOCK_VERSION:8, _RawBytes:32/unsigned-big,
        Compressed/binary>>
) ->
    case client_zstd_decompress(Compressed) of
        <<BlockBytes:32/unsigned-big, Block:BlockBytes/binary,
            EncodedOffsets/binary>> ->
            {Block, client_decode_text_block_offsets(EncodedOffsets)};
        error ->
            erlang:error({invalid_fts_text_block, compressed})
    end;
client_decode_text_block_with_offsets(
    <<?PREVIOUS_TEXT_BLOCK_VERSION:8, Compressed/binary>>
) ->
    case client_zstd_decompress(Compressed) of
        <<BlockBytes:32/unsigned-big, Block:BlockBytes/binary,
            EncodedOffsets/binary>> ->
            {Block, client_decode_text_block_offsets(EncodedOffsets)};
        Encoded when is_binary(Encoded) ->
            client_decode_term_text_block(Encoded, offsets);
        error ->
            erlang:error({invalid_fts_text_block, compressed})
    end;
client_decode_text_block_with_offsets(
    <<?LEGACY_TEXT_BLOCK_VERSION:8, Compressed/binary>>
) ->
    case client_zstd_decompress(Compressed) of
        Block when is_binary(Block) -> {Block, unavailable};
        error -> erlang:error({invalid_fts_text_block, compressed})
    end;
client_decode_text_block_with_offsets(Bad) ->
    erlang:error({invalid_fts_text_block, Bad}).

client_decode_term_text_block(Encoded, Mode) ->
    case binary_to_term(Encoded, [safe]) of
        {Block, Offsets} when is_binary(Block), is_list(Offsets) ->
            case Mode of
                text -> Block;
                offsets -> {Block, Offsets}
            end;
        _Bad ->
            erlang:error({invalid_fts_text_block, payload})
    end.

client_zstd_compress(Binary) when is_binary(Binary) ->
    iolist_to_binary(zstd:compress(Binary)).

client_zstd_decompress(Binary) when is_binary(Binary) ->
    try zstd:decompress(Binary) of
        error -> error;
        Output -> iolist_to_binary(Output)
    catch
        error:{zstd_error, _Reason} -> error
    end.

client_zstd_decompress_many([]) ->
    [];
client_zstd_decompress_many(Frames) ->
    Compressed = iolist_to_binary([Frame || {Frame, _Bytes} <- Frames]),
    case client_zstd_decompress(Compressed) of
        error ->
            error;
        Decoded ->
            client_split_zstd_frames(
                Decoded, [Bytes || {_Frame, Bytes} <- Frames], []
            )
    end.

client_split_zstd_frames(<<>>, [], Acc) ->
    lists:reverse(Acc);
client_split_zstd_frames(Binary, [Bytes | Rest], Acc) when
    byte_size(Binary) >= Bytes
->
    <<Frame:Bytes/binary, Tail/binary>> = Binary,
    client_split_zstd_frames(Tail, Rest, [Frame | Acc]);
client_split_zstd_frames(_Binary, _Sizes, _Acc) ->
    error.

client_encode_text_block_offsets(Offsets) ->
    {Encoded, _Ordinal, _Offset} = lists:foldl(
        fun({Ordinal, Offset, Length}, {Acc, PreviousOrdinal, PreviousOffset}) ->
            true = Ordinal >= PreviousOrdinal,
            true = Offset >= PreviousOffset,
            {
                [
                    Acc,
                    client_encode_uvarint(Ordinal - PreviousOrdinal),
                    client_encode_uvarint(Offset - PreviousOffset),
                    client_encode_uvarint(Length)
                ],
                Ordinal,
                Offset
            }
        end,
        {[], 0, 0},
        Offsets
    ),
    iolist_to_binary(Encoded).

client_encode_seekable_text_block_offsets(Offsets) ->
    Chunks = client_offset_chunks(Offsets, ?TEXT_BLOCK_OFFSET_CHUNK, []),
    {Directory0, Payload0, _PayloadBytes} = lists:foldl(
        fun(Chunk, {Directory, Payload, PayloadBytes}) ->
            [{FirstOrdinal, _FirstOffset, _FirstLength} | _] = Chunk,
            Encoded = client_encode_text_block_offsets(Chunk),
            {
                [
                    <<FirstOrdinal:32/unsigned-big,
                        PayloadBytes:32/unsigned-big>>
                    | Directory
                ],
                [Encoded | Payload],
                PayloadBytes + byte_size(Encoded)
            }
        end,
        {[], [], 0},
        Chunks
    ),
    {
        iolist_to_binary(lists:reverse(Directory0)),
        iolist_to_binary(lists:reverse(Payload0))
    }.

client_offset_chunks([], _Limit, Acc) ->
    lists:reverse(Acc);
client_offset_chunks(Offsets, Limit, Acc) ->
    {Chunk, Rest} = lists:split(erlang:min(Limit, length(Offsets)), Offsets),
    client_offset_chunks(Rest, Limit, [Chunk | Acc]).

client_decode_seekable_text_block_offsets(Directory, Encoded, all) ->
    lists:append([
        client_decode_text_block_offsets(Chunk)
     || Chunk <- client_seekable_offset_chunks(Directory, Encoded)
    ]);
client_decode_seekable_text_block_offsets(
    Directory, Encoded, WantedOrdinals
) when is_list(WantedOrdinals) ->
    lists:usort(
        lists:filtermap(
            fun(Ordinal) ->
                case
                    client_seekable_offset_chunk(Directory, Encoded, Ordinal)
                of
                    not_found ->
                        false;
                    Chunk ->
                        case
                            lists:keyfind(
                                Ordinal,
                                1,
                                client_decode_text_block_offsets(Chunk)
                            )
                        of
                            false -> false;
                            Offset -> {true, Offset}
                        end
                end
            end,
            WantedOrdinals
        )
    ).

client_seekable_offset_chunks(<<>>, _Encoded) ->
    [];
client_seekable_offset_chunks(Directory, Encoded) ->
    Entries = [
        {FirstOrdinal, Offset}
     || <<FirstOrdinal:32/unsigned-big, Offset:32/unsigned-big>> <= Directory
    ],
    client_seekable_offset_chunks(Entries, Encoded, []).

client_seekable_offset_chunks([], _Encoded, Acc) ->
    lists:reverse(Acc);
client_seekable_offset_chunks([{_Ordinal, Start}], Encoded, Acc) ->
    lists:reverse([
        binary:part(Encoded, Start, byte_size(Encoded) - Start) | Acc
    ]);
client_seekable_offset_chunks(
    [{_Ordinal, Start}, {_NextOrdinal, Finish} = Next | Rest], Encoded, Acc
) ->
    client_seekable_offset_chunks(
        [Next | Rest],
        Encoded,
        [binary:part(Encoded, Start, Finish - Start) | Acc]
    ).

client_seekable_offset_chunk(Directory, Encoded, Ordinal) ->
    Entries = [
        {FirstOrdinal, Offset}
     || <<FirstOrdinal:32/unsigned-big, Offset:32/unsigned-big>> <= Directory,
        FirstOrdinal =< Ordinal
    ],
    case lists:reverse(Entries) of
        [] ->
            not_found;
        [{_FirstOrdinal, Start} | _] ->
            Finish = client_next_offset_chunk_start(Directory, Start, Encoded),
            binary:part(Encoded, Start, Finish - Start)
    end.

client_next_offset_chunk_start(Directory, Start, Encoded) ->
    case
        [
            Offset
         || <<_FirstOrdinal:32/unsigned-big, Offset:32/unsigned-big>> <=
                Directory,
            Offset > Start
        ]
    of
        [Finish | _] -> Finish;
        [] -> byte_size(Encoded)
    end.

client_decode_text_block_offsets(Encoded) ->
    client_decode_text_block_offsets(Encoded, 0, 0, []).

client_decode_text_block_offsets(<<>>, _Ordinal, _Offset, Acc) ->
    lists:reverse(Acc);
client_decode_text_block_offsets(
    Encoded0, PreviousOrdinal, PreviousOffset, Acc
) ->
    {OrdinalDelta, Encoded1} = client_decode_uvarint(Encoded0),
    {OffsetDelta, Encoded2} = client_decode_uvarint(Encoded1),
    {Length, Rest} = client_decode_uvarint(Encoded2),
    Ordinal = PreviousOrdinal + OrdinalDelta,
    Offset = PreviousOffset + OffsetDelta,
    client_decode_text_block_offsets(
        Rest,
        Ordinal,
        Offset,
        [{Ordinal, Offset, Length} | Acc]
    ).

client_encode_uvarint(Value) when is_integer(Value), Value >= 0, Value < 128 ->
    <<Value>>;
client_encode_uvarint(Value) when is_integer(Value), Value >= 128 ->
    <<
        ((Value band 16#7F) bor 16#80),
        (client_encode_uvarint(Value bsr 7))/binary
    >>.

client_decode_uvarint(Encoded) ->
    client_decode_uvarint(Encoded, 0, 0).

client_decode_uvarint(<<Byte, Rest/binary>>, Shift, Acc) when Shift =< 63 ->
    Value = Acc bor ((Byte band 16#7F) bsl Shift),
    case Byte band 16#80 of
        0 -> {Value, Rest};
        _ -> client_decode_uvarint(Rest, Shift + 7, Value)
    end;
client_decode_uvarint(_Bad, _Shift, _Acc) ->
    erlang:error({invalid_fts_text_block, offsets}).

client_shard_key(Shard) ->
    client_guard(shard_id, Shard, ?MAX_U16),
    <<Shard:16/unsigned-big>>.

client_shard_id(Token, Shards) ->
    Raw =
        case Token of
            <<>> -> 0;
            <<B1:8>> -> B1 bsl 8;
            <<B1:8, B2:8, _/binary>> -> (B1 bsl 8) bor B2
        end,
    (Raw * Shards) bsr 16.

client_epoch_spec(Bucket, Shard, DocId) ->
    {add, Bucket, client_shard_key(Shard), <<"epoch">>,
        <<DocId:64/unsigned-big>>}.

client_write_shards([]) -> [0];
client_write_shards(Shards) -> Shards.

client_stats_dirty_spec(Bucket) ->
    {add, Bucket, <<"stats">>, <<"dirty">>, <<1>>}.

client_guard(_What, Value, Max) when
    is_integer(Value), Value >= 0, Value =< Max
->
    ok;
client_guard(What, Value, Max) ->
    erlang:error({fts_capacity_exceeded, What, Value, Max}).

%% Posting rows carry the DOC VERSION STAMP (docs/FTS.md §2): an 8-byte
%% content hash shared by every row of one derive batch. The search
%% merge admits a shard's contribution for a doc ONLY when its stamp
%% equals the current manifest's stamp, so a multi-shard query can
%% never assemble postings from two different document versions
%% (cross-shard read skew) into a false match.
client_encode_base_length(none) ->
    {0, 0};
client_encode_base_length(Length) when is_integer(Length), Length >= 0 ->
    client_guard(base_doc_length, Length, ?MAX_U64),
    {1, Length}.

client_decode_base_length(0, _Value) -> none;
client_decode_base_length(1, Value) -> Value.

client_encode_manifest(
    DocVersion,
    DocId,
    Shards,
    DocLength,
    BaseLength,
    Fingerprint,
    BlockCount
) when
    byte_size(DocVersion) == 8
->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    client_guard(manifest_shards, length(Shards), ?MAX_U16),
    client_guard(doc_length, DocLength, ?MAX_U64),
    client_guard(text_block_count, BlockCount, ?MAX_U32),
    32 = byte_size(Fingerprint),
    {BaseFlag, BaseValue} = client_encode_base_length(BaseLength),
    ShardBin = iolist_to_binary([
        client_encode_shard_id(Shard)
     || Shard <- Shards
    ]),
    <<?MANIFEST_VERSION:8, DocVersion:8/binary, DocId:64/unsigned-big,
        (length(Shards)):16/unsigned-big, ShardBin/binary,
        DocLength:64/unsigned-big, BaseFlag:8, BaseValue:64/unsigned-big,
        Fingerprint/binary, BlockCount:32/unsigned-big>>.

client_decode_manifest_value(
    #{shards := _, doc_length := _, fingerprint := _} = M
) ->
    M;
client_decode_manifest_value(
    <<?MANIFEST_VERSION:8, DocVersion:8/binary, DocId:64/unsigned-big,
        N:16/unsigned-big, Rest/binary>>
) ->
    ShardBytes = N * 2,
    case Rest of
        <<ShardBin:ShardBytes/binary, DocLength:64/unsigned-big, BaseFlag:8,
            BaseValue:64/unsigned-big, Fingerprint:32/binary,
            BlockCount:32/unsigned-big>> ->
            #{
                version => DocVersion,
                doc_id => DocId,
                shards => [S || <<S:16/unsigned-big>> <= ShardBin],
                doc_length => DocLength,
                base_length => client_decode_base_length(BaseFlag, BaseValue),
                fingerprint => Fingerprint,
                block_count => BlockCount
            };
        _ ->
            erlang:error({invalid_fts_manifest, Rest})
    end;
client_decode_manifest_value(Bad) ->
    erlang:error({invalid_fts_manifest, Bad}).

client_encode_shard_id(Shard) ->
    client_guard(shard_id, Shard, ?MAX_U16),
    <<Shard:16/unsigned-big>>.

-spec prepare(map(), binary() | list() | all_docs, map() | list()) ->
    {ok, tuple()} | {error, term()}.
prepare(#{fingerprint := Fingerprint} = Schema, Query, Opts0) ->
    try
        case normalise_search_options(Opts0, Schema) of
            {ok, Opts} ->
                case client_prepare_query_ast(Schema, Query, Opts) of
                    {ok, AST} ->
                        {ok,
                            {leveled_fts_prepared_v1, Fingerprint,
                                option_columns(Opts, Schema), AST}};
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

-spec search(
    pid(), map(), binary() | list() | all_docs | tuple(), map() | list()
) ->
    {ok, [map()] | #{hits := [map()], count := non_neg_integer()}}
    | {error, term()}.
search(Bookie, #{fingerprint := _} = Schema, Query, Opts0) when
    is_pid(Bookie)
->
    try
        {Hook, Opts1} = client_take_option(tail_fold_hook, Opts0),
        case normalise_search_options(Opts1, Schema) of
            {ok, Opts} ->
                case client_query_ast(Schema, Query, Opts) of
                    {ok, AST} ->
                        client_search(Bookie, Schema, AST, Opts, Hook);
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

-spec posting_read(
    pid(),
    map(),
    binary() | list() | tuple(),
    [binary() | non_neg_integer()],
    map() | list()
) -> {ok, [map()]} | {error, term()}.
posting_read(Bookie, #{fingerprint := _} = Schema, Query, DocKeys, Opts0) when
    is_pid(Bookie), is_list(DocKeys)
->
    try
        case normalise_search_options(Opts0, Schema) of
            {ok, Opts0N} ->
                case client_query_ast(Schema, Query, Opts0N) of
                    {ok, AST} ->
                        Opts = Opts0N#{
                            limit => length(DocKeys),
                            offset => 0,
                            rank => none,
                            return_positions => true
                        },
                        client_posting_read(
                            Bookie,
                            Schema,
                            AST,
                            lists:usort(DocKeys),
                            Opts
                        );
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

-spec text_blocks(
    pid(),
    map(),
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer()
) -> {ok, [{non_neg_integer(), binary()}]} | {error, term()}.
text_blocks(
    Bookie,
    #{index := _Bucket} = Schema,
    DocId,
    FirstBlock,
    LastBlock
) when
    is_pid(Bookie),
    is_integer(DocId),
    is_integer(FirstBlock),
    is_integer(LastBlock),
    FirstBlock >= 0,
    LastBlock >= FirstBlock
->
    try
        case
            text_blocks_batch(
                Bookie, Schema, [{DocId, FirstBlock, LastBlock}]
            )
        of
            {ok, Blocks} ->
                {ok,
                    lists:sort([
                        {BlockNo, Text}
                     || {{DocId0, BlockNo}, Text} <- maps:to_list(Blocks),
                        DocId0 =:= DocId
                    ])};
            Error ->
                Error
        end
    catch
        error:CaughtReason -> {error, CaughtReason}
    end;
text_blocks(_Bookie, _Schema, _DocId, _FirstBlock, _LastBlock) ->
    {error, invalid_fts_text_block_range}.

-spec text_blocks_batch(
    pid(),
    map(),
    [{non_neg_integer(), non_neg_integer(), non_neg_integer()}]
) ->
    {ok, #{{non_neg_integer(), non_neg_integer()} => binary()}}
    | {error, term()}.
text_blocks_batch(Bookie, #{index := _Bucket} = Schema, Requests) when
    is_pid(Bookie), is_list(Requests)
->
    TextOnlyRequests = [
        {DocId, FirstBlock, LastBlock, []}
     || Request <- Requests,
        {DocId, FirstBlock, LastBlock} <-
            [client_text_block_request_range(Request)]
    ],
    case text_blocks_with_offsets_batch(Bookie, Schema, TextOnlyRequests) of
        {ok, Blocks} ->
            {ok, maps:map(fun(_Key, {Text, _Offsets}) -> Text end, Blocks)};
        Error ->
            Error
    end;
text_blocks_batch(_Bookie, _Schema, _Requests) ->
    {error, invalid_fts_text_block_batch}.

-spec text_blocks_with_offsets_batch(
    pid(),
    map(),
    [{non_neg_integer(), non_neg_integer(), non_neg_integer()}]
) ->
    {ok, #{
        {non_neg_integer(), non_neg_integer()} =>
            {
                binary(),
                unavailable
                | [{non_neg_integer(), non_neg_integer(), non_neg_integer()}]
            }
    }}
    | {error, term()}.
text_blocks_with_offsets_batch(Bookie, #{index := Bucket}, Requests) when
    is_pid(Bookie), is_list(Requests)
->
    try
        Keys = lists:usort([
            {DocId, BlockNo}
         || Request <- Requests,
            {DocId, FirstBlock, LastBlock} <-
                [client_text_block_request_range(Request)],
            is_integer(DocId),
            is_integer(FirstBlock),
            is_integer(LastBlock),
            FirstBlock >= 0,
            LastBlock >= FirstBlock,
            BlockNo <- lists:seq(FirstBlock, LastBlock)
        ]),
        WantedOffsets = client_text_block_wanted_offsets(Requests),
        Wanted = maps:from_list([{Key, true} || Key <- Keys]),
        DocIds = lists:usort([DocId || {DocId, _BlockNo} <- Keys]),
        PackValues = leveled_bookie:book_headonly_many(
            Bookie,
            Bucket,
            [
                {client_text_block_key(DocId), <<0:32/unsigned-big>>}
             || DocId <- DocIds
            ]
        ),
        {PackFrames, PackedDocIds} = lists:foldl(
            fun({DocId, Result}, {Frames, Packed}) ->
                case Result of
                    {ok, Value} ->
                        case client_text_pack_frames(Value, DocId, Wanted) of
                            not_pack ->
                                {Frames, Packed};
                            Selected ->
                                {Selected ++ Frames, Packed#{DocId => true}}
                        end;
                    not_found ->
                        {Frames, Packed}
                end
            end,
            {[], #{}},
            lists:zip(DocIds, PackValues)
        ),
        DirectKeys = [
            {DocId, BlockNo}
         || {DocId, BlockNo} <- Keys,
            not maps:is_key(DocId, PackedDocIds)
        ],
        DirectValues =
            case DirectKeys of
                [] ->
                    [];
                _ ->
                    leveled_bookie:book_headonly_many(
                        Bookie,
                        Bucket,
                        [
                            {client_text_block_key(DocId), <<
                                BlockNo:32/unsigned-big
                            >>}
                         || {DocId, BlockNo} <- DirectKeys
                        ]
                    )
            end,
        {DirectFrames, DirectDecoded} = lists:foldl(
            fun
                (
                    {
                        {DocId, BlockNo},
                        {ok,
                            <<?TEXT_BLOCK_VERSION:8, RawBytes:32/unsigned-big,
                                Frame/binary>>}
                    },
                    {Frames, Decoded}
                ) ->
                    {
                        [
                            {{DocId, BlockNo}, Frame, RawBytes, seekable}
                            | Frames
                        ],
                        Decoded
                    };
                (
                    {
                        {DocId, BlockNo},
                        {ok,
                            <<?VARINT_TEXT_BLOCK_VERSION:8,
                                RawBytes:32/unsigned-big, Frame/binary>>}
                    },
                    {Frames, Decoded}
                ) ->
                    {
                        [{{DocId, BlockNo}, Frame, RawBytes, varint} | Frames],
                        Decoded
                    };
                ({{DocId, BlockNo}, {ok, Value}}, {Frames, Decoded}) ->
                    {
                        Frames,
                        Decoded#{
                            {DocId, BlockNo} =>
                                client_decode_text_block_with_offsets(Value)
                        }
                    };
                ({{_DocId, _BlockNo}, not_found}, Acc) ->
                    Acc
            end,
            {[], #{}},
            lists:zip(DirectKeys, DirectValues)
        ),
        FrameDecoded = client_decode_text_pack_frames(
            PackFrames ++ DirectFrames, WantedOffsets
        ),
        {ok, maps:merge(DirectDecoded, FrameDecoded)}
    catch
        error:CaughtReason -> {error, CaughtReason}
    end;
text_blocks_with_offsets_batch(_Bookie, _Schema, _Requests) ->
    {error, invalid_fts_text_block_batch}.

client_text_block_request_range({DocId, FirstBlock, LastBlock}) ->
    {DocId, FirstBlock, LastBlock};
client_text_block_request_range(
    {DocId, FirstBlock, LastBlock, _WantedOrdinals}
) ->
    {DocId, FirstBlock, LastBlock}.

client_text_block_wanted_offsets(Requests) ->
    lists:foldl(
        fun
            ({DocId, FirstBlock, LastBlock}, Acc) ->
                lists:foldl(
                    fun(BlockNo, InnerAcc) ->
                        InnerAcc#{{DocId, BlockNo} => all}
                    end,
                    Acc,
                    lists:seq(FirstBlock, LastBlock)
                );
            ({DocId, FirstBlock, LastBlock, WantedOrdinals}, Acc) when
                is_list(WantedOrdinals)
            ->
                Selected = lists:usort([
                    Ordinal
                 || Ordinal <- WantedOrdinals,
                    is_integer(Ordinal),
                    Ordinal >= 0,
                    Ordinal =< ?MAX_U32
                ]),
                lists:foldl(
                    fun(BlockNo, InnerAcc) ->
                        Key = {DocId, BlockNo},
                        case maps:get(Key, InnerAcc, []) of
                            all ->
                                InnerAcc;
                            Existing ->
                                InnerAcc#{
                                    Key => lists:umerge(Existing, Selected)
                                }
                        end
                    end,
                    Acc,
                    lists:seq(FirstBlock, LastBlock)
                )
        end,
        #{},
        Requests
    ).

client_text_pack_frames(
    <<?TEXT_PACK_MAGIC:24/unsigned-big, Version:8, Count:32/unsigned-big,
        DirectoryBytes:32/unsigned-big, Directory:DirectoryBytes/binary,
        Payload/binary>>,
    DocId,
    Wanted
) when
    (Version =:= ?TEXT_PACK_VERSION orelse
        Version =:= ?PREVIOUS_TEXT_PACK_VERSION) andalso
        DirectoryBytes =:= Count * ?TEXT_PACK_DIR_STRIDE
->
    Format =
        case Version of
            ?TEXT_PACK_VERSION -> seekable;
            ?PREVIOUS_TEXT_PACK_VERSION -> varint
        end,
    client_text_pack_frames(
        Directory, Payload, DocId, Wanted, Format, []
    );
client_text_pack_frames(_Value, _DocId, _Wanted) ->
    not_pack.

client_text_pack_frames(
    <<>>, _Payload, _DocId, _Wanted, _Format, Acc
) ->
    lists:reverse(Acc);
client_text_pack_frames(
    <<BlockNo:32/unsigned-big, Offset:32/unsigned-big, Bytes:32/unsigned-big,
        RawBytes:32/unsigned-big, Rest/binary>>,
    Payload,
    DocId,
    Wanted,
    Format,
    Acc
) ->
    Acc1 =
        case maps:is_key({DocId, BlockNo}, Wanted) of
            true ->
                Frame = binary:part(Payload, Offset, Bytes),
                [{{DocId, BlockNo}, Frame, RawBytes, Format} | Acc];
            false ->
                Acc
        end,
    client_text_pack_frames(
        Rest, Payload, DocId, Wanted, Format, Acc1
    ).

client_decode_text_pack_frames([], _WantedOffsets) ->
    #{};
client_decode_text_pack_frames(Frames, WantedOffsets) ->
    Inputs = [{Frame, RawBytes} || {_Key, Frame, RawBytes, _Format} <- Frames],
    case client_zstd_decompress_many(Inputs) of
        error ->
            erlang:error({invalid_fts_text_pack, compressed});
        Decoded ->
            maps:from_list([
                {Key,
                    client_decode_text_pack_payload(
                        Payload, Format, maps:get(Key, WantedOffsets, all)
                    )}
             || {{Key, _Frame, _RawBytes, Format}, Payload} <- lists:zip(
                    Frames, Decoded
                )
            ])
    end.

client_decode_text_pack_payload(
    <<BlockBytes:32/unsigned-big, Block:BlockBytes/binary,
        EncodedOffsets/binary>>
) ->
    {Block, client_decode_text_block_offsets(EncodedOffsets)};
client_decode_text_pack_payload(Bad) ->
    erlang:error({invalid_fts_text_pack_payload, Bad}).

client_decode_text_pack_payload(
    <<BlockBytes:32/unsigned-big, Block:BlockBytes/binary,
        OffsetDirectoryBytes:32/unsigned-big,
        OffsetDirectory:OffsetDirectoryBytes/binary, EncodedOffsets/binary>>,
    seekable,
    WantedOrdinals
) when
    OffsetDirectoryBytes rem ?TEXT_BLOCK_OFFSET_DIR_STRIDE =:= 0
->
    {Block,
        client_decode_seekable_text_block_offsets(
            OffsetDirectory, EncodedOffsets, WantedOrdinals
        )};
client_decode_text_pack_payload(
    <<BlockBytes:32/unsigned-big, Block:BlockBytes/binary,
        EncodedOffsets/binary>>,
    varint,
    all
) ->
    {Block, client_decode_text_block_offsets(EncodedOffsets)};
client_decode_text_pack_payload(Payload, varint, _WantedOrdinals) ->
    %% Version 4 and packed compatibility rows have a delta stream without a
    %% seek directory. Decode it completely; fresh version 5 rows use the
    %% seek directory above.
    client_decode_text_pack_payload(Payload);
client_decode_text_pack_payload(Bad, _Format, _WantedOrdinals) ->
    erlang:error({invalid_fts_text_pack_payload, Bad}).

-spec document_text(pid(), map(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.
document_text(Bookie, #{index := Bucket} = Schema, DocKey) when
    is_pid(Bookie), is_binary(DocKey)
->
    try
        case
            leveled_bookie:book_headonly(
                Bookie, Bucket, <<"doc">>, DocKey
            )
        of
            {ok, Manifest0} ->
                Manifest = client_decode_manifest_value(Manifest0),
                DocId = maps:get(doc_id, Manifest),
                BlockCount = maps:get(block_count, Manifest, 0),
                case BlockCount of
                    0 ->
                        {ok, <<>>};
                    _ ->
                        case
                            text_blocks(
                                Bookie, Schema, DocId, 0, BlockCount - 1
                            )
                        of
                            {ok, Blocks} when length(Blocks) =:= BlockCount ->
                                {ok,
                                    iolist_to_binary([
                                        Block
                                     || {_BlockNo, Block} <- Blocks
                                    ])};
                            {ok, Blocks} ->
                                {error, {
                                    incomplete_fts_text_blocks,
                                    BlockCount,
                                    length(Blocks)
                                }};
                            Error ->
                                Error
                        end
                end;
            not_found ->
                not_found;
            {error, ReadReason} ->
                {error, ReadReason}
        end
    catch
        error:CaughtReason -> {error, CaughtReason}
    end;
document_text(_Bookie, _Schema, _DocKey) ->
    {error, invalid_fts_document_text_request}.

-spec document_text_batch(pid(), map(), [binary()]) ->
    {ok, #{binary() => binary()}} | {error, term()}.
document_text_batch(Bookie, #{index := Bucket} = Schema, DocKeys0) when
    is_pid(Bookie), is_list(DocKeys0)
->
    try
        DocKeys = lists:usort(DocKeys0),
        ManifestValues = leveled_bookie:book_headonly_many(
            Bookie,
            Bucket,
            [{<<"doc">>, DocKey} || DocKey <- DocKeys]
        ),
        Manifests = lists:foldl(
            fun
                ({DocKey, {ok, Manifest0}}, Acc) ->
                    Acc#{DocKey => client_decode_manifest_value(Manifest0)};
                ({_DocKey, not_found}, Acc) ->
                    Acc
            end,
            #{},
            lists:zip(DocKeys, ManifestValues)
        ),
        Requests = maps:fold(
            fun(_DocKey, Manifest, Acc) ->
                case maps:get(block_count, Manifest, 0) of
                    0 -> Acc;
                    BlockCount ->
                        [{maps:get(doc_id, Manifest), 0, BlockCount - 1} | Acc]
                end
            end,
            [],
            Manifests
        ),
        case text_blocks_batch(Bookie, Schema, Requests) of
            {ok, Blocks} ->
                Texts = maps:map(
                    fun(_DocKey, Manifest) ->
                        DocId = maps:get(doc_id, Manifest),
                        BlockCount = maps:get(block_count, Manifest, 0),
                        iolist_to_binary([
                            maps:get({DocId, BlockNo}, Blocks)
                         || BlockNo <- lists:seq(0, BlockCount - 1)
                        ])
                    end,
                    Manifests
                ),
                {ok, Texts};
            Error ->
                Error
        end
    catch
        error:CaughtReason -> {error, CaughtReason}
    end;
document_text_batch(_Bookie, _Schema, _DocKeys) ->
    {error, invalid_fts_document_text_batch_request}.

-spec record_fetch(pid(), map(), [non_neg_integer()], fold | points) ->
    {ok, map()} | {error, term()}.
record_fetch(Bookie, #{index := _} = Schema, DocIds0, Strategy) when
    is_pid(Bookie),
    is_list(DocIds0),
    (Strategy =:= fold orelse Strategy =:= points)
->
    try
        DocIds = lists:usort(DocIds0),
        Documents = fts2_lookup_documents(Bookie, Schema, DocIds),
        Found = maps:map(
            fun(_DocId, Document) ->
                {
                    maps:get(doc_key, Document),
                    maps:get(doc_version, Document),
                    maps:get(hit_record, Document)
                }
            end,
            Documents
        ),
        {ok, Found}
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end;
record_fetch(_Bookie, _Schema, _DocIds, _Strategy) ->
    {error, invalid_fts_record_fetch}.

-spec candidate_fetch(pid(), map(), [non_neg_integer()], fold | points) ->
    {ok, map()} | {error, term()}.
candidate_fetch(Bookie, #{index := _} = Schema, DocIds0, Strategy) when
    is_pid(Bookie),
    is_list(DocIds0),
    (Strategy =:= fold orelse Strategy =:= points)
->
    try
        DocIds = lists:usort(DocIds0),
        Documents = fts2_lookup_documents(Bookie, Schema, DocIds),
        Found = maps:map(
            fun(_DocId, Document) ->
                {
                    maps:get(doc_key, Document),
                    maps:get(doc_version, Document),
                    maps:get(candidate_record, Document)
                }
            end,
            Documents
        ),
        Validated = maps:map(
            fun(DocId, {DocKey, DocVersion, _Candidate} = Mapping) ->
                client_validate_doc_id_mapping(DocId, DocKey, DocVersion),
                Mapping
            end,
            Found
        ),
        {ok, Validated}
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end;
candidate_fetch(_Bookie, _Schema, _DocIds, _Strategy) ->
    {error, invalid_fts_candidate_fetch}.

%% Phase two of served search: hydrate only final-page identity coordinates.
%% The page address is stable within the root used by phase one and resolves
%% through one batched read per distinct identity page.
-spec hydrate_page(pid(), map(), [{non_neg_integer(), non_neg_integer()}]) ->
    {ok, map()} | {error, term()}.
hydrate_page(_Bookie, #{fingerprint := _}, []) ->
    {ok, #{}};
hydrate_page(Bookie, #{fingerprint := _} = Schema, Addresses) when
    is_pid(Bookie), is_list(Addresses)
->
    try
        case leveled_fts_residency:prepare(Bookie, Schema) of
            ok ->
                case fts2_search_root(Bookie, Schema) of
                    {ok, Root} ->
                        Requests = lists:usort(Addresses),
                        Rows = fts2_search_read_identity_rows(
                            Bookie, Schema, Root, Requests
                        ),
                        {ok, lists:foldl(
                            fun
                                (undefined, Acc) ->
                                    Acc;
                                ({GroupId, _GroupKey, ChunkId, SourceId,
                                    DocKey, DocVersion, DocLength, Candidate,
                                    HitRecord}, Acc) ->
                                    Acc#{{GroupId, ChunkId} =>
                                        fts2_search_page_row(
                                            SourceId, DocKey, DocVersion,
                                            DocLength, Candidate, HitRecord
                                        )}
                            end,
                            #{},
                            Rows
                        )};
                    not_found ->
                        {ok, #{}}
                end;
            {error, PrepareReason} ->
                {error, PrepareReason}
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end;
hydrate_page(_Bookie, _Schema, _Addresses) ->
    {error, invalid_fts_hydrate_page}.

fts2_search_page_row(
    SourceId, DocKey, DocVersion, DocLength, Candidate, HitRecord
) ->
    Base = #{
        doc_id => SourceId,
        doc_length => DocLength,
        candidate_key => DocKey,
        candidate_version => DocVersion,
        candidate_record => Candidate,
        hit_record => HitRecord
    },
    case {maps:find('$fts_text_blocks', Candidate),
        maps:find('$fts_text_bytes', Candidate)}
    of
        {{ok, Blocks}, {ok, Bytes}} ->
            Base#{text_blocks => Blocks, text_bytes => Bytes,
                index_resident_complete => true};
        _ ->
            Base
    end.

client_query_ast(
    #{fingerprint := Fingerprint} = Schema,
    {leveled_fts_prepared_v1, Fingerprint, Columns, AST},
    Opts
) ->
    case Columns =:= option_columns(Opts, Schema) of
        true -> {ok, AST};
        false -> {error, invalid_fts_prepared_query}
    end;
client_query_ast(
    _Schema,
    {leveled_fts_prepared_v1, _Fingerprint, _Columns, _AST},
    _Opts
) ->
    {error, invalid_fts_prepared_query};
client_query_ast(Schema, Query, Opts) ->
    client_prepare_query_ast(Schema, Query, Opts).

client_prepare_query_ast(Schema, Query, Opts) ->
    case client_plain_query_ast(Schema, Query, Opts) of
        {ok, AST} ->
            {ok, AST};
        parse ->
            client_prepare_parsed_query_ast(Schema, Query, Opts)
    end.

client_prepare_parsed_query_ast(Schema, Query, Opts) ->
    case parse(Query, Opts) of
        {ok, AST0} ->
            Columns = option_columns(Opts, Schema),
            case validate_ast_columns(AST0, Columns) of
                ok ->
                    {ok,
                        canonicalise_ast_columns(
                            restrict_ast_columns(AST0, Columns)
                        )};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

client_plain_query_ast(Schema, Query, Opts) when
    is_binary(Query), byte_size(Query) > 0
->
    case client_plain_query_bytes(Query) of
        true ->
            {ok, {term, Query, false,
                canonical_selector(option_columns(Opts, Schema))}};
        false ->
            client_simple_phrase_query_ast(Schema, Query, Opts)
    end;
client_plain_query_ast(_Schema, _Query, _Opts) ->
    parse.

%% The public interactive grammar commonly emits a two-token quoted phrase.
%% Its lowercase ASCII spelling is already in the exact token form accepted by
%% the plain-term fast path, so avoid constructing the general lexer/parser
%% state solely to recover the same two offsets. All other phrase spellings
%% retain the complete grammar path.
client_simple_phrase_query_ast(Schema, <<$", Rest/binary>>, Opts) when
    byte_size(Rest) >= 2
->
    InnerBytes = byte_size(Rest) - 1,
    case Rest of
        <<Inner:InnerBytes/binary, $">> ->
            case binary:split(Inner, <<" ">>, [global]) of
                [First, Second] when byte_size(First) > 0,
                    byte_size(Second) > 0 ->
                    case client_plain_query_bytes(First) andalso
                        client_plain_query_bytes(Second) of
                        true ->
                            {ok, {phrase,
                                [{First, false, 0}, {Second, false, 1}],
                                canonical_selector(
                                    option_columns(Opts, Schema)
                                )}};
                        false ->
                            parse
                    end;
                _ ->
                    parse
            end;
        _ ->
            parse
    end;
client_simple_phrase_query_ast(_Schema, _Query, _Opts) ->
    parse.

client_plain_query_bytes(<<>>) ->
    true;
client_plain_query_bytes(<<Byte, Rest/binary>>) when
    (Byte >= $a andalso Byte =< $z) orelse
        (Byte >= $0 andalso Byte =< $9)
->
    client_plain_query_bytes(Rest);
client_plain_query_bytes(_Query) ->
    false.

client_posting_read(_Bookie, _Schema, _AST, [], _Opts) ->
    {ok, []};
client_posting_read(Bookie, Schema, AST, DocKeys, Opts) ->
    CandidateIds = maps:keys(
        client_posting_candidates(Bookie, Schema, DocKeys)
    ),
    case client_fts2_state(Bookie, Schema) of
        {clean, Root} when is_map(Root) ->
            fts2_posting_read(
                Bookie, Schema, Root, AST, CandidateIds, Opts
            );
        {_State, Root} ->
            fts2_posting_read_dirty(
                Bookie, Schema, Root, AST, CandidateIds, Opts
            )
    end.

client_posting_candidates(Bookie, Schema, DocKeys) ->
    Bucket = maps:get(index, Schema),
    Fingerprint = maps:get(fingerprint, Schema),
    Direct = maps:from_list([
        {DocId, true}
     || DocId <- DocKeys,
        is_integer(DocId),
        DocId >= 0,
        DocId =< ?MAX_DOC_ID
    ]),
    ValidKeys = lists:usort([
        DocKey
     || DocKey <- DocKeys, is_binary(DocKey)
    ]),
    case ValidKeys of
        [] ->
            Direct;
        _ ->
            Wanted = maps:from_list([{DocKey, true} || DocKey <- ValidKeys]),
            First = hd(ValidKeys),
            Last = lists:last(ValidKeys),
            Fold = fun
                (B, {<<"doc">>, DocKey}, Value, Acc) when B =:= Bucket ->
                    case maps:is_key(DocKey, Wanted) of
                        true ->
                            Manifest = client_decode_manifest_value(Value),
                            case maps:get(fingerprint, Manifest) of
                                Fingerprint ->
                                    Acc#{
                                        maps:get(doc_id, Manifest) => true
                                    };
                                _OtherFingerprint ->
                                    Acc
                            end;
                        false ->
                            Acc
                    end;
                (_B, _K, _Value, Acc) ->
                    Acc
            end,
            {async, Runner} = leveled_bookie:book_headfold(
                Bookie,
                ?HEAD_TAG,
                {range, Bucket, {
                    {<<"doc">>, First},
                    {<<"doc">>, Last}
                }},
                {client_cancellable_fold(Fold), #{}},
                false,
                true,
                false
            ),
            maps:merge(Direct, client_run_fold(Runner))
    end.

client_take_option(Key, Opts) when is_map(Opts) ->
    {maps:get(Key, Opts, undefined), maps:remove(Key, Opts)};
client_take_option(Key, Opts) when is_list(Opts) ->
    {proplists:get_value(Key, Opts, undefined), proplists:delete(Key, Opts)}.

%% A generation becomes visible only after both mutable delta markers are
%% clear.  Consolidation publishes the immutable FTS2 root before clearing the
%% record marker, so there is no interval in which a stale generation can be
%% selected after a write.
client_fts2_state_for_ast(Bookie, Schema, AST) ->
    TermRequirements = client_term_bloom_requirements(AST, Schema),
    BigramRequirements = client_bigram_bloom_requirements(AST, Schema),
    case {TermRequirements, BigramRequirements} of
        {[], []} -> client_fts2_state(Bookie, Schema);
        _ ->
            client_fts2_state_with_bloom(
                Bookie, Schema, TermRequirements, BigramRequirements
            )
    end.

client_term_bloom_requirements({term, Token, false, Columns}, Schema) ->
    [{Token, fts2_search_selector_ids(Columns, Schema)}];
client_term_bloom_requirements({phrase, Specs, Columns}, Schema) ->
    ColumnIds = fts2_search_selector_ids(Columns, Schema),
    [
        {Token, ColumnIds}
     || {Token, Prefix, _Offset} <- Specs,
        Prefix =:= false
    ];
client_term_bloom_requirements(_AST, _Schema) ->
    [].

client_bigram_bloom_requirements(
    {phrase, [{First, false, Offset}, {Second, false, NextOffset}], Columns},
    Schema
) when NextOffset =:= Offset + 1 ->
    [{First, Second, fts2_search_selector_ids(Columns, Schema)}];
client_bigram_bloom_requirements(_AST, _Schema) ->
    [].

client_fts2_state_with_bloom(
    Bookie, #{index := Bucket} = Schema, Requirements, BigramRequirements
) ->
    TermShards = lists:usort([
        fts2_term_bloom_shard(Column, Token)
     || {Token, ColumnIds} <- Requirements,
        Column <- ColumnIds
    ]),
    BigramShards = lists:usort([
        fts2_bigram_bloom_shard(Column, First, Second)
     || {First, Second, ColumnIds} <- BigramRequirements,
        Column <- ColumnIds
    ]),
    ShardKeys =
        [{term, Shard} || Shard <- TermShards] ++
            [{bigram, Shard} || Shard <- BigramShards],
    case length(ShardKeys) =< 8 of
        false ->
            client_fts2_state_with_bloom_legacy(
                Bookie, Schema, Requirements
            );
        true ->
            [StateResult | BloomResults] = leveled_fts_residency:headonly_many(
                Bookie,
                Bucket,
                [{<<"f2:state">>, <<"current">>}] ++
                    [client_bloom_shard_key(ShardKey)
                     || ShardKey <- ShardKeys]
            ),
            case StateResult of
                {ok, <<1, RootValue/binary>>} ->
                    Root = fts2_codec_decode_root(RootValue),
                    case maps:get(fingerprint, Root) =:=
                        maps:get(fingerprint, Schema) of
                        false ->
                            {dirty, undefined};
                        true ->
                            BloomByShard = maps:from_list(
                                lists:zip(ShardKeys, BloomResults)
                            ),
                            TermProof = client_term_shard_bloom_proves_absent(
                                Root, BloomByShard, Requirements
                            ),
                            BigramProof =
                                client_bigram_shard_bloom_proves_absent(
                                    Root,
                                    BloomByShard,
                                    BigramRequirements
                                ),
                            case {TermProof, BigramProof} of
                                {true, _} -> {clean_absent, Root};
                                {_, true} -> {clean_absent, Root};
                                {false, false} -> {clean, Root};
                                %% Missing optional shards select the legacy
                                %% proof, never an empty result.
                                _ ->
                                    client_fts2_state_with_bloom_legacy(
                                        Bookie, Schema, Requirements
                                    )
                            end
                    end;
                _DirtyOrLegacy ->
                    client_fts2_state(Bookie, Schema)
            end
    end.

client_bloom_shard_key({term, Shard}) ->
    {<<"f2:bloom">>, <<"s", Shard:8>>};
client_bloom_shard_key({bigram, Shard}) ->
    {<<"f2:bloom">>, <<"g", Shard:8>>}.

client_term_shard_bloom_proves_absent(
    #{generation := Generation,
        term_bloom_shards := ?TERM_BLOOM_SHARDS},
    BloomByShard,
    Requirements
) ->
    try
        lists:any(
            fun({Token, ColumnIds}) ->
                ColumnIds =/= [] andalso
                    not lists:any(
                        fun(Column) ->
                            Shard = fts2_term_bloom_shard(Column, Token),
                            case maps:get(
                                {term, Shard}, BloomByShard, not_found
                            ) of
                                {ok, <<1, Generation:64/unsigned-big,
                                    Bloom/binary>>} when
                                    byte_size(Bloom) >=
                                        ?TERM_BLOOM_SHARD_WORDS * 8
                                ->
                                    fts2_term_bloom_might_contain(
                                        Bloom,
                                        ?TERM_BLOOM_SHARD_BITS,
                                        Column,
                                        Token,
                                        0
                                    );
                                _ ->
                                    throw(unavailable)
                            end
                        end,
                        ColumnIds
                    )
            end,
            Requirements
        )
    catch
        throw:unavailable -> unavailable
    end;
client_term_shard_bloom_proves_absent(_Root, _BloomByShard, _Requirements) ->
    unavailable.

client_bigram_shard_bloom_proves_absent(_Root, _BloomByShard, []) ->
    false;
client_bigram_shard_bloom_proves_absent(
    #{generation := Generation,
        bigram_bloom_shards := ?BIGRAM_BLOOM_SHARDS},
    BloomByShard,
    Requirements
) ->
    try
        lists:any(
            fun({First, Second, ColumnIds}) ->
                ColumnIds =/= [] andalso
                    not lists:any(
                        fun(Column) ->
                            Shard = fts2_bigram_bloom_shard(
                                Column, First, Second
                            ),
                            case maps:get(
                                {bigram, Shard}, BloomByShard, not_found
                            ) of
                                {ok, <<1, Generation:64/unsigned-big,
                                    Bloom/binary>>} when
                                    byte_size(Bloom) >=
                                        ?BIGRAM_BLOOM_SHARD_WORDS * 8
                                ->
                                    fts2_bigram_bloom_might_contain(
                                        Bloom,
                                        ?BIGRAM_BLOOM_SHARD_BITS,
                                        Column,
                                        First,
                                        Second,
                                        0
                                    );
                                _ ->
                                    throw(unavailable)
                            end
                        end,
                        ColumnIds
                    )
            end,
            Requirements
        )
    catch
        throw:unavailable -> unavailable
    end;
client_bigram_shard_bloom_proves_absent(
    _Root, _BloomByShard, _Requirements
) ->
    unavailable.

fts2_term_bloom_shard(Column, Token) ->
    erlang:phash2({Column, Token}, ?TERM_BLOOM_SHARDS).

fts2_bigram_bloom_shard(Column, First, Second) ->
    erlang:phash2({Column, First, Second}, ?BIGRAM_BLOOM_SHARDS).

client_fts2_state_with_bloom_legacy(
    Bookie, #{index := Bucket} = Schema, Requirements
) ->
    [StateResult, BloomResult] = leveled_fts_residency:headonly_many(
        Bookie,
        Bucket,
        [
            {<<"f2:state">>, <<"current">>},
            {<<"f2:bloom">>, <<"current">>}
        ]
    ),
    case StateResult of
        {ok, <<1, RootValue/binary>>} ->
            client_fts2_state_with_bloom_value(
                RootValue, BloomResult, Schema, Requirements
            );
        _DirtyOrLegacy ->
            client_fts2_state(Bookie, Schema)
    end.

client_fts2_state_with_bloom_value(
    RootValue, BloomResult, Schema, Requirements
) ->
    Root = fts2_codec_decode_root(RootValue),
    case maps:get(fingerprint, Root) =:= maps:get(fingerprint, Schema) of
        false ->
            {dirty, undefined};
        true ->
            case client_term_bloom_proves_absent(
                Root, BloomResult, Requirements
            ) of
                true -> {clean_absent, Root};
                false -> {clean, Root}
            end
    end.

client_term_bloom_proves_absent(
    #{generation := Generation, term_bloom := 1},
    {ok, <<1, Generation:64/unsigned-big, Bloom/binary>>},
    Requirements
) when byte_size(Bloom) >= ?TERM_BLOOM_WORDS * 8 ->
    BloomBits = byte_size(Bloom) * 8,
    lists:any(
        fun({Token, ColumnIds}) ->
            ColumnIds =/= [] andalso
                not lists:any(
                    fun(Column) ->
                        fts2_term_bloom_might_contain(
                            Bloom, BloomBits, Column, Token, 0
                        )
                    end,
                    ColumnIds
                )
        end,
        Requirements
    );
client_term_bloom_proves_absent(_Root, _BloomResult, _Requirements) ->
    false.

fts2_term_bloom_might_contain(_Bloom, _BloomBits, _Column, _Token, 4) ->
    true;
fts2_term_bloom_might_contain(Bloom, BloomBits, Column, Token, Salt) ->
    BitIndex = erlang:phash2({Salt, Column, Token}, BloomBits),
    Offset = (BitIndex bsr 6) * 8,
    <<Word:64/unsigned-little>> = binary:part(Bloom, Offset, 8),
    case Word band (1 bsl (BitIndex band 63)) of
        0 -> false;
        _ -> fts2_term_bloom_might_contain(
            Bloom, BloomBits, Column, Token, Salt + 1
        )
    end.

fts2_bigram_bloom_might_contain(
    _Bloom, _BloomBits, _Column, _First, _Second, 4
) ->
    true;
fts2_bigram_bloom_might_contain(
    Bloom, BloomBits, Column, First, Second, Salt
) ->
    BitIndex = erlang:phash2(
        {Salt, Column, First, Second}, BloomBits
    ),
    Offset = (BitIndex bsr 6) * 8,
    <<Word:64/unsigned-little>> = binary:part(Bloom, Offset, 8),
    case Word band (1 bsl (BitIndex band 63)) of
        0 -> false;
        _ -> fts2_bigram_bloom_might_contain(
            Bloom, BloomBits, Column, First, Second, Salt + 1
        )
    end.

client_fts2_state(Bookie, #{index := Bucket} = Schema) ->
    case leveled_fts_residency:headonly(
        Bookie, Bucket, <<"f2:state">>, <<"current">>
    ) of
        {ok, <<1, RootValue/binary>>} ->
            Root = fts2_codec_decode_root(RootValue),
            case maps:get(fingerprint, Root) =:= maps:get(fingerprint, Schema) of
                true -> {clean, Root};
                false -> {dirty, undefined}
            end;
        {ok, <<0>>} ->
            Root = case fts2_search_root(Bookie, Schema) of
                {ok, ExistingRoot} -> ExistingRoot;
                not_found -> undefined
            end,
            {dirty, Root};
        not_found ->
            client_fts2_legacy_state(Bookie, Bucket, Schema)
    end.

client_fts2_legacy_state(Bookie, Bucket, Schema) ->
    {RootKey, RootSubKey} = fts2_codec_root_key(),
    [RootResult, StatsResult, RecordResult] =
        leveled_fts_residency:headonly_many(
            Bookie,
            Bucket,
            [
                {RootKey, RootSubKey},
                {<<"stats">>, <<"dirty">>},
                {<<"record-tail">>, <<"dirty">>}
            ]
        ),
    Root = case RootResult of
        {ok, RootValue} ->
            ExistingRoot = fts2_codec_decode_root(RootValue),
            case maps:get(fingerprint, ExistingRoot) =:=
                maps:get(fingerprint, Schema) of
                true -> ExistingRoot;
                false -> undefined
            end;
        not_found ->
            undefined
    end,
    State = case {StatsResult, RecordResult} of
        {not_found, not_found} -> clean;
        _DirtyOrUnavailable -> dirty
    end,
    {State, Root}.

client_search(Bookie, Schema, AST, Opts, Hook) ->
    case leveled_fts_residency:prepare(Bookie, Schema) of
        ok ->
            case client_fts2_state_for_ast(Bookie, Schema, AST) of
                {clean_absent, _Root} ->
                    client_empty_search_result(Opts);
                {clean, Root} when is_map(Root) ->
                    fts2_search(Bookie, Schema, Root, AST, Opts);
                {dirty, Root} when is_map(Root) ->
                    case {maps:get(grouping, Opts, grouped),
                        maps:get(candidate_group_fields, Schema, [])} of
                        {grouped, [_ | _]} ->
                            {error, {fts_index_dirty,
                                grouped_search_requires_consolidation,
                                maps:get(generation, Root)}};
                        _ExactDirtyMode ->
                            fts2_search_dirty(
                                Bookie, Schema, Root, AST, Opts, Hook
                            )
                    end;
                {_State, Root} ->
                    fts2_search_dirty(Bookie, Schema, Root, AST, Opts, Hook)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

client_empty_search_result(Opts) ->
    case maps:get(return_count, Opts, false) of
        true -> {ok, #{hits => [], count => 0,
            count_kind => fts2_search_count_kind(Opts)}};
        false -> {ok, []}
    end.

client_validate_doc_id_mapping(DocId, DocKey, DocVersion) ->
    case client_transient_doc_id(DocId) of
        false ->
            ok;
        true ->
            case client_doc_id(DocKey, DocVersion) of
                DocId -> ok;
                _ -> erlang:error({invalid_fts_doc_id_mapping, DocId})
            end
    end.

-spec consolidate(pid(), map(), map() | list()) ->
    {ok, map()} | {error, term()}.
consolidate(Bookie, #{fingerprint := _} = Schema, Opts0) when is_pid(Bookie) ->
    try
        {Hook, Opts} = client_take_option(before_consolidate_commit, Opts0),
        AllShards = lists:seq(0, maps:get(shards, Schema) - 1),
        case {client_option(shards, Opts, all), client_fts2_state(Bookie, Schema)} of
            {_Requested, {clean, _Root}} ->
                case client_option(reclaim, Opts, true) of
                    true ->
                        fts2_build_reclaim_ledgers(
                            lists:usort([
                                Bookie,
                                fts2_build_identity_bookie(Bookie, Schema)
                            ]),
                            client_option(reclaim_timeout_ms, Opts, 300000)
                        );
                    false ->
                        ok
                end,
                {ok, #{consolidated => [], skipped => []}};
            {Shards, _State} when is_list(Shards) ->
                {ok, #{consolidated => Shards, skipped => []}};
            {all, _State} ->
                case fts2_consolidate(Bookie, Schema, Hook, Opts) of
                    {ok, _Root} ->
                        {ok, #{consolidated => AllShards, skipped => []}}
                end
        end
    catch
        error:fts2_generation_raced ->
            {ok, #{consolidated => [], skipped =>
                lists:seq(0, maps:get(shards, Schema) - 1)}};
        error:Reason -> {error, Reason}
    end.

client_option(Key, Opts, Default) when is_map(Opts) ->
    maps:get(Key, Opts, Default);
client_option(Key, Opts, Default) when is_list(Opts) ->
    proplists:get_value(Key, Opts, Default).

normalise_column_specs(Columns) when is_list(Columns), Columns =/= [] ->
    try
        Specs = [normalise_column_spec(Column) || Column <- Columns],
        case duplicate_columns(Specs) of
            false -> {ok, Specs};
            true -> {error, invalid_fts_columns}
        end
    catch
        _:_ -> {error, invalid_fts_columns}
    end;
normalise_column_specs(_Columns) ->
    {error, invalid_fts_columns}.

normalise_hit_fields(Fields) when is_list(Fields) ->
    [normalise_hit_field(Field) || Field <- Fields].

normalise_hit_field(#{name := Name, path := Path}) when is_list(Path) ->
    {Name, Path};
normalise_hit_field({Name, Path}) when is_list(Path) ->
    {Name, Path};
normalise_hit_field(Name) when is_atom(Name) orelse is_binary(Name) ->
    {Name, [Name]}.

normalise_candidate_filter_fields(Fields) when is_list(Fields) ->
    [normalise_candidate_filter_field(Field) || Field <- Fields].

normalise_candidate_filter_field(#{column := Column, field := Field}) ->
    {normalise_column(Column), Field};
normalise_candidate_filter_field({Column, Field}) ->
    {normalise_column(Column), Field}.

normalise_field_names(Fields) when is_list(Fields) ->
    [Name || Name <- Fields, is_atom(Name) orelse is_binary(Name)].

normalise_optional_field_name(undefined) ->
    undefined;
normalise_optional_field_name(nil) ->
    undefined;
normalise_optional_field_name(Name) when is_atom(Name) orelse is_binary(Name) ->
    Name.

normalise_text_path(undefined) ->
    undefined;
normalise_text_path(nil) ->
    undefined;
normalise_text_path(Path) when is_list(Path) ->
    Path;
normalise_text_path(Name) when is_atom(Name) orelse is_binary(Name) ->
    [Name].

%% A column spec may carry a mode: text (default -- tokenised through the
%% schema tokenizer) or verbatim (the field value posts as one exact token
%% at position 0; no tokenisation, case folding, or diacritic removal).
%% Verbatim columns exist for filter facts such as tenant ids, where
%% partial or case-folded matches would be wrong. Changing a column's mode
%% changes what is posted at write time, so it requires a reindex, exactly
%% like any other schema change.
normalise_column_spec(#{name := Name, path := Path} = Spec) when
    is_list(Path)
->
    {
        normalise_column(Name),
        Path,
        normalise_column_mode(maps:get(mode, Spec, text))
    };
normalise_column_spec({Name, Path, Mode}) when is_list(Path) ->
    {normalise_column(Name), Path, normalise_column_mode(Mode)};
normalise_column_spec({Name, Path}) when is_list(Path) ->
    {normalise_column(Name), Path, text};
normalise_column_spec(Name) ->
    Column = normalise_column(Name),
    {Column, [Name], text}.

normalise_column_mode(text) -> text;
normalise_column_mode(verbatim) -> verbatim;
normalise_column_mode(<<"text">>) -> text;
normalise_column_mode(<<"verbatim">>) -> verbatim.

duplicate_columns(Specs) ->
    Columns = [Column || {Column, _Path, _Mode} <- Specs],
    length(Columns) =/= length(lists:usort(Columns)).

extract_fields(Object, ColumnSpecs) ->
    [
        {Column, Mode, normalise_text(extract_path(Object, Path))}
     || {Column, Path, Mode} <- ColumnSpecs
    ].

client_extract_text(_Object, #{text_path := undefined}) ->
    <<>>;
client_extract_text(Object, #{text_path := Path}) ->
    normalise_text(extract_path(Object, Path)).

client_hit_record(DocKey, Object, Schema, BlockDirectory, TextBytes) ->
    Record = maps:from_list([
        {Name, extract_path(Object, Path)}
     || {Name, Path} <- maps:get(hit_fields, Schema)
    ]),
    HitRecord = #{
        doc_key => DocKey,
        record => Record,
        text_blocks => BlockDirectory,
        text_bytes => TextBytes
    },
    Encoded = client_encode_hit_record(HitRecord),
    client_guard(
        hit_record_bytes, byte_size(Encoded), ?MAX_HIT_RECORD_BYTES
    ),
    HitRecord.

client_candidate_record(Object, Schema) ->
    maps:from_list([
        {Name, extract_path(Object, Path)}
     || {Name, Path} <- maps:get(candidate_fields, Schema, [])
    ]).

client_encode_hit_record(HitRecord) ->
    term_to_binary(HitRecord, [deterministic, compressed]).

client_text_blocks(<<>>) ->
    [];
client_text_blocks(Text) ->
    client_text_blocks(Text, 0, []).

client_text_blocks(<<>>, _BlockNo, Acc) ->
    lists:reverse(Acc);
client_text_blocks(Text, BlockNo, Acc) ->
    Bytes = erlang:min(byte_size(Text), ?TEXT_BLOCK_BYTES),
    <<Block:Bytes/binary, Rest/binary>> = Text,
    client_text_blocks(Rest, BlockNo + 1, [{BlockNo, Block} | Acc]).

client_text_structure(<<>>, _Opts) ->
    {[], #{}};
client_text_structure(Text, Opts) ->
    BlockCount =
        (byte_size(Text) + ?TEXT_BLOCK_BYTES - 1) div ?TEXT_BLOCK_BYTES,
    {BlockCount, NextDirectoryBlock, Directory0, CurrentBlock,
        CurrentOffsets, OffsetBlocks0, EndOrdinal} = client_token_fold(
        fun client_text_structure_token/5,
        {BlockCount, 0, [], undefined, [], [], 0},
        Text,
        Opts
    ),
    Directory = client_finish_text_directory(
        NextDirectoryBlock, BlockCount, EndOrdinal, Directory0
    ),
    OffsetBlocks = case CurrentBlock of
        undefined -> OffsetBlocks0;
        _ -> [{CurrentBlock, lists:reverse(CurrentOffsets)} | OffsetBlocks0]
    end,
    {lists:reverse(Directory), maps:from_list(OffsetBlocks)}.

client_text_structure_token(
    _Token,
    Ordinal,
    Offset,
    Length,
    {BlockCount, NextDirectoryBlock, Directory, CurrentBlock,
        CurrentOffsets, OffsetBlocks, _EndOrdinal}
) ->
    {NextDirectoryBlock1, Directory1} = client_fill_text_directory(
        NextDirectoryBlock,
        BlockCount,
        Offset + Length,
        Ordinal,
        Directory
    ),
    TokenBlock = Offset div ?TEXT_BLOCK_BYTES,
    RelativeOffset = Offset - TokenBlock * ?TEXT_BLOCK_BYTES,
    {CurrentBlock1, CurrentOffsets1, OffsetBlocks1} =
        case CurrentBlock of
            undefined ->
                {TokenBlock, [{Ordinal, RelativeOffset, Length}], OffsetBlocks};
            TokenBlock ->
                {CurrentBlock,
                    [{Ordinal, RelativeOffset, Length} | CurrentOffsets],
                    OffsetBlocks};
            _ ->
                {TokenBlock,
                    [{Ordinal, RelativeOffset, Length}],
                    [{CurrentBlock, lists:reverse(CurrentOffsets)}
                        | OffsetBlocks]}
        end,
    {BlockCount, NextDirectoryBlock1, Directory1, CurrentBlock1,
        CurrentOffsets1, OffsetBlocks1, Ordinal + 1}.

client_fill_text_directory(
    BlockNo, BlockCount, TokenEnd, Ordinal, Directory
) when BlockNo < BlockCount, BlockNo * ?TEXT_BLOCK_BYTES < TokenEnd ->
    Start = BlockNo * ?TEXT_BLOCK_BYTES,
    client_fill_text_directory(
        BlockNo + 1,
        BlockCount,
        TokenEnd,
        Ordinal,
        [{BlockNo, Ordinal, Start} | Directory]
    );
client_fill_text_directory(
    BlockNo, _BlockCount, _TokenEnd, _Ordinal, Directory
) ->
    {BlockNo, Directory}.

client_finish_text_directory(BlockNo, BlockCount, _Ordinal, Directory) when
    BlockNo >= BlockCount
->
    Directory;
client_finish_text_directory(BlockNo, BlockCount, Ordinal, Directory) ->
    Start = BlockNo * ?TEXT_BLOCK_BYTES,
    client_finish_text_directory(
        BlockNo + 1,
        BlockCount,
        Ordinal,
        [{BlockNo, Ordinal, Start} | Directory]
    ).

%% Stores that journal externally-encoded terms (term_to_binary bodies)
%% declare decode => external_term so column paths address the decoded
%% term. Decoding happens only at write-time derivation; replay rebuilds
%% from stored key changes and never re-derives.
maybe_decode_object(Object, #{options := #{decode := external_term}}) when
    is_binary(Object)
->
    try
        binary_to_term(Object)
    catch
        _:_ -> Object
    end;
maybe_decode_object(Object, _Schema) ->
    Object.

extract_path(Value, []) ->
    Value;
extract_path(Map, [Key | Rest]) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            extract_path(Value, Rest);
        error ->
            AltKey = alternate_map_key(Key),
            case maps:find(AltKey, Map) of
                {ok, Value} -> extract_path(Value, Rest);
                error -> <<>>
            end
    end;
extract_path(Tuple, [N | Rest]) when
    is_tuple(Tuple), is_integer(N), N > 0, N =< tuple_size(Tuple)
->
    extract_path(element(N, Tuple), Rest);
extract_path(List, [N | Rest]) when
    is_list(List), is_integer(N), N > 0, N =< length(List)
->
    extract_path(lists:nth(N, List), Rest);
extract_path(_Value, _Path) ->
    <<>>.

alternate_map_key(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
alternate_map_key(Key) when is_binary(Key) ->
    try
        binary_to_existing_atom(Key, utf8)
    catch
        _:_ -> Key
    end;
alternate_map_key(Key) ->
    Key.

validate_schema_contract(Schema, Opts, Mode) ->
    case validate_schema_columns(Schema, Opts, Mode) of
        ok ->
            case validate_schema_prefixes(Schema, Opts, Mode) of
                ok -> validate_schema_tokenizer(Schema, Opts);
                Error -> Error
            end;
        Error ->
            Error
    end.

validate_schema_columns(#{columns := Existing}, #{columns := Columns0}, write) ->
    Columns = schema_columns(Columns0),
    case Columns =:= Existing of
        true ->
            ok;
        false ->
            {error, {invalid_fts_contract_change, columns, Existing, Columns}}
    end;
validate_schema_columns(#{columns := _Existing}, _Opts, write) ->
    ok;
validate_schema_columns(#{columns := Existing}, #{columns := Columns0}, search) ->
    Columns = schema_columns(Columns0),
    case first_unknown(Columns, Existing) of
        none -> ok;
        {unknown, Column} -> {error, {fts_parse, unknown_column, Column}}
    end;
validate_schema_columns(_Schema, _Opts, search) ->
    ok.

validate_schema_prefixes(
    #{prefixes := Existing}, #{prefixes := Prefixes0}, write
) ->
    Prefixes = normalise_prefixes(Prefixes0),
    case Prefixes =:= [] orelse Prefixes =:= Existing of
        true ->
            ok;
        false ->
            {error, {invalid_fts_contract_change, prefixes, Existing, Prefixes}}
    end;
validate_schema_prefixes(_Schema, _Opts, write) ->
    ok;
validate_schema_prefixes(
    #{prefixes := Existing}, #{prefixes := Prefixes0}, search
) ->
    Prefixes = normalise_prefixes(Prefixes0),
    case
        Prefixes =:= [] orelse
            lists:all(fun(P) -> lists:member(P, Existing) end, Prefixes)
    of
        true ->
            ok;
        false ->
            {error, {invalid_fts_contract_change, prefixes, Existing, Prefixes}}
    end;
validate_schema_prefixes(_Schema, _Opts, search) ->
    ok.

validate_schema_tokenizer(#{tokenizer := Existing}, Opts) ->
    Tokenizer = tokenizer_description(Opts),
    case Tokenizer =:= Existing of
        true ->
            ok;
        false ->
            {error,
                {invalid_fts_contract_change, tokenizer, Existing, Tokenizer}}
    end.

build_column_terms(Fields, Opts) ->
    [
        {Column, column_token_positions(Mode, Text, Opts)}
     || {Column, Mode, Text} <- Fields
    ].

%% Verbatim columns post the exact field value as one token at position 0;
%% an empty or over-frame value posts nothing. Text columns tokenise
%% through the schema tokenizer; tokens beyond the page format's 16-bit
%% length frame are dropped after position assignment: queries are capped
%% at ?MAX_QUERY_BYTES, so no exact query can ever name such a token, and
%% surviving tokens keep their positions so phrase and NEAR distances are
%% unaffected.
column_token_positions(verbatim, <<>>, _Opts) ->
    [];
column_token_positions(verbatim, Token, _Opts) when byte_size(Token) =< 65535 ->
    [{Token, [0]}];
column_token_positions(verbatim, _Oversized, _Opts) ->
    [];
column_token_positions(text, Text, Opts) ->
    Grouped = client_token_fold(
        fun(Token, Position, _Offset, _Length, Acc) ->
            case byte_size(Token) =< 65535 of
                true ->
                    maps:update_with(
                        Token,
                        fun(Positions) -> [Position | Positions] end,
                        [Position],
                        Acc
                    );
                false ->
                    Acc
            end
        end,
        #{},
        Text,
        Opts
    ),
    [
        {Token, lists:reverse(Positions)}
     || {Token, Positions} <- maps:to_list(Grouped)
    ].

client_cancellable_fold(Fold) ->
    fun(Bucket, Key, Value, Acc) ->
        client_check_cancellation(),
        Fold(Bucket, Key, Value, Acc)
    end.

client_run_fold(Runner) ->
    Result = Runner(),
    client_check_cancellation(),
    Result.

client_check_cancellation() ->
    case erlang:get(ash_leveled_search_cancellation) of
        #{
            owner := Owner,
            deadline_at := DeadlineAt,
            cancel_token := CancelToken
        } ->
            receive
                {ash_leveled_cancel, CancelToken, Reason} ->
                    throw({fts_error, {search_cancelled, Reason}})
            after 0 ->
                case erlang:is_process_alive(Owner) of
                    false ->
                        throw({fts_error, {search_cancelled, caller_down}});
                    true ->
                        case
                            DeadlineAt =/= infinity andalso
                                erlang:monotonic_time(millisecond) >= DeadlineAt
                        of
                            true ->
                                throw(
                                    {fts_error, {search_cancelled, deadline}}
                                );
                            false ->
                                ok
                        end
                end
            end;
        _ ->
            ok
    end.

normalise_search_options(Opts0, Schema) ->
    case options_map(Opts0) of
        {ok, Opts0Map} ->
            SearchOpts0 = maps:merge(
                schema_tokenizer_options(Schema), Opts0Map
            ),
            case validate_search_options(SearchOpts0) of
                {ok, Opts1} ->
                    case validate_schema_contract(Schema, Opts1, search) of
                        ok ->
                            Prefixes =
                                case maps:is_key(prefixes, Opts0Map) of
                                    true -> maps:get(prefixes, Opts1);
                                    false -> maps:get(prefixes, Schema)
                                end,
                            {ok, Opts1#{
                                columns => maps:get(
                                    columns, Opts1, maps:get(columns, Schema)
                                ),
                                prefixes => Prefixes
                            }};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        error ->
            {error, invalid_fts_options}
    end.

schema_tokenizer_options(Schema) ->
    maps:with(
        [tokenizer, remove_diacritics, tokenchars, separators, stopwords],
        maps:get(options, Schema, #{})
    ).

options_map(Opts) when is_map(Opts) ->
    {ok, Opts};
options_map(Opts) when is_list(Opts) ->
    try
        {ok, maps:from_list(Opts)}
    catch
        _:_ -> error
    end;
options_map(_Opts) ->
    error.

validate_search_options(Opts) ->
    case validate_search_option_list(maps:to_list(Opts)) of
        ok -> validate_search_window(normalise_options(Opts));
        {error, Reason} -> {error, Reason};
        error -> {error, invalid_fts_options}
    end.

validate_search_option_list([]) ->
    ok;
validate_search_option_list([{columns, Columns} | Rest]) ->
    case valid_columns(Columns) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{prefixes, Prefixes} | Rest]) ->
    case valid_prefixes(Prefixes) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{tokenizer, Tokenizer} | Rest]) ->
    case valid_tokenizer(Tokenizer) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{remove_diacritics, Value} | Rest]) ->
    case valid_remove_diacritics(Value) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{tokenchars, Value} | Rest]) ->
    case valid_char_option(Value) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{separators, Value} | Rest]) ->
    case valid_char_option(Value) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{stopwords, Words} | Rest]) when is_list(Words) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, none} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, bm25} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, _Other} | _Rest]) ->
    {error, invalid_rank_option};
validate_search_option_list([{grouping, grouped} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{grouping, ungrouped} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{grouping, _Other} | _Rest]) ->
    {error, invalid_grouping_option};
validate_search_option_list([{limit, Limit} | Rest]) when
    is_integer(Limit), Limit >= 0
->
    validate_search_option_list(Rest);
validate_search_option_list([{offset, Offset} | Rest]) when
    is_integer(Offset), Offset >= 0
->
    validate_search_option_list(Rest);
%% return_positions does not cap match_count.  It returns at most
%% MAX_RETURN_POSITIONS ordinals per hit as deterministic ordered prefixes;
%% phrase and NEAR ordinals are their match starts.
validate_search_option_list([{return_positions, Bool} | Rest]) when
    is_boolean(Bool)
->
    validate_search_option_list(Rest);
validate_search_option_list([{return_terms, Bool} | Rest]) when
    is_boolean(Bool)
->
    validate_search_option_list(Rest);
validate_search_option_list([{return_count, Bool} | Rest]) when
    is_boolean(Bool)
->
    validate_search_option_list(Rest);
validate_search_option_list([{resolve_hits, Bool} | Rest]) when
    is_boolean(Bool)
->
    validate_search_option_list(Rest);
validate_search_option_list([{impact, Bool} | Rest]) when is_boolean(Bool) ->
    validate_search_option_list(Rest);
validate_search_option_list([{count_only, Bool} | Rest]) when is_boolean(Bool) ->
    validate_search_option_list(Rest);
validate_search_option_list([{page_only, Bool} | Rest]) when is_boolean(Bool) ->
    validate_search_option_list(Rest);
validate_search_option_list([{impact_facet, Facet} | Rest]) when
    is_list(Facet) orelse Facet =:= undefined orelse Facet =:= nil
->
    validate_search_option_list(Rest);
validate_search_option_list([{rank_tie_fields, Fields} | Rest]) when
    is_list(Fields)
->
    case lists:all(fun erlang:is_atom/1, Fields) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{_Other, _Value} | _Rest]) ->
    error.

validate_search_window(Opts) ->
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    Offset = maps:get(offset, Opts, 0),
    case Limit =< ?MAX_LIMIT andalso Offset + Limit =< ?MAX_WINDOW of
        true -> {ok, Opts};
        false -> {error, fts_query_limit_exceeded}
    end.

normalise_options(Opts) ->
    Opts0 = Opts#{
        prefixes => normalise_prefixes(maps:get(prefixes, Opts, [])),
        tokenchars => normalise_char_list(maps:get(tokenchars, Opts, [])),
        separators => normalise_char_list(maps:get(separators, Opts, [])),
        remove_diacritics => maps:get(remove_diacritics, Opts, false),
        tokenizer => normalise_tokenizer(maps:get(tokenizer, Opts, unicode61))
    },
    Opts1 = Opts0#{
        stopwords => normalise_stopwords(maps:get(stopwords, Opts, []), Opts0)
    },
    case maps:find(columns, Opts1) of
        {ok, Columns} -> Opts1#{columns => schema_columns(Columns)};
        error -> Opts1
    end.

normalise_text(T) when is_binary(T) ->
    T;
normalise_text(T) when is_list(T) ->
    unicode:characters_to_binary(T, utf8);
normalise_text(T) ->
    client_term_binary(T).

valid_columns(Columns) when is_list(Columns), Columns =/= [] ->
    true;
valid_columns(_Columns) ->
    false.

valid_prefixes(Prefixes) when is_list(Prefixes) ->
    lists:all(fun(P) -> is_integer(P) andalso P > 0 end, Prefixes);
valid_prefixes(_Prefixes) ->
    false.

valid_tokenizer(unicode61) -> true;
valid_tokenizer(<<"unicode61">>) -> true;
valid_tokenizer("unicode61") -> true;
valid_tokenizer(_Tokenizer) -> false.

valid_remove_diacritics(false) -> true;
valid_remove_diacritics(true) -> true;
valid_remove_diacritics(0) -> true;
valid_remove_diacritics(1) -> true;
valid_remove_diacritics(2) -> true;
valid_remove_diacritics(_Other) -> false.

valid_char_option(Bin) when is_binary(Bin) -> true;
valid_char_option(C) when is_integer(C) -> true;
valid_char_option(List) when is_list(List) ->
    lists:all(fun valid_char_option/1, List);
valid_char_option(_Other) ->
    false.

normalise_prefixes(Prefixes) ->
    lists:usort([P || P <- Prefixes, is_integer(P), P > 0]).

normalise_char_list(Bin) when is_binary(Bin) ->
    unicode_chars(Bin);
normalise_char_list(C) when is_integer(C) ->
    [C];
normalise_char_list(List) when is_list(List) ->
    lists:append([normalise_char_list(Item) || Item <- List]);
normalise_char_list(_Other) ->
    [].

normalise_stopwords(Words, Opts) ->
    [
        Token
     || Word <- Words, {Token, _Pos} <- tokenize(Word, Opts#{stopwords => []})
    ].

normalise_tokenizer(unicode61) -> unicode61;
normalise_tokenizer(<<"unicode61">>) -> unicode61;
normalise_tokenizer("unicode61") -> unicode61.

tokenizer_description(Opts) ->
    #{
        tokenizer => normalise_tokenizer(maps:get(tokenizer, Opts, unicode61)),
        remove_diacritics => maps:get(remove_diacritics, Opts, false),
        tokenchars => normalise_char_list(maps:get(tokenchars, Opts, [])),
        separators => normalise_char_list(maps:get(separators, Opts, [])),
        stopwords => maps:get(stopwords, Opts, [])
    }.

schema_columns(Columns) ->
    lists:usort([normalise_column(C) || C <- Columns]).

normalise_column(C) when is_binary(C) ->
    normalise_column_binary(C);
normalise_column(C) when is_atom(C) ->
    normalise_column_binary(atom_to_binary(C, utf8));
normalise_column(C) when is_list(C) ->
    normalise_column_binary(unicode:characters_to_binary(C, utf8));
normalise_column(C) ->
    normalise_column_binary(client_term_binary(C)).

normalise_column_binary(Bin) ->
    unicode:characters_to_binary(lower_chars(unicode_chars(Bin)), utf8).

normalise_index(I) when is_binary(I) -> I;
normalise_index(I) when is_atom(I) -> atom_to_binary(I, utf8);
normalise_index(I) when is_list(I) -> unicode:characters_to_binary(I, utf8);
normalise_index(I) -> client_term_binary(I).

client_term_binary(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).

first_unknown([], _Known) ->
    none;
first_unknown([Item | Rest], Known) ->
    case lists:member(Item, Known) of
        true -> first_unknown(Rest, Known);
        false -> {unknown, Item}
    end.

%% Return the index tokenizer's normalized token stream together with source
%% coordinates.  Coordinates address Text0 itself, before the SQLite UTF-8
%% compatibility rewrite and before token normalization.  In particular, the
%% F0 9F 92 truncation quirk maps the synthesized two-byte U+07D2 character
%% back to all three original input bytes.
-spec tokenize_with_offsets(binary(), map()) ->
    [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()}].
tokenize_with_offsets(Text, Opts) when is_binary(Text), is_map(Opts) ->
    case maps:get(presize, Opts, false) andalso byte_size(Text) >= 262144 of
        true ->
            Words = (byte_size(Text) div 6) * 12 + 32768,
            OldHeap = erlang:process_flag(min_heap_size, Words),
            OldSweep = erlang:process_flag(fullsweep_after, 65535),
            erlang:garbage_collect(),
            try client_tokenize_with_offsets(Text, Opts)
            after
                _ = erlang:process_flag(min_heap_size, OldHeap),
                _ = erlang:process_flag(fullsweep_after, OldSweep)
            end;
        false ->
            client_tokenize_with_offsets(Text, Opts)
    end.

client_tokenize_with_offsets(Text, Opts) ->
    Stopwords = maps:get(stopwords, Opts, []),
    case
        maps:get(tokenchars, Opts, []) =:= [] andalso
            maps:get(separators, Opts, []) =:= []
    of
        true ->
            Tokens = client_ascii_tokens(Text, Text, 0, 0, Opts),
            case Stopwords of
                [] -> Tokens;
                _ -> [
                    Token
                 || {Value, _, _, _} = Token <- Tokens,
                    not lists:member(Value, Stopwords)
                ]
            end;
        false ->
            Tokens = unicode_tokens_with_offsets(
                Text, 0, Opts, Stopwords, <<>>, undefined, 0, []
            ),
            [compat_source_range(Text, Token) || Token <- Tokens]
    end.

-ifdef(TEST).
tokenize_fold_for_test(Fun, Acc, Text, Opts) ->
    client_token_fold(Fun, Acc, Text, Opts).
-endif.

client_token_fold(Fun, Acc, Text, Opts) when
    is_function(Fun, 5), is_binary(Text), is_map(Opts)
->
    case
        maps:get(tokenchars, Opts, []) =:= [] andalso
            maps:get(separators, Opts, []) =:= []
    of
        true ->
            client_ascii_fold(
                Text,
                Text,
                0,
                0,
                Opts,
                maps:get(stopwords, Opts, []),
                Fun,
                Acc
            );
        false ->
            Tokens = unicode_tokens_with_offsets(
                Text,
                0,
                Opts,
                maps:get(stopwords, Opts, []),
                <<>>,
                undefined,
                0,
                []
            ),
            lists:foldl(
                fun(Token0, Inner) ->
                    {Token, Ordinal, Offset, Length} =
                        compat_source_range(Text, Token0),
                    Fun(Token, Ordinal, Offset, Length, Inner)
                end,
                Acc,
                Tokens
            )
    end.

%% Lane-05 ASCII-run scanner. The common lane records only a start offset and
%% extracts one sub-binary at the boundary; uppercase runs fold in one integer
%% OR for lengths up to seven bytes. Unicode machinery is entered only at the
%% first non-ASCII byte and hands back at the next boundary.
client_ascii_tokens(<<C, Rest/binary>>, Text, Offset, Ordinal, Opts) when
    C >= $a, C =< $z
->
    client_ascii_lower(Rest, Text, Offset + 1, Offset, Ordinal, Opts);
client_ascii_tokens(<<C, Rest/binary>>, Text, Offset, Ordinal, Opts) when
    C >= $0, C =< $9
->
    client_ascii_lower(Rest, Text, Offset + 1, Offset, Ordinal, Opts);
client_ascii_tokens(<<C, Rest/binary>>, Text, Offset, Ordinal, Opts) when
    C >= $A, C =< $Z
->
    client_ascii_upper(Rest, Text, Offset + 1, Offset, Ordinal, Opts);
client_ascii_tokens(<<C, Rest/binary>>, Text, Offset, Ordinal, Opts) when
    C < 128
->
    client_ascii_tokens(Rest, Text, Offset + 1, Ordinal, Opts);
client_ascii_tokens(<<>>, _Text, _Offset, _Ordinal, _Opts) ->
    [];
client_ascii_tokens(Binary, Text, Offset, Ordinal, Opts) ->
    client_ascii_slow(
        Binary, Text, Offset, <<>>, false, undefined, Ordinal, Opts
    ).

client_ascii_lower(<<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts) when
    (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)
->
    client_ascii_lower(Rest, Text, Offset + 1, Start, Ordinal, Opts);
client_ascii_lower(<<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts) when
    C >= $A, C =< $Z
->
    client_ascii_upper(Rest, Text, Offset + 1, Start, Ordinal, Opts);
client_ascii_lower(<<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts) when
    C < 128
->
    Length = Offset - Start,
    [
        {binary_part(Text, Start, Length), Ordinal, Start, Length}
        | client_ascii_tokens(Rest, Text, Offset + 1, Ordinal + 1, Opts)
    ];
client_ascii_lower(<<>>, Text, Offset, Start, Ordinal, _Opts) ->
    Length = Offset - Start,
    [{binary_part(Text, Start, Length), Ordinal, Start, Length}];
client_ascii_lower(Binary, Text, Offset, Start, Ordinal, Opts) ->
    client_ascii_slow(
        Binary,
        Text,
        Offset,
        client_ascii_lowercase(binary_part(Text, Start, Offset - Start)),
        false,
        Start,
        Ordinal,
        Opts
    ).

client_ascii_upper(<<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts) when
    (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9) orelse
        (C >= $A andalso C =< $Z)
->
    client_ascii_upper(Rest, Text, Offset + 1, Start, Ordinal, Opts);
client_ascii_upper(<<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts) when
    C < 128
->
    Length = Offset - Start,
    [
        {client_ascii_lowercase(binary_part(Text, Start, Length)),
            Ordinal, Start, Length}
        | client_ascii_tokens(Rest, Text, Offset + 1, Ordinal + 1, Opts)
    ];
client_ascii_upper(<<>>, Text, Offset, Start, Ordinal, _Opts) ->
    Length = Offset - Start,
    [{client_ascii_lowercase(binary_part(Text, Start, Length)),
        Ordinal, Start, Length}];
client_ascii_upper(Binary, Text, Offset, Start, Ordinal, Opts) ->
    client_ascii_slow(
        Binary,
        Text,
        Offset,
        client_ascii_lowercase(binary_part(Text, Start, Offset - Start)),
        false,
        Start,
        Ordinal,
        Opts
    ).

client_ascii_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal, Opts
) when C >= $a, C =< $z ->
    client_ascii_slow(
        Rest,
        Text,
        Offset + 1,
        <<Token/binary, C>>,
        NonAscii,
        token_start(Start, Offset),
        Ordinal,
        Opts
    );
client_ascii_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal, Opts
) when C >= $0, C =< $9 ->
    client_ascii_slow(
        Rest,
        Text,
        Offset + 1,
        <<Token/binary, C>>,
        NonAscii,
        token_start(Start, Offset),
        Ordinal,
        Opts
    );
client_ascii_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal, Opts
) when C >= $A, C =< $Z ->
    client_ascii_slow(
        Rest,
        Text,
        Offset + 1,
        <<Token/binary, (C bor 16#20)>>,
        NonAscii,
        token_start(Start, Offset),
        Ordinal,
        Opts
    );
client_ascii_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal, Opts
) when C < 128 ->
    case client_ascii_slow_token(
        Token, NonAscii, Start, Offset, Text, Ordinal, Opts
    ) of
        skip ->
            client_ascii_tokens(Rest, Text, Offset + 1, Ordinal, Opts);
        Term ->
            [Term | client_ascii_tokens(
                Rest, Text, Offset + 1, Ordinal + 1, Opts
            )]
    end;
client_ascii_slow(
    <<16#F0, 16#9F, 16#92, Next, _/binary>> = Binary,
    Text,
    Offset,
    Token,
    _NonAscii,
    Start,
    Ordinal,
    Opts
) when Next band 16#C0 =/= 16#80 ->
    <<_:3/binary, Rest/binary>> = Binary,
    client_ascii_slow(
        Rest,
        Text,
        Offset + 3,
        <<Token/binary, 16#DF, 16#92>>,
        true,
        token_start(Start, Offset),
        Ordinal,
        Opts
    );
client_ascii_slow(
    <<Codepoint/utf8, Rest/binary>> = Binary,
    Text,
    Offset,
    Token,
    NonAscii,
    Start,
    Ordinal,
    Opts
) ->
    Bytes = byte_size(Binary) - byte_size(Rest),
    case unicode_token_char(Codepoint, Opts) of
        true ->
            <<Character:Bytes/binary, _/binary>> = Binary,
            client_ascii_slow(
                Rest,
                Text,
                Offset + Bytes,
                <<Token/binary, Character/binary>>,
                true,
                token_start(Start, Offset),
                Ordinal,
                Opts
            );
        false ->
            case client_ascii_slow_token(
                Token, NonAscii, Start, Offset, Text, Ordinal, Opts
            ) of
                skip ->
                    client_ascii_tokens(
                        Rest, Text, Offset + Bytes, Ordinal, Opts
                    );
                Term ->
                    [Term | client_ascii_tokens(
                        Rest, Text, Offset + Bytes, Ordinal + 1, Opts
                    )]
            end
    end;
client_ascii_slow(
    <<_Bad, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal, Opts
) ->
    case client_ascii_slow_token(
        Token, NonAscii, Start, Offset, Text, Ordinal, Opts
    ) of
        skip -> client_ascii_tokens(Rest, Text, Offset + 1, Ordinal, Opts);
        Term -> [Term | client_ascii_tokens(
            Rest, Text, Offset + 1, Ordinal + 1, Opts
        )]
    end;
client_ascii_slow(
    <<>>, Text, Offset, Token, NonAscii, Start, Ordinal, Opts
) ->
    case client_ascii_slow_token(
        Token, NonAscii, Start, Offset, Text, Ordinal, Opts
    ) of
        skip -> [];
        Term -> [Term]
    end.

client_ascii_slow_token(<<>>, _NonAscii, _Start, _End, _Text, _Ordinal, _Opts) ->
    skip;
client_ascii_slow_token(Token, false, Start, End, Text, Ordinal, _Opts) ->
    client_ascii_compat(Text, Token, Ordinal, Start, End - Start);
client_ascii_slow_token(Token, true, Start, End, Text, Ordinal, Opts) ->
    case normalise_token(Token, Opts) of
        <<>> -> skip;
        Normalised ->
            client_ascii_compat(
                Text, Normalised, Ordinal, Start, End - Start
            )
    end.

client_ascii_compat(Text, Token, Ordinal, Offset, Length) when
    Offset + Length < byte_size(Text), Length >= 3
->
    case binary_part(Text, Offset + Length - 3, 3) of
        <<16#F0, 16#9F, 16#92>> ->
            {Token, Ordinal, Offset, Length + 1};
        _ ->
            {Token, Ordinal, Offset, Length}
    end;
client_ascii_compat(_Text, Token, Ordinal, Offset, Length) ->
    {Token, Ordinal, Offset, Length}.

client_ascii_lowercase(<<A>>) -> <<(A bor 16#20)>>;
client_ascii_lowercase(<<A:16>>) -> <<(A bor 16#2020):16>>;
client_ascii_lowercase(<<A:24>>) -> <<(A bor 16#202020):24>>;
client_ascii_lowercase(<<A:32>>) -> <<(A bor 16#20202020):32>>;
client_ascii_lowercase(<<A:40>>) -> <<(A bor 16#2020202020):40>>;
client_ascii_lowercase(<<A:48>>) -> <<(A bor 16#202020202020):48>>;
client_ascii_lowercase(<<A:56>>) -> <<(A bor 16#20202020202020):56>>;
client_ascii_lowercase(<<>>) -> <<>>;
client_ascii_lowercase(Binary) -> client_ascii_lowercase(Binary, <<>>).

client_ascii_lowercase(<<Word:32, Rest/binary>>, Acc) ->
    client_ascii_lowercase(
        Rest, <<Acc/binary, (Word bor 16#20202020):32>>
    );
client_ascii_lowercase(<<C, Rest/binary>>, Acc) ->
    client_ascii_lowercase(Rest, <<Acc/binary, (C bor 16#20)>>);
client_ascii_lowercase(<<>>, Acc) ->
    Acc.

client_ascii_fold(
    <<C, Rest/binary>>, Text, Offset, Ordinal, Opts, Stopwords, Fun, Acc
) when C >= $a, C =< $z; C >= $0, C =< $9 ->
    client_ascii_fold_lower(
        Rest, Text, Offset + 1, Offset, Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold(
    <<C, Rest/binary>>, Text, Offset, Ordinal, Opts, Stopwords, Fun, Acc
) when C >= $A, C =< $Z ->
    client_ascii_fold_upper(
        Rest, Text, Offset + 1, Offset, Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold(
    <<C, Rest/binary>>, Text, Offset, Ordinal, Opts, Stopwords, Fun, Acc
) when C < 128 ->
    client_ascii_fold(
        Rest, Text, Offset + 1, Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold(<<>>, _Text, _Offset, _Ordinal, _Opts, _Stopwords, _Fun, Acc) ->
    Acc;
client_ascii_fold(Binary, Text, Offset, Ordinal, Opts, Stopwords, Fun, Acc) ->
    client_ascii_fold_slow(
        Binary,
        Text,
        Offset,
        <<>>,
        false,
        undefined,
        Ordinal,
        Opts,
        Stopwords,
        Fun,
        Acc
    ).

client_ascii_fold_lower(
    <<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) when (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9) ->
    client_ascii_fold_lower(
        Rest, Text, Offset + 1, Start, Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold_lower(
    <<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) when C >= $A, C =< $Z ->
    client_ascii_fold_upper(
        Rest, Text, Offset + 1, Start, Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold_lower(
    <<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) when C < 128 ->
    Length = Offset - Start,
    {NextOrdinal, NextAcc} = client_ascii_fold_emit(
        binary_part(Text, Start, Length),
        Ordinal,
        Start,
        Length,
        Stopwords,
        Fun,
        Acc
    ),
    client_ascii_fold(
        Rest,
        Text,
        Offset + 1,
        NextOrdinal,
        Opts,
        Stopwords,
        Fun,
        NextAcc
    );
client_ascii_fold_lower(
    <<>>, Text, Offset, Start, Ordinal, _Opts, Stopwords, Fun, Acc
) ->
    Length = Offset - Start,
    {_NextOrdinal, NextAcc} = client_ascii_fold_emit(
        binary_part(Text, Start, Length),
        Ordinal,
        Start,
        Length,
        Stopwords,
        Fun,
        Acc
    ),
    NextAcc;
client_ascii_fold_lower(
    Binary, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) ->
    client_ascii_fold_slow(
        Binary,
        Text,
        Offset,
        client_ascii_lowercase(binary_part(Text, Start, Offset - Start)),
        false,
        Start,
        Ordinal,
        Opts,
        Stopwords,
        Fun,
        Acc
    ).

client_ascii_fold_upper(
    <<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) when
    (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9) orelse
        (C >= $A andalso C =< $Z)
->
    client_ascii_fold_upper(
        Rest, Text, Offset + 1, Start, Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold_upper(
    <<C, Rest/binary>>, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) when C < 128 ->
    Length = Offset - Start,
    {NextOrdinal, NextAcc} = client_ascii_fold_emit(
        client_ascii_lowercase(binary_part(Text, Start, Length)),
        Ordinal,
        Start,
        Length,
        Stopwords,
        Fun,
        Acc
    ),
    client_ascii_fold(
        Rest,
        Text,
        Offset + 1,
        NextOrdinal,
        Opts,
        Stopwords,
        Fun,
        NextAcc
    );
client_ascii_fold_upper(
    <<>>, Text, Offset, Start, Ordinal, _Opts, Stopwords, Fun, Acc
) ->
    Length = Offset - Start,
    {_NextOrdinal, NextAcc} = client_ascii_fold_emit(
        client_ascii_lowercase(binary_part(Text, Start, Length)),
        Ordinal,
        Start,
        Length,
        Stopwords,
        Fun,
        Acc
    ),
    NextAcc;
client_ascii_fold_upper(
    Binary, Text, Offset, Start, Ordinal, Opts, Stopwords, Fun, Acc
) ->
    client_ascii_fold_slow(
        Binary,
        Text,
        Offset,
        client_ascii_lowercase(binary_part(Text, Start, Offset - Start)),
        false,
        Start,
        Ordinal,
        Opts,
        Stopwords,
        Fun,
        Acc
    ).

client_ascii_fold_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal,
    Opts, Stopwords, Fun, Acc
) when C >= $a, C =< $z; C >= $0, C =< $9 ->
    client_ascii_fold_slow(
        Rest, Text, Offset + 1, <<Token/binary, C>>, NonAscii,
        token_start(Start, Offset), Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal,
    Opts, Stopwords, Fun, Acc
) when C >= $A, C =< $Z ->
    client_ascii_fold_slow(
        Rest, Text, Offset + 1, <<Token/binary, (C bor 16#20)>>, NonAscii,
        token_start(Start, Offset), Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold_slow(
    <<C, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal,
    Opts, Stopwords, Fun, Acc
) when C < 128 ->
    {NextOrdinal, NextAcc} = client_ascii_fold_slow_emit(
        Token, NonAscii, Start, Offset, Text, Ordinal, Opts, Stopwords,
        Fun, Acc
    ),
    client_ascii_fold(
        Rest, Text, Offset + 1, NextOrdinal, Opts, Stopwords, Fun, NextAcc
    );
client_ascii_fold_slow(
    <<16#F0, 16#9F, 16#92, Next, _/binary>> = Binary,
    Text, Offset, Token, _NonAscii, Start, Ordinal, Opts, Stopwords, Fun, Acc
) when Next band 16#C0 =/= 16#80 ->
    <<_:3/binary, Rest/binary>> = Binary,
    client_ascii_fold_slow(
        Rest, Text, Offset + 3, <<Token/binary, 16#DF, 16#92>>, true,
        token_start(Start, Offset), Ordinal, Opts, Stopwords, Fun, Acc
    );
client_ascii_fold_slow(
    <<Codepoint/utf8, Rest/binary>> = Binary,
    Text, Offset, Token, NonAscii, Start, Ordinal, Opts, Stopwords, Fun, Acc
) ->
    Bytes = byte_size(Binary) - byte_size(Rest),
    case unicode_token_char(Codepoint, Opts) of
        true ->
            <<Character:Bytes/binary, _/binary>> = Binary,
            client_ascii_fold_slow(
                Rest, Text, Offset + Bytes, <<Token/binary, Character/binary>>,
                true, token_start(Start, Offset), Ordinal, Opts, Stopwords,
                Fun, Acc
            );
        false ->
            {NextOrdinal, NextAcc} = client_ascii_fold_slow_emit(
                Token, NonAscii, Start, Offset, Text, Ordinal, Opts,
                Stopwords, Fun, Acc
            ),
            client_ascii_fold(
                Rest, Text, Offset + Bytes, NextOrdinal, Opts, Stopwords,
                Fun, NextAcc
            )
    end;
client_ascii_fold_slow(
    <<_Bad, Rest/binary>>, Text, Offset, Token, NonAscii, Start, Ordinal,
    Opts, Stopwords, Fun, Acc
) ->
    {NextOrdinal, NextAcc} = client_ascii_fold_slow_emit(
        Token, NonAscii, Start, Offset, Text, Ordinal, Opts, Stopwords,
        Fun, Acc
    ),
    client_ascii_fold(
        Rest, Text, Offset + 1, NextOrdinal, Opts, Stopwords, Fun, NextAcc
    );
client_ascii_fold_slow(
    <<>>, Text, Offset, Token, NonAscii, Start, Ordinal,
    Opts, Stopwords, Fun, Acc
) ->
    {_NextOrdinal, NextAcc} = client_ascii_fold_slow_emit(
        Token, NonAscii, Start, Offset, Text, Ordinal, Opts, Stopwords,
        Fun, Acc
    ),
    NextAcc.

client_ascii_fold_slow_emit(
    Token, NonAscii, Start, End, Text, Ordinal, Opts, Stopwords, Fun, Acc
) ->
    case client_ascii_slow_token(
        Token, NonAscii, Start, End, Text, Ordinal, Opts
    ) of
        skip -> {Ordinal, Acc};
        {Value, Ordinal, Offset, Length} ->
            client_ascii_fold_emit(
                Value, Ordinal, Offset, Length, Stopwords, Fun, Acc
            )
    end.

client_ascii_fold_emit(Token, Ordinal, Offset, Length, Stopwords, Fun, Acc) ->
    NextAcc = case lists:member(Token, Stopwords) of
        true -> Acc;
        false -> Fun(Token, Ordinal, Offset, Length, Acc)
    end,
    {Ordinal + 1, NextAcc}.

%% sqlite_utf8_compat/1 recognizes the malformed three-byte prefix only by
%% looking at the following non-continuation byte.  If a token ends at that
%% synthesized character, include the one-byte lookahead in its source range
%% so tokenizing binary:part(Input, Offset, Length) reproduces the same token.
%% When the token continues past the quirk its ordinary range already includes
%% the lookahead and no extension is needed.
compat_source_range(
    Text, {Token, Pos, Offset, Length} = WithOffset
) when Offset + Length < byte_size(Text), Length >= 3 ->
    End = Offset + Length,
    case binary:part(Text, End - 3, 3) of
        <<16#F0, 16#9F, 16#92>> ->
            {Token, Pos, Offset, Length + 1};
        _Other ->
            WithOffset
    end;
compat_source_range(_Text, WithOffset) ->
    WithOffset.

tokenize(Text0, Opts) ->
    Text = sqlite_utf8_compat(normalise_text(Text0)),
    case
        maps:get(tokenchars, Opts, []) =:= [] andalso
            maps:get(separators, Opts, []) =:= []
    of
        true ->
            Stopwords = maps:get(stopwords, Opts, []),
            fast_tokens(Text, Opts, Stopwords, <<>>, false, 0, []);
        false ->
            tokenize_unicode(Text, Opts)
    end.

%% SQLite's UTF-8 reader has one long-standing compatibility quirk included in
%% the differential corpus: a truncated F0 9F 92 sequence is decoded as U+07D2
%% (DF 92) when the following byte is not a continuation.  Preserve that exact
%% behaviour; all other malformed bytes remain hard token boundaries.
sqlite_utf8_compat(Bin) when is_binary(Bin) ->
    sqlite_utf8_compat(Bin, <<>>).

sqlite_utf8_compat(<<16#F0, 16#9F, 16#92, Next, Rest/binary>>, Acc) when
    Next band 16#C0 =/= 16#80
->
    sqlite_utf8_compat(<<Next, Rest/binary>>, <<Acc/binary, 16#DF, 16#92>>);
sqlite_utf8_compat(<<Byte, Rest/binary>>, Acc) ->
    sqlite_utf8_compat(Rest, <<Acc/binary, Byte>>);
sqlite_utf8_compat(<<>>, Acc) ->
    Acc.

%% Custom tokenchars/separators use the same classifier and normalization as
%% tokenize_unicode/2, with the original byte range carried beside each token.
unicode_tokens_with_offsets(
    <<16#F0, 16#9F, 16#92, Next, Rest/binary>>,
    Offset,
    Opts,
    SW,
    Tok,
    Start,
    Pos,
    Acc
) when Next band 16#C0 =/= 16#80 ->
    unicode_offset_char(
        16#7D2,
        <<16#DF, 16#92>>,
        <<Next, Rest/binary>>,
        Offset,
        Offset + 3,
        Opts,
        SW,
        Tok,
        Start,
        Pos,
        Acc
    );
unicode_tokens_with_offsets(
    <<CP/utf8, Rest/binary>> = Bin,
    Offset,
    Opts,
    SW,
    Tok,
    Start,
    Pos,
    Acc
) ->
    CharLen = byte_size(Bin) - byte_size(Rest),
    <<Char:CharLen/binary, _/binary>> = Bin,
    unicode_offset_char(
        CP,
        Char,
        Rest,
        Offset,
        Offset + CharLen,
        Opts,
        SW,
        Tok,
        Start,
        Pos,
        Acc
    );
unicode_tokens_with_offsets(
    <<_Bad, Rest/binary>>, Offset, Opts, SW, Tok, Start, Pos, Acc
) ->
    {Pos1, Acc1} = unicode_offset_flush(
        Tok, Start, Offset, Opts, SW, Pos, Acc
    ),
    unicode_tokens_with_offsets(
        Rest, Offset + 1, Opts, SW, <<>>, undefined, Pos1, Acc1
    );
unicode_tokens_with_offsets(
    <<>>, Offset, Opts, SW, Tok, Start, Pos, Acc
) ->
    {_Pos1, Acc1} = unicode_offset_flush(
        Tok, Start, Offset, Opts, SW, Pos, Acc
    ),
    lists:reverse(Acc1).

unicode_offset_char(
    CP,
    Char,
    Rest,
    Offset,
    NextOffset,
    Opts,
    SW,
    Tok,
    Start,
    Pos,
    Acc
) ->
    case token_char(CP, Opts) of
        true ->
            unicode_tokens_with_offsets(
                Rest,
                NextOffset,
                Opts,
                SW,
                <<Tok/binary, Char/binary>>,
                token_start(Start, Offset),
                Pos,
                Acc
            );
        false ->
            {Pos1, Acc1} = unicode_offset_flush(
                Tok, Start, Offset, Opts, SW, Pos, Acc
            ),
            unicode_tokens_with_offsets(
                Rest, NextOffset, Opts, SW, <<>>, undefined, Pos1, Acc1
            )
    end.

unicode_offset_flush(<<>>, _Start, _End, _Opts, _SW, Pos, Acc) ->
    {Pos, Acc};
unicode_offset_flush(Tok, Start, End, Opts, SW, Pos, Acc) ->
    Norm = normalise_token(Tok, Opts),
    case {Norm =:= <<>>, lists:member(Norm, SW)} of
        {true, _} -> {Pos, Acc};
        {false, true} -> {Pos + 1, Acc};
        {false, false} -> {Pos + 1, [{Norm, Pos, Start, End - Start} | Acc]}
    end.

token_start(undefined, Offset) -> Offset;
token_start(Start, _Offset) -> Start.

tokenize_unicode(Text0, Opts) ->
    %% Keep malformed input explicit.  Dropping a bad byte before this fold
    %% concatenates the valid runs on either side and changes token identity.
    Text = unicode_chars_with_boundaries(normalise_text(Text0)),
    Stopwords = maps:get(stopwords, Opts, []),
    {Tokens, Current, Pos} =
        lists:foldl(
            fun
                (invalid_utf8, {Acc, Current, Pos}) ->
                    finish_token(Acc, Current, Pos, Stopwords, Opts);
                (Char, {Acc, Current, Pos}) ->
                    case token_char(Char, Opts) of
                        true ->
                            {Acc,
                                lists:reverse(normalise_char(Char, Opts)) ++
                                    Current,
                                Pos};
                        false ->
                            finish_token(Acc, Current, Pos, Stopwords, Opts)
                    end
            end,
            {[], [], 0},
            Text
        ),
    {Final, _Current2, _Pos2} = finish_token(
        Tokens, Current, Pos, Stopwords, Opts
    ),
    lists:reverse(Final).

%% Fast tokenizer for the common case where no custom tokenchars/separators are
%% configured. Runs of ASCII alphanumerics are classified and lowercased with
%% byte comparisons only (no per-character Unicode table lookups). Non-ASCII
%% characters fall back to the Unicode classifier, and any token that contained
%% a non-ASCII byte is normalised through normalise_token/2, so output is
%% byte-identical to tokenize_unicode/2 (unicode61 + remove_diacritics parity
%% with SQLite FTS5).
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when
    C >= $a, C =< $z
->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, C>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when
    C >= $0, C =< $9
->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, C>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when
    C >= $A, C =< $Z
->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, (C bor 16#20)>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when C < 128 ->
    {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1);
fast_tokens(<<CP/utf8, Rest/binary>> = Bin, Opts, SW, Tok, NA, Pos, Acc) ->
    case unicode_token_char(CP, Opts) of
        true ->
            CharLen = byte_size(Bin) - byte_size(Rest),
            <<Char:CharLen/binary, _/binary>> = Bin,
            fast_tokens(
                Rest, Opts, SW, <<Tok/binary, Char/binary>>, true, Pos, Acc
            );
        false ->
            {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
            fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1)
    end;
fast_tokens(<<_Bad, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) ->
    {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1);
fast_tokens(<<>>, Opts, SW, Tok, NA, Pos, Acc) ->
    {_Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    lists:reverse(Acc1).

fast_flush(<<>>, _NA, _Opts, _SW, Pos, Acc) ->
    {Pos, Acc};
fast_flush(Tok, false, _Opts, SW, Pos, Acc) ->
    case lists:member(Tok, SW) of
        true -> {Pos + 1, Acc};
        false -> {Pos + 1, [{Tok, Pos} | Acc]}
    end;
fast_flush(Tok, true, Opts, SW, Pos, Acc) ->
    Norm = normalise_token(Tok, Opts),
    case {Norm =:= <<>>, lists:member(Norm, SW)} of
        {true, _} -> {Pos, Acc};
        {false, true} -> {Pos + 1, Acc};
        {false, false} -> {Pos + 1, [{Norm, Pos} | Acc]}
    end.

finish_token(Acc, [], Pos, _Stopwords, _Opts) ->
    {Acc, [], Pos};
finish_token(Acc, Current, Pos, Stopwords, Opts) ->
    Token = normalise_token(
        unicode:characters_to_binary(lists:reverse(Current), utf8), Opts
    ),
    case {Token =:= <<>>, lists:member(Token, Stopwords)} of
        {true, _} -> {Acc, [], Pos};
        {false, true} -> {Acc, [], Pos + 1};
        {false, false} -> {[{Token, Pos} | Acc], [], Pos + 1}
    end.

token_char(Char, Opts) ->
    TokenChars = maps:get(tokenchars, Opts, []),
    Separators = maps:get(separators, Opts, []),
    (unicode_token_char(Char, Opts) orelse lists:member(Char, TokenChars)) andalso
        not lists:member(Char, Separators).

unicode_token_char(Char, Opts) ->
    case fts5_token_char(Char) of
        true ->
            true;
        false ->
            remove_diacritics_enabled(Opts) andalso sqlite_diacritic_mark(Char)
    end.

%% Generated from SQLite ext/fts5/fts5_unicode2.c by replaying
%% sqlite3Fts5UnicodeCategory over every codepoint with the unicode61
%% default categories "L* N* Co" PLUS category 0: fts5's CatParse sets
%% aArray[0]=1 unconditionally, so codepoints the data table does not
%% cover — including the deliberately under-encoded CJK Unified/Ext and
%% Hangul Syllable mega-blocks, unassigned gaps, and everything at or
%% above 2^20 — are token characters in FTS5. Classification therefore
%% matches SQLite FTS5 unicode61 exactly and does not depend on the OTP
%% unicode_util data version.
fts5_token_ranges() ->
    {
        48,
        57,
        65,
        90,
        97,
        122,
        170,
        170,
        178,
        179,
        181,
        181,
        185,
        186,
        188,
        190,
        192,
        214,
        216,
        246,
        248,
        705,
        710,
        721,
        736,
        740,
        748,
        748,
        750,
        750,
        880,
        884,
        886,
        893,
        895,
        899,
        902,
        902,
        904,
        1013,
        1015,
        1153,
        1162,
        1369,
        1376,
        1416,
        1419,
        1422,
        1424,
        1424,
        1480,
        1522,
        1525,
        1535,
        1541,
        1541,
        1564,
        1565,
        1568,
        1610,
        1632,
        1641,
        1646,
        1647,
        1649,
        1747,
        1749,
        1749,
        1765,
        1766,
        1774,
        1788,
        1791,
        1791,
        1806,
        1806,
        1808,
        1808,
        1810,
        1839,
        1867,
        1957,
        1969,
        2026,
        2036,
        2037,
        2042,
        2069,
        2074,
        2074,
        2084,
        2084,
        2088,
        2088,
        2094,
        2095,
        2111,
        2136,
        2140,
        2141,
        2143,
        2275,
        2303,
        2303,
        2308,
        2361,
        2365,
        2365,
        2384,
        2384,
        2392,
        2401,
        2406,
        2415,
        2417,
        2432,
        2436,
        2491,
        2493,
        2493,
        2501,
        2502,
        2505,
        2506,
        2510,
        2518,
        2520,
        2529,
        2532,
        2545,
        2548,
        2553,
        2556,
        2560,
        2564,
        2619,
        2621,
        2621,
        2627,
        2630,
        2633,
        2634,
        2638,
        2640,
        2642,
        2671,
        2674,
        2676,
        2678,
        2688,
        2692,
        2747,
        2749,
        2749,
        2758,
        2758,
        2762,
        2762,
        2766,
        2785,
        2788,
        2799,
        2802,
        2816,
        2820,
        2875,
        2877,
        2877,
        2885,
        2886,
        2889,
        2890,
        2894,
        2901,
        2904,
        2913,
        2916,
        2927,
        2929,
        2945,
        2947,
        3005,
        3011,
        3013,
        3017,
        3017,
        3022,
        3030,
        3032,
        3058,
        3067,
        3072,
        3076,
        3133,
        3141,
        3141,
        3145,
        3145,
        3150,
        3156,
        3159,
        3169,
        3172,
        3198,
        3200,
        3201,
        3204,
        3259,
        3261,
        3261,
        3269,
        3269,
        3273,
        3273,
        3278,
        3284,
        3287,
        3297,
        3300,
        3329,
        3332,
        3389,
        3397,
        3397,
        3401,
        3401,
        3406,
        3414,
        3416,
        3425,
        3428,
        3448,
        3450,
        3457,
        3460,
        3529,
        3531,
        3534,
        3541,
        3541,
        3543,
        3543,
        3552,
        3569,
        3573,
        3632,
        3634,
        3635,
        3643,
        3646,
        3648,
        3654,
        3664,
        3673,
        3676,
        3760,
        3762,
        3763,
        3770,
        3770,
        3773,
        3783,
        3790,
        3840,
        3872,
        3891,
        3904,
        3952,
        3976,
        3980,
        3992,
        3992,
        4029,
        4029,
        4045,
        4045,
        4059,
        4138,
        4159,
        4169,
        4176,
        4181,
        4186,
        4189,
        4193,
        4193,
        4197,
        4198,
        4206,
        4208,
        4213,
        4225,
        4238,
        4238,
        4240,
        4249,
        4256,
        4346,
        4348,
        4956,
        4969,
        5007,
        5018,
        5119,
        5121,
        5740,
        5743,
        5759,
        5761,
        5786,
        5789,
        5866,
        5870,
        5905,
        5909,
        5937,
        5943,
        5969,
        5972,
        6001,
        6004,
        6067,
        6103,
        6103,
        6108,
        6108,
        6110,
        6143,
        6159,
        6312,
        6314,
        6431,
        6444,
        6447,
        6460,
        6463,
        6465,
        6467,
        6470,
        6575,
        6593,
        6599,
        6602,
        6621,
        6656,
        6678,
        6684,
        6685,
        6688,
        6740,
        6751,
        6751,
        6781,
        6782,
        6784,
        6815,
        6823,
        6823,
        6830,
        6911,
        6917,
        6963,
        6981,
        7001,
        7037,
        7039,
        7043,
        7072,
        7086,
        7141,
        7156,
        7163,
        7168,
        7203,
        7224,
        7226,
        7232,
        7293,
        7296,
        7359,
        7368,
        7375,
        7401,
        7404,
        7406,
        7409,
        7413,
        7615,
        7655,
        7675,
        7680,
        8124,
        8126,
        8126,
        8130,
        8140,
        8144,
        8156,
        8160,
        8172,
        8176,
        8188,
        8191,
        8191,
        8293,
        8297,
        8304,
        8313,
        8319,
        8329,
        8335,
        8351,
        8378,
        8399,
        8433,
        8447,
        8450,
        8450,
        8455,
        8455,
        8458,
        8467,
        8469,
        8469,
        8473,
        8477,
        8484,
        8484,
        8486,
        8486,
        8488,
        8488,
        8490,
        8493,
        8495,
        8505,
        8508,
        8511,
        8517,
        8521,
        8526,
        8526,
        8528,
        8591,
        9204,
        9215,
        9255,
        9279,
        9291,
        9371,
        9450,
        9471,
        9984,
        9984,
        10102,
        10131,
        11085,
        11087,
        11098,
        11492,
        11499,
        11502,
        11506,
        11512,
        11517,
        11517,
        11520,
        11631,
        11633,
        11646,
        11648,
        11743,
        11823,
        11823,
        11836,
        11903,
        11930,
        11930,
        12020,
        12031,
        12246,
        12271,
        12284,
        12287,
        12293,
        12295,
        12321,
        12329,
        12337,
        12341,
        12344,
        12348,
        12352,
        12440,
        12445,
        12447,
        12449,
        12538,
        12540,
        12687,
        12690,
        12693,
        12704,
        12735,
        12772,
        12799,
        12831,
        12841,
        12872,
        12879,
        12881,
        12895,
        12928,
        12937,
        12977,
        12991,
        13055,
        13055,
        13312,
        19903,
        19968,
        42127,
        42183,
        42237,
        42240,
        42508,
        42512,
        42606,
        42623,
        42654,
        42656,
        42735,
        42744,
        42751,
        42775,
        42783,
        42786,
        42888,
        42891,
        43009,
        43011,
        43013,
        43015,
        43018,
        43020,
        43042,
        43052,
        43061,
        43066,
        43123,
        43128,
        43135,
        43138,
        43187,
        43205,
        43213,
        43216,
        43231,
        43250,
        43255,
        43259,
        43301,
        43312,
        43334,
        43348,
        43358,
        43360,
        43391,
        43396,
        43442,
        43470,
        43485,
        43488,
        43560,
        43575,
        43586,
        43588,
        43595,
        43598,
        43611,
        43616,
        43638,
        43642,
        43642,
        43644,
        43695,
        43697,
        43697,
        43701,
        43702,
        43705,
        43709,
        43712,
        43712,
        43714,
        43741,
        43744,
        43754,
        43762,
        43764,
        43767,
        44002,
        44014,
        55295,
        55297,
        56190,
        56193,
        56318,
        56321,
        57342,
        57344,
        64285,
        64287,
        64296,
        64298,
        64433,
        64450,
        64829,
        64832,
        65019,
        65022,
        65023,
        65050,
        65055,
        65063,
        65071,
        65107,
        65107,
        65127,
        65127,
        65132,
        65278,
        65280,
        65280,
        65296,
        65305,
        65313,
        65338,
        65345,
        65370,
        65382,
        65503,
        65511,
        65511,
        65519,
        65528,
        65534,
        65791,
        65795,
        65846,
        65856,
        65912,
        65930,
        65935,
        65948,
        65999,
        66046,
        66462,
        66464,
        66511,
        66513,
        67670,
        67672,
        67870,
        67872,
        67902,
        67904,
        68096,
        68100,
        68100,
        68103,
        68107,
        68112,
        68151,
        68155,
        68158,
        68160,
        68175,
        68185,
        68222,
        68224,
        68408,
        68416,
        69631,
        69635,
        69687,
        69710,
        69759,
        69763,
        69807,
        69826,
        69887,
        69891,
        69926,
        69941,
        69951,
        69956,
        70015,
        70019,
        70066,
        70081,
        70084,
        70089,
        71338,
        71352,
        74863,
        74868,
        94032,
        94079,
        94094,
        94099,
        118783,
        119030,
        119039,
        119079,
        119080,
        119262,
        119295,
        119366,
        119551,
        119639,
        120512,
        120514,
        120538,
        120540,
        120570,
        120572,
        120596,
        120598,
        120628,
        120630,
        120654,
        120656,
        120686,
        120688,
        120712,
        120714,
        120744,
        120746,
        120770,
        120772,
        126703,
        126706,
        126975,
        127020,
        127023,
        127124,
        127135,
        127151,
        127152,
        127167,
        127168,
        127184,
        127184,
        127200,
        127247,
        127279,
        127279,
        127340,
        127343,
        127387,
        127461,
        127491,
        127503,
        127547,
        127551,
        127561,
        127567,
        127570,
        127743,
        127777,
        127791,
        127798,
        127798,
        127869,
        127871,
        127892,
        127903,
        127941,
        127941,
        127947,
        127967,
        127985,
        127999,
        128063,
        128063,
        128065,
        128065,
        128248,
        128248,
        128253,
        128255,
        128318,
        128319,
        128324,
        128335,
        128360,
        128506,
        128577,
        128580,
        128592,
        128639,
        128710,
        128767,
        128884,
        917504,
        917506,
        917535,
        917632,
        917759,
        918000,
        1114111
    }.

fts5_token_char(Char) when is_integer(Char), Char >= 0 ->
    Ranges = fts5_token_ranges(),
    fts5_token_char(Char, Ranges, 1, tuple_size(Ranges) div 2).

fts5_token_char(_Char, _Ranges, Lo, Hi) when Lo > Hi ->
    false;
fts5_token_char(Char, Ranges, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    Start = element(2 * Mid - 1, Ranges),
    End = element(2 * Mid, Ranges),
    if
        Char < Start -> fts5_token_char(Char, Ranges, Lo, Mid - 1);
        Char > End -> fts5_token_char(Char, Ranges, Mid + 1, Hi);
        true -> true
    end.

normalise_char(Char, _Opts) ->
    lower_chars([Char]).

normalise_token(Token0, Opts) ->
    Lower = unicode:characters_to_binary(
        lower_chars(unicode_chars(Token0)), utf8
    ),
    case maps:get(remove_diacritics, Opts, false) of
        false -> Lower;
        0 -> Lower;
        true -> strip_diacritics(Lower, 1);
        1 -> strip_diacritics(Lower, 1);
        2 -> strip_diacritics(Lower, 2)
    end.

lower_chars(Chars) ->
    [
        client_simple_fold(Char)
     || Char <- lists:flatten(
            [unicode_util:lowercase([C]) || C <- lists:flatten(Chars)]
        )
    ].

%% SQLite's Unicode-6.1 simple fold places both Greek sigma forms in the same
%% equivalence class.  OTP's context-sensitive lowercase retains final sigma.
client_simple_fold(16#03C2) -> 16#03C3;
client_simple_fold(Char) -> Char.

%% NFD exposes each character's combining marks so the SQLite diacritic
%% mask can drop them; recomposing with NFC afterwards restores every
%% decomposition the mask did not consume (Hangul syllables decompose to
%% Jamo under NFD, and FTS5 — which never decomposes — keeps them
%% precomposed; marks outside the mask likewise recompose back).
strip_diacritics(Token, Mode) ->
    Kept = lists:append([
        sqlite_fold_diacritic(Char, Mode)
     || Char <- lists:flatten(unicode_chars(Token))
    ]),
    unicode:characters_to_nfc_binary(Kept).

%% unicode61 drops combining marks from decomposed input, but its precomposed
%% fold table is Latin-only.  Mode 1 deliberately leaves a Latin codepoint
%% carrying multiple combining marks unchanged; mode 2 folds it to its ASCII
%% base.  Testing the NFD base reproduces the generated SQLite table without
%% applying the incorrect Greek/Cyrillic-wide NFD transform.
sqlite_fold_diacritic(Char, _Mode) when Char >= 16#0300, Char =< 16#036F ->
    case sqlite_diacritic_mark(Char) of
        true -> [];
        false -> [Char]
    end;
sqlite_fold_diacritic(Char, Mode) ->
    Decomposed = lists:flatten(unicode_util:nfd([Char])),
    case Decomposed of
        [Base | Marks] when
            (Base >= $a andalso Base =< $z) orelse
                (Base >= $A andalso Base =< $Z)
        ->
            Removable = [M || M <- Marks, sqlite_diacritic_mark(M)],
            case
                {Mode, length(Removable), length(Removable) =:= length(Marks)}
            of
                {_Any, 0, _} ->
                    [Char];
                {1, N, true} when N > 1 -> [Char];
                {_Any, _N, true} ->
                    [Base];
                {_Any, _N, false} ->
                    [Base | [M || M <- Marks, not sqlite_diacritic_mark(M)]]
            end;
        _ ->
            [Char]
    end.

remove_diacritics_enabled(Opts) ->
    case maps:get(remove_diacritics, Opts, false) of
        false -> false;
        0 -> false;
        _ -> true
    end.

sqlite_diacritic_mark(Char) when Char >= 16#0300, Char =< 16#0331 ->
    Offset = Char - 16#0300,
    {Mask, Bit} =
        case Offset < 32 of
            true -> {16#08029FDF, Offset};
            false -> {16#000361F8, Offset - 32}
        end,
    (Mask band (1 bsl Bit)) =/= 0;
sqlite_diacritic_mark(_Char) ->
    false.

parse(all_docs, _Opts) ->
    {ok, {all_docs}};
parse(Query0, Opts) ->
    case bounded_query(Query0) of
        {ok, Query} ->
            case blank(Query) of
                true ->
                    {error, {fts_parse, empty_query}};
                false ->
                    case lex(Query, Opts) of
                        {ok, Tokens} ->
                            case length(Tokens) =< ?MAX_QUERY_TOKENS of
                                true ->
                                    parse_tokens(Tokens, Opts);
                                false ->
                                    {error, fts_query_too_many_tokens}
                            end;
                        Error ->
                            Error
                    end
            end;
        Error ->
            Error
    end.

parse_tokens([], _Opts) ->
    {ok, {empty}};
parse_tokens(Tokens, Opts) ->
    case parse_or(Tokens, Opts) of
        {ok, AST, []} -> validate_ast_caps(ungroup(AST));
        {ok, _AST, Rest} -> {error, {fts_parse, trailing_tokens, Rest}};
        Error -> Error
    end.

bounded_query(Query) when is_binary(Query) ->
    case byte_size(Query) =< ?MAX_QUERY_BYTES of
        true -> {ok, Query};
        false -> {error, fts_query_too_large}
    end;
bounded_query(Query) when is_list(Query) ->
    try unicode:characters_to_binary(Query, utf8) of
        Bin when byte_size(Bin) =< ?MAX_QUERY_BYTES -> {ok, Bin};
        _ -> {error, fts_query_too_large}
    catch
        _:_ -> {error, invalid_fts_query}
    end;
bounded_query(Query) ->
    bounded_query(client_term_binary(Query)).

blank(Query) ->
    lists:all(fun(C) -> lists:member(C, " \t\r\n") end, unicode_chars(Query)).

lex(Query, Opts) ->
    case lex_chars(unicode_chars(Query), Opts, [], true) of
        {ok, Tokens} -> {ok, normalise_near_tokens(Tokens)};
        Error -> Error
    end.

lex_chars([], _Opts, Acc, _AfterSpace) ->
    {ok, lists:reverse(Acc)};
lex_chars([C | Rest], Opts, Acc, _AfterSpace) when
    C == 32; C == 9; C == 10; C == 13
->
    lex_chars(Rest, Opts, Acc, true);
lex_chars([$( | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [lparen | Acc], false);
lex_chars([$) | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [rparen | Acc], false);
lex_chars([$: | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [colon | Acc], false);
lex_chars([$* | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [star | Acc], false);
lex_chars([$+ | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [plus | Acc], false);
lex_chars([$, | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [comma | Acc], false);
lex_chars([$/ | Rest], Opts, [near_candidate | _] = Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [slash | Acc], false);
lex_chars([$/ | Rest], Opts, [near_case_variant | _] = Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [slash | Acc], false);
lex_chars([${ | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [lbrace | Acc], false);
lex_chars([$} | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [rbrace | Acc], false);
lex_chars([$- | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [minus | Acc], false);
lex_chars([$^ | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [caret | Acc], false);
lex_chars([$" | Rest], Opts, Acc, _AfterSpace) ->
    case collect_quote(Rest, []) of
        {ok, Phrase, Rest2} ->
            lex_chars(
                Rest2,
                Opts,
                [
                    {phrase, unicode:characters_to_binary(Phrase, utf8)} | Acc
                ],
                false
            );
        error ->
            {error, {fts_parse, unterminated_quote}}
    end;
lex_chars([C | Rest], Opts, Acc, AfterSpace) ->
    case token_char(C, Opts) of
        true ->
            {Chars, Rest2} = collect_word(Rest, Opts, [C]),
            Word = unicode:characters_to_binary(lists:reverse(Chars), utf8),
            case classify_word(Word, Opts) of
                skip -> lex_chars(Rest2, Opts, Acc, false);
                Token -> lex_chars(Rest2, Opts, [Token | Acc], false)
            end;
        false ->
            case separator_concat(Rest, Opts, Acc, AfterSpace) of
                true -> lex_chars(Rest, Opts, [plus | Acc], false);
                false -> lex_chars(Rest, Opts, Acc, false)
            end
    end.

separator_concat([Next | _Rest], Opts, [Token | _Acc], false) ->
    lexical_token(Token) andalso token_char(Next, Opts);
separator_concat(_Rest, _Opts, _Acc, _AfterSpace) ->
    false.

lexical_token({word, _Token}) -> true;
lexical_token({phrase, _Text}) -> true;
lexical_token({phrase_tokens, _Tokens}) -> true;
lexical_token(_Other) -> false.

collect_quote([], _Acc) -> error;
collect_quote([$" | Rest], Acc) -> {ok, lists:reverse(Acc), Rest};
collect_quote([C | Rest], Acc) -> collect_quote(Rest, [C | Acc]).

collect_word([], _Opts, Acc) ->
    {Acc, []};
collect_word([$-, Next | Rest], Opts, Acc) ->
    case token_char(Next, Opts) of
        true -> collect_word(Rest, Opts, [Next, $- | Acc]);
        false -> {Acc, [$-, Next | Rest]}
    end;
collect_word([C | Rest], Opts, Acc) ->
    case token_char(C, Opts) of
        true -> collect_word(Rest, Opts, [C | Acc]);
        false -> {Acc, [C | Rest]}
    end.

classify_word(<<"AND">>, _Opts) ->
    'and';
classify_word(<<"OR">>, _Opts) ->
    'or';
classify_word(<<"NOT">>, _Opts) ->
    'not';
classify_word(<<"NEAR">>, _Opts) ->
    near_candidate;
classify_word(Word, Opts) ->
    case normalise_token(Word, Opts) of
        <<"near">> ->
            near_case_variant;
        _ ->
            case tokenize(Word, Opts) of
                [] -> skip;
                [{Token, _}] -> {word, Token};
                Tokens -> {phrase_tokens, [Token || {Token, _} <- Tokens]}
            end
    end.

%% NEAR syntax synonyms are resolved at the lexer seam.  The parser sees one
%% operator token for every infix spelling, while the long-standing function
%% form retains its grouped token shape.  Lowercase "near" remains an ordinary
%% prose term unless the spelling carries an explicit distance.
normalise_near_tokens([near_candidate, lparen | Rest]) ->
    [near, lparen | normalise_near_tokens(Rest)];
normalise_near_tokens([near_candidate, comma, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_candidate, comma);
normalise_near_tokens([near_candidate, slash, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_candidate, slash);
normalise_near_tokens([near_candidate | Rest]) ->
    [{near_op, ?DEFAULT_NEAR} | normalise_near_tokens(Rest)];
normalise_near_tokens([near_case_variant, comma, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_case_variant, comma);
normalise_near_tokens([near_case_variant, slash, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_case_variant, slash);
normalise_near_tokens([near_case_variant | Rest]) ->
    [{word, <<"near">>} | normalise_near_tokens(Rest)];
normalise_near_tokens([Token | Rest]) ->
    [Token | normalise_near_tokens(Rest)];
normalise_near_tokens([]) ->
    [].

normalise_near_distance(Distance, Rest, NearToken, Separator) ->
    try binary_to_integer(Distance) of
        N when N >= 0 ->
            [{near_op, N} | normalise_near_tokens(Rest)];
        _ ->
            [
                NearToken,
                Separator,
                {word, Distance}
                | normalise_near_tokens(Rest)
            ]
    catch
        _:_ ->
            [
                NearToken,
                Separator,
                {word, Distance}
                | normalise_near_tokens(Rest)
            ]
    end.

parse_or(Tokens, Opts) ->
    case parse_and(Tokens, Opts) of
        {ok, Left, Rest} -> parse_or_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_or_tail(Left, ['or' | Rest], Opts) ->
    case parse_and(Rest, Opts) of
        {ok, Right, Rest2} ->
            parse_or_tail({'or', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error ->
            Error
    end;
parse_or_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_and(Tokens, Opts) ->
    case parse_not(Tokens, Opts) of
        {ok, Left, Rest} -> parse_and_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_and_tail(Left, ['and' | Rest], Opts) ->
    case parse_not(Rest, Opts) of
        {ok, Right, Rest2} ->
            parse_and_tail({'and', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error ->
            Error
    end;
parse_and_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_not(Tokens, Opts) ->
    case parse_implicit(Tokens, Opts) of
        {ok, Left, Rest} -> parse_not_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_not_tail(Left, ['not' | Rest], Opts) ->
    case parse_implicit(Rest, Opts) of
        {ok, Right, Rest2} ->
            parse_not_tail({'not', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error ->
            Error
    end;
parse_not_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_implicit(Tokens, Opts) ->
    case parse_near_operator(Tokens, Opts) of
        {ok, Left, Rest} -> parse_implicit_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_implicit_tail(Left, Rest, Opts) ->
    case starts_primary(Rest) of
        true ->
            case is_group(Left) orelse starts_group(Rest) of
                true ->
                    {error, {fts_parse, invalid_group_adjacency}};
                false ->
                    case parse_near_operator(Rest, Opts) of
                        {ok, Right, Rest2} ->
                            parse_implicit_tail(
                                {'and', ungroup(Left), ungroup(Right)},
                                Rest2,
                                Opts
                            );
                        Error ->
                            Error
                    end
            end;
        false ->
            {ok, Left, Rest}
    end.

parse_near_operator(Tokens, Opts) ->
    case parse_primary(Tokens, Opts) of
        {ok, Left, Rest} -> parse_near_operator_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_near_operator_tail(Left, [{near_op, Distance} | Rest], Opts) ->
    case parse_primary(Rest, Opts) of
        {ok, Right, Rest2} ->
            case append_near_item(ungroup(Left), ungroup(Right), Distance) of
                {ok, Near} ->
                    parse_near_operator_tail(Near, Rest2, Opts);
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
parse_near_operator_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

append_near_item(
    {near, _Items, Distance, _Columns},
    {near, _RightItems, _RightDistance, _RightColumns},
    Distance
) ->
    {error, {fts_parse, invalid_near_nesting}};
append_near_item(
    {near, Items, Distance, Columns},
    Right,
    Distance
) ->
    {ok, {near, Items ++ [Right], Distance, Columns}};
append_near_item(
    {near, _Items, _OtherDistance, _Columns},
    _Right,
    _Distance
) ->
    {error, {fts_parse, mixed_near_distances}};
append_near_item(
    _Left,
    {near, _Items, _Distance, _Columns},
    _NearDistance
) ->
    {error, {fts_parse, invalid_near_nesting}};
append_near_item(Left, Right, Distance) ->
    {ok, {near, [Left, Right], Distance, all}}.

parse_primary([], _Opts) ->
    {error, {fts_parse, unexpected_end}};
parse_primary(Tokens, Opts) ->
    case parse_primary_base(Tokens, Opts) of
        {ok, AST, Rest} -> parse_concat_tail(AST, Rest, Opts);
        Error -> Error
    end.

parse_primary_base([], _Opts) ->
    {error, {fts_parse, unexpected_end}};
parse_primary_base([near, lparen | Rest], Opts) ->
    parse_near(Rest, Opts);
parse_primary_base([caret, {word, _Column}, colon | _Rest], _Opts) ->
    {error, {fts_parse, invalid_anchor}};
parse_primary_base([caret, near, lparen | _Rest], _Opts) ->
    {error, {fts_parse, invalid_anchor}};
parse_primary_base([caret | Rest], Opts) ->
    case parse_primary_base(Rest, Opts) of
        {ok, AST, Rest2} -> {ok, {anchor, ungroup(AST)}, Rest2};
        Error -> Error
    end;
parse_primary_base([{word, Column}, colon | Rest], Opts) ->
    parse_column_primary([normalise_column(Column)], Rest, Opts);
parse_primary_base([minus, {word, Column}, colon | Rest], Opts) ->
    parse_column_primary({not_columns, [normalise_column(Column)]}, Rest, Opts);
parse_primary_base([lbrace | Rest], Opts) ->
    case collect_columns(Rest, []) of
        {ok, Columns, [colon | Rest2]} ->
            parse_column_primary(Columns, Rest2, Opts);
        Error ->
            Error
    end;
parse_primary_base([minus, lbrace | Rest], Opts) ->
    case collect_columns(Rest, []) of
        {ok, Columns, [colon | Rest2]} ->
            parse_column_primary(
                {not_columns, Columns},
                Rest2,
                Opts
            );
        Error ->
            Error
    end;
parse_primary_base([lparen | Rest], Opts) ->
    case parse_or(Rest, Opts) of
        {ok, AST, [rparen | Rest2]} -> {ok, {group, AST}, Rest2};
        {ok, _AST, Other} -> {error, {fts_parse, expected_rparen, Other}};
        Error -> Error
    end;
parse_primary_base([{phrase, Text}, star | Rest], Opts) ->
    {ok, phrase_ast(Text, Opts, true), Rest};
parse_primary_base([{phrase, Text} | Rest], Opts) ->
    {ok, phrase_ast(Text, Opts, false), Rest};
parse_primary_base([{phrase_tokens, Tokens}, star | Rest], _Opts) ->
    {ok, phrase_tokens_ast(Tokens, true), Rest};
parse_primary_base([{phrase_tokens, Tokens} | Rest], _Opts) ->
    {ok, phrase_tokens_ast(Tokens, false), Rest};
parse_primary_base([{word, Token}, star | Rest], _Opts) ->
    {ok, {term, Token, true, all}, Rest};
parse_primary_base([{word, Token} | Rest], _Opts) ->
    {ok, {term, Token, false, all}, Rest};
parse_primary_base([Other | _Rest], _Opts) ->
    {error, {fts_parse, unexpected_token, Other}}.

parse_column_primary(Columns, Rest, Opts) ->
    case parse_primary(Rest, Opts) of
        {ok, AST, Rest2} ->
            {ok, restrict_ast_columns(ungroup(AST), Columns), Rest2};
        Error ->
            Error
    end.

collect_columns([rbrace | _Rest], []) ->
    {error, {fts_parse, empty_column_list}};
collect_columns([rbrace | Rest], Acc) ->
    {ok, lists:reverse(Acc), Rest};
collect_columns([{word, Column} | Rest], Acc) ->
    collect_columns(Rest, [normalise_column(Column) | Acc]);
collect_columns([{phrase, Column} | Rest], Acc) ->
    collect_columns(Rest, [normalise_column(Column) | Acc]);
collect_columns(Other, _Acc) ->
    {error, {fts_parse, invalid_column_list, Other}}.

parse_concat_tail(Left, [plus | Rest], Opts) ->
    case parse_primary_base(Rest, Opts) of
        {ok, Right, Rest2} ->
            case concat_phrase(ungroup(Left), ungroup(Right)) of
                {ok, AST} -> parse_concat_tail(AST, Rest2, Opts);
                Error -> Error
            end;
        Error ->
            Error
    end;
parse_concat_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_near(Tokens, Opts) ->
    case parse_near_items(Tokens, Opts, []) of
        {ok, Items, Distance, Rest} -> {ok, {near, Items, Distance, all}, Rest};
        Error -> Error
    end.

parse_near_items([rparen | Rest], _Opts, []) ->
    {error, {fts_parse, empty_near, Rest}};
parse_near_items([rparen | Rest], _Opts, Acc) ->
    {ok, lists:reverse(Acc), ?DEFAULT_NEAR, Rest};
parse_near_items([comma, {word, NBin}, rparen | Rest], _Opts, Acc) ->
    try binary_to_integer(NBin) of
        N when N >= 0 -> {ok, lists:reverse(Acc), N, Rest};
        _ -> {error, {fts_parse, invalid_near_distance, NBin}}
    catch
        _:_ -> {error, {fts_parse, invalid_near_distance, NBin}}
    end;
parse_near_items([comma | Rest], Opts, Acc) when Acc =/= [] ->
    case starts_primary(Rest) of
        true -> parse_near_items(Rest, Opts, Acc);
        false -> {error, {fts_parse, unexpected_token, comma}}
    end;
parse_near_items(Tokens, Opts, Acc) ->
    case parse_primary_base(Tokens, Opts) of
        {ok, Item, Rest} ->
            %% SQLite FTS5 rejects NEAR nested inside NEAR; match that.
            case ungroup(Item) of
                {near, _Items, _Distance, _Columns} ->
                    {error, {fts_parse, invalid_near_nesting}};
                Item1 ->
                    parse_near_items(Rest, Opts, [Item1 | Acc])
            end;
        Error ->
            Error
    end.

phrase_ast(Text, Opts, PrefixLast) ->
    phrase_position_ast(tokenize(Text, Opts), PrefixLast).

phrase_position_ast(Tokens, PrefixLast) ->
    LastPos =
        case Tokens of
            [] -> -1;
            _ -> lists:max([Pos || {_Token, Pos} <- Tokens])
        end,
    Specs =
        [
            {Token, PrefixLast andalso Pos =:= LastPos, Pos}
         || {Token, Pos} <- Tokens
        ],
    {phrase, Specs, all}.

phrase_tokens_ast(Tokens, PrefixLast) ->
    Specs =
        [
            {Token, PrefixLast andalso N =:= length(Tokens) - 1, N}
         || {Token, N} <- lists:zip(
                Tokens, lists:seq(0, max(0, length(Tokens) - 1))
            )
        ],
    {phrase, Specs, all}.

concat_phrase({term, Token, Prefix, Columns}, Right) ->
    concat_phrase({phrase, [{Token, Prefix, 0}], Columns}, Right);
concat_phrase(
    {phrase, LeftSpecs, LeftColumns}, {term, Token, Prefix, RightColumns}
) ->
    Offset = length(LeftSpecs),
    {ok,
        {phrase, LeftSpecs ++ [{Token, Prefix, Offset}],
            concat_columns(LeftColumns, RightColumns)}};
concat_phrase(
    {phrase, LeftSpecs, LeftColumns}, {phrase, RightSpecs, RightColumns}
) ->
    Offset = length(LeftSpecs),
    {ok,
        {phrase,
            LeftSpecs ++
                [
                    {Token, Prefix, Offset + Pos}
                 || {Token, Prefix, Pos} <- RightSpecs
                ],
            concat_columns(LeftColumns, RightColumns)}};
concat_phrase(_Left, _Right) ->
    {error, {fts_parse, invalid_phrase_concat}}.

concat_columns(all, Columns) ->
    Columns;
concat_columns(Columns, all) ->
    Columns;
concat_columns({not_columns, A}, {not_columns, B}) ->
    {not_columns, lists:usort(schema_columns(A) ++ schema_columns(B))};
concat_columns({not_columns, Excluded}, Columns) ->
    lists:subtract(schema_columns(Columns), schema_columns(Excluded));
concat_columns(Columns, {not_columns, Excluded}) ->
    lists:subtract(schema_columns(Columns), schema_columns(Excluded));
concat_columns(A, B) ->
    [
        Column
     || Column <- schema_columns(A), lists:member(Column, schema_columns(B))
    ].

starts_primary([{word, _} | _]) -> true;
starts_primary([{phrase, _} | _]) -> true;
starts_primary([{phrase_tokens, _} | _]) -> true;
starts_primary([near, lparen | _]) -> true;
starts_primary([caret | _]) -> true;
starts_primary([lparen | _]) -> true;
starts_primary([lbrace | _]) -> true;
starts_primary([minus, {word, _}, colon | _]) -> true;
starts_primary([minus, lbrace | _]) -> true;
starts_primary(_Other) -> false.

ungroup({group, AST}) -> ungroup(AST);
ungroup(AST) -> AST.

is_group({group, _AST}) -> true;
is_group(_AST) -> false.

starts_group([lparen | _Rest]) -> true;
starts_group(_Rest) -> false.

validate_ast_caps(AST) ->
    case ast_depth(AST) =< ?MAX_AST_DEPTH of
        true ->
            case validate_near_distance(AST) of
                ok ->
                    case validate_prefix_bytes(AST) of
                        ok -> {ok, AST};
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        false ->
            {error, fts_query_ast_too_deep}
    end.

ast_depth({'and', A, B}) ->
    1 + max(ast_depth(A), ast_depth(B));
ast_depth({'or', A, B}) ->
    1 + max(ast_depth(A), ast_depth(B));
ast_depth({'not', A, B}) ->
    1 + max(ast_depth(A), ast_depth(B));
ast_depth({anchor, A}) ->
    1 + ast_depth(A);
ast_depth({near, Items, _Distance, _Columns}) ->
    1 + lists:max([0 | [ast_depth(I) || I <- Items]]);
ast_depth(_Other) ->
    1.

validate_near_distance({near, Items, Distance, _Columns}) when
    is_integer(Distance), Distance >= 0, Distance =< ?MAX_NEAR_DISTANCE
->
    validate_ast_list(Items, fun validate_near_distance/1);
validate_near_distance({near, _Items, _Distance, _Columns}) ->
    {error, fts_query_near_distance_exceeded};
validate_near_distance(AST) ->
    validate_children(AST, fun validate_near_distance/1).

validate_prefix_bytes({term, Token, true, _Columns}) when
    byte_size(Token) > ?MAX_PREFIX_BYTES
->
    {error, fts_query_prefix_too_large};
validate_prefix_bytes({phrase, Specs, _Columns}) ->
    case
        lists:any(
            fun
                ({Token, true, _Pos}) -> byte_size(Token) > ?MAX_PREFIX_BYTES;
                (_) -> false
            end,
            Specs
        )
    of
        true -> {error, fts_query_prefix_too_large};
        false -> ok
    end;
validate_prefix_bytes(AST) ->
    validate_children(AST, fun validate_prefix_bytes/1).

validate_children({'and', A, B}, Fun) ->
    validate_pair(A, B, Fun);
validate_children({'or', A, B}, Fun) ->
    validate_pair(A, B, Fun);
validate_children({'not', A, B}, Fun) ->
    validate_pair(A, B, Fun);
validate_children({anchor, A}, Fun) ->
    Fun(A);
validate_children({near, Items, _Distance, _Columns}, Fun) ->
    validate_ast_list(Items, Fun);
validate_children(_Other, _Fun) ->
    ok.

validate_pair(A, B, Fun) ->
    case Fun(A) of
        ok -> Fun(B);
        Error -> Error
    end.

validate_ast_list([], _Fun) ->
    ok;
validate_ast_list([Item | Rest], Fun) ->
    case Fun(Item) of
        ok -> validate_ast_list(Rest, Fun);
        Error -> Error
    end.

option_columns(Opts, Schema) ->
    schema_columns(maps:get(columns, Opts, maps:get(columns, Schema))).

validate_ast_columns(AST, Columns) ->
    case
        [
            Column
         || Column <- ast_columns(AST), not lists:member(Column, Columns)
        ]
    of
        [] -> ok;
        [Unknown | _Rest] -> {error, {fts_parse, unknown_column, Unknown}}
    end.

ast_columns({term, _T, _P, Columns}) ->
    selector_columns(Columns);
ast_columns({phrase, _Specs, Columns}) ->
    selector_columns(Columns);
ast_columns({near, Items, _Distance, Columns}) ->
    lists:usort(
        selector_columns(Columns) ++
            lists:append([ast_columns(Item) || Item <- Items])
    );
ast_columns({anchor, AST}) ->
    ast_columns(AST);
ast_columns({'and', A, B}) ->
    lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns({'or', A, B}) ->
    lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns({'not', A, B}) ->
    lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns(_Other) ->
    [].

selector_columns(all) -> [];
selector_columns({not_columns, Columns}) -> schema_columns(Columns);
selector_columns(Columns) -> schema_columns(Columns).

restrict_ast_columns(AST, all) ->
    AST;
restrict_ast_columns(_AST, []) ->
    {empty};
restrict_ast_columns({term, T, P, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] -> {empty};
        Cols1 -> {term, T, P, Cols1}
    end;
restrict_ast_columns({phrase, Specs, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] -> {empty};
        Cols1 -> {phrase, Specs, Cols1}
    end;
restrict_ast_columns({near, Items, Distance, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] ->
            {empty};
        Cols1 ->
            {near, [restrict_ast_columns(I, Cols1) || I <- Items], Distance,
                Cols1}
    end;
restrict_ast_columns({anchor, AST}, Cols) ->
    case restrict_ast_columns(AST, Cols) of
        {empty} -> {empty};
        AST1 -> {anchor, AST1}
    end;
restrict_ast_columns({'and', A, B}, Cols) ->
    case {restrict_ast_columns(A, Cols), restrict_ast_columns(B, Cols)} of
        {{empty}, _} -> {empty};
        {_, {empty}} -> {empty};
        {A1, B1} -> {'and', A1, B1}
    end;
restrict_ast_columns({'or', A, B}, Cols) ->
    {'or', restrict_ast_columns(A, Cols), restrict_ast_columns(B, Cols)};
restrict_ast_columns({'not', A, B}, Cols) ->
    {'not', restrict_ast_columns(A, Cols), restrict_ast_columns(B, Cols)};
restrict_ast_columns(Other, _Cols) ->
    Other.

combine_columns(all, Cols) ->
    normalise_column_selector(Cols);
combine_columns(Cols, all) ->
    normalise_column_selector(Cols);
combine_columns({not_columns, ExcludedA}, {not_columns, ExcludedB}) ->
    %% Nested negative selectors compose by union: the term must avoid
    %% every excluded column from both levels.
    {not_columns,
        lists:usort(
            schema_columns(ExcludedA) ++ schema_columns(ExcludedB)
        )};
combine_columns({not_columns, Excluded}, Cols) ->
    lists:subtract(
        normalise_column_selector(Cols), normalise_column_selector(Excluded)
    );
combine_columns(Cols, {not_columns, Excluded}) ->
    lists:subtract(
        normalise_column_selector(Cols), normalise_column_selector(Excluded)
    );
combine_columns(A, B) ->
    [
        C
     || C <- normalise_column_selector(A),
        lists:member(C, normalise_column_selector(B))
    ].

normalise_column_selector({not_columns, Columns}) ->
    {not_columns, schema_columns(Columns)};
normalise_column_selector(Columns) ->
    schema_columns(Columns).

unicode_chars(Bin) when is_binary(Bin) ->
    case unicode:characters_to_list(Bin, utf8) of
        {error, Good, <<_Bad, Rest/binary>>} ->
            Good ++ unicode_chars(Rest);
        {error, Good, _Rest} ->
            Good;
        {incomplete, Good, _Rest} ->
            Good;
        Chars ->
            Chars
    end;
unicode_chars(List) when is_list(List) ->
    case unicode:characters_to_list(List, utf8) of
        {error, Good, [_Bad | Rest]} ->
            Good ++ unicode_chars(Rest);
        {error, Good, _Rest} ->
            Good;
        {incomplete, Good, _Rest} ->
            Good;
        Chars ->
            Chars
    end.

unicode_chars_with_boundaries(Bin) when is_binary(Bin) ->
    case unicode:characters_to_list(Bin, utf8) of
        {error, Good, <<_Bad, Rest/binary>>} ->
            Good ++ [invalid_utf8 | unicode_chars_with_boundaries(Rest)];
        {error, Good, _Rest} ->
            Good ++ [invalid_utf8];
        {incomplete, Good, <<>>} ->
            Good;
        {incomplete, Good, _Rest} ->
            Good ++ [invalid_utf8];
        Chars ->
            Chars
    end.

canonicalise_ast_columns({term, T, P, Cols}) ->
    {term, T, P, canonical_selector(Cols)};
canonicalise_ast_columns({phrase, Specs, Cols}) ->
    {phrase, Specs, canonical_selector(Cols)};
canonicalise_ast_columns({near, Items, D, Cols}) ->
    {near, [canonicalise_ast_columns(I) || I <- Items], D,
        canonical_selector(Cols)};
canonicalise_ast_columns({anchor, A}) ->
    {anchor, canonicalise_ast_columns(A)};
canonicalise_ast_columns({'and', A, B}) ->
    {'and', canonicalise_ast_columns(A), canonicalise_ast_columns(B)};
canonicalise_ast_columns({'or', A, B}) ->
    {'or', canonicalise_ast_columns(A), canonicalise_ast_columns(B)};
canonicalise_ast_columns({'not', A, B}) ->
    {'not', canonicalise_ast_columns(A), canonicalise_ast_columns(B)};
canonicalise_ast_columns(Other) ->
    Other.

canonical_selector(all) ->
    all;
canonical_selector({not_columns, Cols}) ->
    {not_columns, schema_columns(Cols)};
canonical_selector(Cols) ->
    schema_columns(Cols).


%% ===========================================================================
%% INTERNAL FTS-2 FACADE
%% ===========================================================================

%% Internal FTS2 facade.  leveled_fts remains the only public search API.

-ifdef(TEST).
fts2_available(Bookie, Schema) ->
    fts2_search_root(Bookie, Schema).

fts2_consolidate_with_heap_for_test(Bookie, Schema, Opts, MaxHeapWords) ->
    fts2_consolidate_with_heap_for_test(
        Bookie, Schema, Opts, MaxHeapWords, undefined
    ).

fts2_consolidate_with_heap_for_test(
    Bookie, Schema, Opts, MaxHeapWords, Observer
) ->
    Previous = put('$fts2_build_max_heap_words', MaxHeapWords),
    PreviousObserver = put('$fts2_build_observer', Observer),
    PreviousReportObserver = put('$fts2_build_report_observer', Observer),
    try
        case Observer of
            Pid when is_pid(Pid) ->
                Pid ! {fts2_build_outer, self()};
            _ ->
                ok
        end,
        consolidate(Bookie, Schema, Opts)
    after
        case Previous of
            undefined -> erase('$fts2_build_max_heap_words');
            _ -> put('$fts2_build_max_heap_words', Previous)
        end,
        case PreviousObserver of
            undefined -> erase('$fts2_build_observer');
            _ -> put('$fts2_build_observer', PreviousObserver)
        end,
        case PreviousReportObserver of
            undefined -> erase('$fts2_build_report_observer');
            _ -> put('$fts2_build_report_observer', PreviousReportObserver)
        end
    end.

fts2_consolidate_with_shards_for_test(
    Bookie, Schema, Opts, MaxHeapWords, Shards, Observer
) ->
    Previous = put('$fts2_build_shards', Shards),
    try
        fts2_consolidate_with_heap_for_test(
            Bookie, Schema, Opts, MaxHeapWords, Observer
        )
    after
        case Previous of
            undefined -> erase('$fts2_build_shards');
            _ -> put('$fts2_build_shards', Previous)
        end
    end.

fts2_legacy_outer_assemble_for_test(Bookie, Schema) ->
    Existing = case fts2_search_root(Bookie, Schema) of
        {ok, Root} -> fts2_search_export_documents(Bookie, Schema, Root);
        not_found -> #{}
    end,
    Documents = fts2_build_apply_deltas(
        Existing, fts2_build_read_deltas(Bookie, Schema)
    ),
    {TokenDocs, CandidateRecords, HitRecords} =
        fts2_build_document_maps(Documents),
    SourceLengths = fts2_build_source_lengths(TokenDocs),
    {SourceMap, Groups} = fts2_build_group_sources(
        Schema, CandidateRecords, HitRecords, SourceLengths
    ),
    {TermRows, ByChunk} = fts2_build_build_term_rows(TokenDocs, SourceMap),
    FrequentTerms = maps:from_list([
        {Key, true}
     || {Key, Row} <- maps:to_list(TermRows),
        length(maps:get(entries, Row)) >= 64
    ]),
    {BigramRows, _BloomShards} = fts2_build_build_bigram_rows(
        ByChunk, FrequentTerms
    ),
    #{
        documents => map_size(Documents),
        groups => length(Groups),
        terms => map_size(TermRows),
        bigrams => map_size(BigramRows)
    }.
-endif.

fts2_lookup_documents(Bookie, Schema, SourceIds) ->
    fts2_search_lookup_documents(Bookie, Schema, SourceIds).

fts2_consolidate(Bookie, Schema, Hook, Opts) ->
    Pressure = spawn_link(fun() -> fts2_build_pressure_loop(#{}) end),
    PreviousPressure = put('$fts2_build_pressure', Pressure),
    try
        fts2_build_consolidate(Bookie, Schema, Hook, Opts)
    after
        Pressure ! stop,
        case PreviousPressure of
            undefined -> erase('$fts2_build_pressure');
            _ -> put('$fts2_build_pressure', PreviousPressure)
        end
    end.

fts2_search(Bookie, Schema, Root, AST, Opts) ->
    fts2_search_search(Bookie, Schema, Root, AST, Opts).

fts2_search_dirty(Bookie, Schema, Root, AST, Opts, Hook) ->
    fts2_search_search_dirty(Bookie, Schema, Root, AST, Opts, Hook).

fts2_posting_read(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    fts2_search_posting_read(
        Bookie, Schema, Root, AST, SourceIds, Opts
    ).

fts2_posting_read_dirty(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    fts2_search_posting_read_dirty(
        Bookie, Schema, Root, AST, SourceIds, Opts
    ).


%% ===========================================================================
%% INTERNAL FTS-2 BUILD
%% ===========================================================================

%% FTS2 immutable generation builder.
%%
%% The migration input is the canonical consolidated posting state.  The
%% builder assigns group-contiguous chunk ids, writes every generation row,
%% and publishes the root last with compare-and-swap.

-define(CHUNK_BITS, 12).
-define(CHUNKS_PER_GROUP, 1 bsl ?CHUNK_BITS).
-define(LEGACY_IDENTITY_PAGE_SHIFT, 10).
-define(GENERATION_IDENTITY_PAGE_SHIFT, 2).
-define(HEAD_WINDOW, 256).
-define(BIGRAM_MIN_CHUNKS, 256).
-define(WRITE_SLICE, 192).
-define(WRITE_BARRIER_SLICES, 128).
-define(FTS2_MAX_BUILD_CONCURRENCY, 1).
%% Term and bigram rows are assembled one shard at a time. A shard's document
%% fragments are stored SHARD-MAJOR, so a shard worker reads only its own data
%% in one range fold. Its heap is therefore proportional to its share of the
%% generation. The share must stay small on a Zipfian corpus, where a few hot
%% terms carry most of the positions, so the term space is cut finely.
%% Bigram workers additionally carry the generation's frequent-term map, which
%% is copied once per worker, so the bigram space is cut less finely.
%% Fragment rounds are flushed once their accumulated position weight
%% reaches this many positions, so a workspace worker's fragment buffer is
%% bounded independently of the document's size.
-define(FRAGMENT_FLUSH_WEIGHT, 65536).
-define(FTS2_BUILD_TERM_SHARDS, 1024).
-define(FTS2_BUILD_BIGRAM_SHARDS, 256).
%% Anti-runaway backstop, not a tuning knob. Every build role now streams:
%% the outer holds ordering descriptors, a term shard holds its own shard
%% (whose floor is the single hottest term's row), and a workspace worker
%% holds one document -- the atomic delta grain -- with its fragments flushed
%% in bounded rounds. On the 360,473-term Zipfian gate (9,000 documents, one
%% 1.96 MB document) the measured live peaks are 10.66 MiB for a term shard,
%% 7.86 MiB for a bigram shard, 26.70 MiB for a workspace worker and 39.28 MiB
%% for the outer. max_heap_size counts allocated heap BLOCKS, which run about
%% 3.4 times live data across a copying collection, so the workspace worker's
%% document-grain floor already needs roughly 106 MiB of block budget here and
%% scales with the largest single document. 512 MiB covers a document several
%% times larger than the live store's while still stopping a runaway loudly.
-define(FTS2_MAX_BUILD_HEAP_WORDS,
    (512 * 1024 * 1024 div erlang:system_info(wordsize))
).

-ifdef(TEST).
fts2_build_document_maps(Documents) ->
    maps:fold(
        fun(SourceId, Document, {Terms, Candidates, Hits}) ->
            Candidate = {
                maps:get(doc_key, Document),
                maps:get(doc_version, Document),
                maps:get(candidate_record, Document)
            },
            Hit = {
                maps:get(doc_key, Document),
                maps:get(doc_version, Document),
                maps:get(hit_record, Document)
            },
            Posting = maps:get(posting, Document, #{}),
            NextTerms = maps:fold(
                fun(Column, Tokens, TermAcc) ->
                    maps:fold(
                        fun(Token, Entry, Inner) ->
                            Docs = maps:get(Token, Inner, #{}),
                            TokenPosting = #{Column => #{Token => Entry}},
                            Inner#{Token => Docs#{SourceId => {
                                SourceId,
                                maps:get(doc_length, Document),
                                TokenPosting
                            }}}
                        end,
                        TermAcc,
                        Tokens
                    )
                end,
                Terms,
                Posting
            ),
            {
                NextTerms,
                Candidates#{SourceId => Candidate},
                Hits#{SourceId => Hit}
            }
        end,
        {#{}, #{}, #{}},
        Documents
    ).
-endif.

fts2_build_consolidate(Bookie, Schema, Hook, Opts) ->
    EpochConditions = fts2_build_epoch_conditions(Bookie, Schema),
    Previous = fts2_build_current_root(Bookie, maps:get(index, Schema)),
    Root = case Previous of
        absent -> undefined;
        {_SQN, ExistingRoot} -> ExistingRoot
    end,
    DeltaRefs = fts2_build_read_delta_refs(Bookie, Schema),
    Generation = fts2_build_generation_id(),
    BuildConcurrency = client_option(concurrency, Opts, 1),
    Result =
        try
            SelectedGroups = fts2_build_prepare_workspace(
                Bookie, Schema, Root, DeltaRefs, Generation,
                BuildConcurrency
            ),
            case Hook of
                undefined -> ok;
                Fun when is_function(Fun, 1) ->
                    Fun({fts2, EpochConditions});
                Fun when is_function(Fun, 0) ->
                    Fun()
            end,
            TailPresenceCleanup = fts2_build_tail_presence_cleanup(
                Bookie, Schema
            ),
            Published = fts2_build_publish_workspace(
                Bookie,
                Schema,
                Root,
                Previous,
                Generation,
                SelectedGroups,
                [
                    {remove, maps:get(index, Schema), <<"stats">>,
                        <<"dirty">>, <<>>},
                    {remove, maps:get(index, Schema), <<"record-tail">>,
                        <<"dirty">>, <<>>}
                ] ++ TailPresenceCleanup,
                EpochConditions,
                BuildConcurrency
            ),
            Published
        catch
            Class:Reason:Stacktrace ->
                fts2_build_cleanup_generation(Bookie, Schema, Generation),
                erlang:raise(Class, Reason, Stacktrace)
        end,
    %% The root now makes the new generation authoritative. Cleanup failures
    %% after this point must never remove that published generation.
    fts2_build_remove_workspace(Bookie, Schema, Generation),
    case fts2_build_previous_generation(Previous) of
        undefined -> ok;
        CleanupGeneration ->
            fts2_build_cleanup_generation(Bookie, Schema, CleanupGeneration)
    end,
    %% Seal the large immutable generation at its published root.  Delta
    %% tombstones are deliberately written to the next journal so retention
    %% of a few non-ledger tombstones cannot pin the complete build journal.
    fts2_build_trim_journals(Bookie, Schema),
    fts2_build_remove_deltas(Bookie, maps:get(index, Schema), DeltaRefs),
    {ok, PublishedRoot} = Result,
    case {
        client_option(reclaim, Opts, true),
        maps:get(previous_generation, PublishedRoot, undefined)
    } of
        {true, PreviousGeneration} when is_integer(PreviousGeneration) ->
            fts2_build_reclaim_ledgers(
                lists:usort([Bookie, fts2_build_identity_bookie(Bookie, Schema)]),
                client_option(reclaim_timeout_ms, Opts, 300000)
            );
        _NoSupersededGeneration ->
            ok
    end,
    fts2_build_trim_journals(Bookie, Schema),
    Result.

%% The coordinator retains only compact ordering descriptors and source ids.
%% Full records and posting graphs pass through generation-qualified workspace
%% rows. The rows are build spill, not a read cache: search never reads them,
%% and success/failure cleanup removes the complete workspace.
fts2_build_prepare_workspace(
    Bookie, Schema, Root, DeltaRefs, Generation, BuildConcurrency
) ->
    {DeltaDescriptors, RetiredIds} = fts2_build_delta_descriptors(
        Bookie, Schema, DeltaRefs, Generation, BuildConcurrency
    ),
    DeltaSources = maps:from_keys(
        [SourceId || {SourceId, _SQN} <- DeltaRefs], true
    ),
    Retired = maps:from_keys(RetiredIds, true),
    ExistingDescriptors = fts2_build_existing_descriptors(
        Bookie,
        Schema,
        Root,
        Generation,
        DeltaSources,
        Retired,
        BuildConcurrency
    ),
    fts2_build_select_groups(DeltaDescriptors ++ ExistingDescriptors).

fts2_build_delta_descriptors(
    Bookie, Schema, DeltaRefs, Generation, _BuildConcurrency
) ->
    Bucket = maps:get(index, Schema),
    Results = [
        fts2_build_run_worker(
            workspace_worker,
            fun() ->
                lists:map(
                    fun({SourceId, _SQN}) ->
                        Delta = fts2_build_read_delta(Bookie, Bucket, SourceId),
                        Retired = maps:get(retired_ids, Delta, []),
                        case maps:get(status, Delta) of
                            live ->
                                Metadata = maps:remove(posting, Delta),
                                ok = fts2_build_workspace_write(
                                    Bookie,
                                    Bucket,
                                    Generation,
                                    SourceId,
                                    <<"m">>,
                                    Metadata
                                ),
                                {SourceId, live, Retired,
                                    fts2_build_descriptor(
                                        Schema, Metadata, delta, undefined
                                    )};
                            remove ->
                                {SourceId, remove, Retired, undefined}
                        end
                    end,
                    RefBatch
                )
            end
        )
     || RefBatch <- fts2_build_chunks(DeltaRefs, 8, [])
    ],
    {DescriptorMap, RetiredIds} = lists:foldl(
        fun({SourceId, Status, Retired, Descriptor}, {Descriptors, RetiredAcc}) ->
            WithoutRetired = maps:without(Retired, Descriptors),
            NextDescriptors = case Status of
                live -> WithoutRetired#{SourceId => Descriptor};
                remove -> maps:remove(SourceId, WithoutRetired)
            end,
            {NextDescriptors, Retired ++ RetiredAcc}
        end,
        {#{}, []},
        lists:append(Results)
    ),
    {maps:values(DescriptorMap), RetiredIds}.

fts2_build_existing_descriptors(
    _Bookie, _Schema, undefined, _Generation, _DeltaSources, _Retired,
    _BuildConcurrency
) ->
    [];
fts2_build_existing_descriptors(
    Bookie, Schema, Root, Generation, DeltaSources, Retired,
    _BuildConcurrency
) ->
    PageCount = maps:get(identity_page_count, Root),
    lists:append([
        fts2_build_run_worker(
            workspace_worker,
            fun() ->
                fts2_build_existing_descriptor_page(
                    Bookie,
                    Schema,
                    Root,
                    Generation,
                    PageNo,
                    DeltaSources,
                    Retired
                )
            end
        )
     || PageNo <- lists:seq(0, erlang:max(PageCount - 1, -1))
    ]).

fts2_build_existing_descriptor_page(
    Bookie,
    #{index := Bucket} = Schema,
    Root,
    Generation,
    PageNo,
    DeltaSources,
    Retired
) ->
    IdentityBookie = fts2_build_identity_bookie(Bookie, Schema),
    Key = fts2_codec_identity_key(maps:get(generation, Root)),
    Shift = fts2_search_identity_page_shift(Root, Schema),
    FirstGroupId = PageNo bsl Shift,
    LastGroupId = erlang:min(
        maps:get(group_count, Root) - 1,
        FirstGroupId + (1 bsl Shift) - 1
    ),
    case leveled_bookie:book_headonly(
        IdentityBookie,
        Bucket,
        Key,
        fts2_codec_identity_subkey(PageNo)
    ) of
        not_found ->
            [];
        {ok, Value} ->
            Groups = maps:values(fts2_search_groups_to_map(
                fts2_codec_decode_identity_page(
                    Value, lists:seq(FirstGroupId, LastGroupId)
                )
            )),
            lists:foldl(
                fun(Group, Acc) ->
                    lists:foldl(
                        fun(Chunk, Inner) ->
                            SourceId = maps:get(source_id, Chunk),
                            case maps:is_key(SourceId, DeltaSources) orelse
                                maps:is_key(SourceId, Retired)
                            of
                                true ->
                                    Inner;
                                false ->
                                    Metadata = (maps:with(
                                        [
                                            source_id,
                                            doc_key,
                                            doc_version,
                                            doc_length,
                                            candidate_record,
                                            hit_record
                                        ],
                                        Chunk
                                    ))#{status => live, retired_ids => []},
                                    ok = fts2_build_workspace_write(
                                        Bookie,
                                        Bucket,
                                        Generation,
                                        SourceId,
                                        <<"m">>,
                                        Metadata
                                    ),
                                    [
                                        fts2_build_descriptor(
                                            Schema,
                                            Metadata,
                                            existing,
                                            maps:get(chunk_id, Chunk)
                                        )
                                        | Inner
                                    ]
                            end
                        end,
                        Acc,
                        maps:get(chunks, Group)
                    )
                end,
                [],
                Groups
            )
    end.

%% {GroupKey, Version, DocKey, SourceId, Length, Origin, OldChunkId} is the
%% complete coordinator state per candidate. It deliberately excludes posting,
%% candidate, and hit records.
fts2_build_descriptor(Schema, Metadata, Origin, OldChunkId) ->
    SourceId = maps:get(source_id, Metadata),
    Candidate = maps:get(candidate_record, Metadata),
    {
        fts2_build_group_key(
            maps:get(candidate_group_fields, Schema, []),
            SourceId,
            Candidate
        ),
        fts2_build_group_version(
            maps:get(candidate_version_field, Schema, undefined),
            Candidate
        ),
        maps:get(doc_key, Metadata),
        SourceId,
        maps:get(doc_length, Metadata, 0),
        Origin,
        OldChunkId
    }.

fts2_build_select_groups(Descriptors) ->
    Ordered = lists:sort(
        fun(A, B) ->
            {element(1, A), element(3, A), element(4, A)} =<
                {element(1, B), element(3, B), element(4, B)}
        end,
        Descriptors
    ),
    fts2_build_select_groups(Ordered, []).

fts2_build_select_groups([], Acc) ->
    lists:reverse(Acc);
fts2_build_select_groups([First | Rest], Acc) ->
    GroupKey = element(1, First),
    {SameGroup, Tail} = lists:splitwith(
        fun(Descriptor) -> element(1, Descriptor) =:= GroupKey end,
        [First | Rest]
    ),
    Version = lists:max([element(2, Descriptor) || Descriptor <- SameGroup]),
    Rows = [
        Descriptor
     || Descriptor <- SameGroup,
        element(2, Descriptor) =:= Version
    ],
    true = length(Rows) =< ?CHUNKS_PER_GROUP,
    fts2_build_select_groups(Tail, [{GroupKey, Rows} | Acc]).

fts2_build_publish_workspace(
    Bookie,
    Schema,
    PreviousRoot,
    Previous,
    Generation,
    SelectedGroups,
    CommitSpecs,
    CommitConditions,
    BuildConcurrency
) ->
    Bucket = maps:get(index, Schema),
    Identity = fts2_build_workspace_identities(
        Bookie,
        Schema,
        Generation,
        SelectedGroups,
        BuildConcurrency
    ),
    Sources = maps:get(sources, Identity),
    ok = fts2_build_workspace_postings(
        Bookie,
        Schema,
        PreviousRoot,
        Generation,
        Sources,
        BuildConcurrency
    ),
    TermState = fts2_build_workspace_terms(
        Bookie,
        Schema,
        Generation,
        maps:get(group_count, Identity),
        maps:get(total_length, Identity),
        BuildConcurrency
    ),
    BigramState = fts2_build_workspace_bigrams(
        Bookie,
        Schema,
        Generation,
        maps:get(frequent_terms, TermState),
        maps:get(chunk_count, Identity),
        maps:get(total_length, Identity),
        BuildConcurrency
    ),
    Root = #{
        version => 1,
        generation => Generation,
        fingerprint => maps:get(fingerprint, Schema),
        chunk_bits => ?CHUNK_BITS,
        group_count => maps:get(group_count, Identity),
        chunk_count => maps:get(chunk_count, Identity),
        term_count => maps:get(term_count, TermState),
        bigram_count => maps:get(bigram_count, BigramState),
        identity_page_count => fts2_build_identity_page_count(
            maps:get(group_count, Identity),
            ?GENERATION_IDENTITY_PAGE_SHIFT
        ),
        identity_page_shift => ?GENERATION_IDENTITY_PAGE_SHIFT,
        total_length => maps:get(total_length, Identity),
        phrase_strategy => bigram,
        facet_domain => maps:get(facet_domain, Identity),
        term_bloom => 1,
        term_bloom_shards => ?TERM_BLOOM_SHARDS,
        bigram_bloom_shards => ?BIGRAM_BLOOM_SHARDS,
        position_order => chunk,
        group_order => native,
        group_tie_fields => maps:get(candidate_group_fields, Schema, []),
        previous_generation => fts2_build_previous_generation(Previous)
    },
    fts2_build_publish_root(
        Bookie,
        Bucket,
        Previous,
        Root,
        maps:get(term_bloom, TermState),
        maps:get(term_bloom_shards, TermState),
        maps:get(bigram_bloom_shards, BigramState),
        CommitSpecs,
        CommitConditions
    ),
    {ok, Root#{row_count =>
        maps:get(row_count, Identity) +
            maps:get(row_count, TermState) +
            maps:get(row_count, BigramState)}}.

fts2_build_workspace_identities(
    Bookie, Schema, Generation, SelectedGroups, _BuildConcurrency
) ->
    fts2_build_workspace_identities(
        Bookie,
        Schema,
        Generation,
        SelectedGroups,
        0,
        0,
        #{
            sources => [],
            group_count => 0,
            chunk_count => 0,
            total_length => 0,
            facet_domain => #{},
            row_count => 0
        }
    ).

fts2_build_workspace_identities(
    _Bookie, _Schema, _Generation, [], _GroupId, _DenseId, State
) ->
    State#{sources := lists:reverse(maps:get(sources, State))};
fts2_build_workspace_identities(
    Bookie,
    Schema,
    Generation,
    Groups,
    GroupId,
    DenseId,
    State
) ->
    Count = erlang:min(1 bsl ?GENERATION_IDENTITY_PAGE_SHIFT, length(Groups)),
    {PageGroups, Rest} = lists:split(Count, Groups),
    Page = fts2_build_run_worker(
        identity_worker,
        fun() ->
            fts2_build_workspace_identity_page(
                Bookie,
                Schema,
                Generation,
                PageGroups,
                GroupId,
                DenseId
            )
        end
    ),
    NextState = State#{
        sources := lists:reverse(maps:get(sources, Page)) ++
            maps:get(sources, State),
        group_count := maps:get(group_count, State) +
            maps:get(group_count, Page),
        chunk_count := maps:get(chunk_count, State) +
            maps:get(chunk_count, Page),
        total_length := maps:get(total_length, State) +
            maps:get(total_length, Page),
        facet_domain := fts2_build_merge_facet_domains(
            maps:get(facet_domain, State),
            maps:get(facet_domain, Page)
        ),
        row_count := maps:get(row_count, State) + maps:get(row_count, Page)
    },
    fts2_build_workspace_identities(
        Bookie,
        Schema,
        Generation,
        Rest,
        GroupId + maps:get(group_count, Page),
        maps:get(next_dense_id, Page),
        NextState
    ).

fts2_build_workspace_identity_page(
    Bookie,
    #{index := Bucket} = Schema,
    Generation,
    PageGroups,
    FirstGroupId,
    FirstDenseId
) ->
    {Groups, Sources, NextDenseId, ChunkCount, TotalLength, FacetDomain} =
        lists:foldl(
            fun({GroupKey, Descriptors},
                {GroupAcc, SourceAcc, DenseId0, ChunkAcc, LengthAcc,
                    DomainAcc}) ->
                GroupId = FirstGroupId + length(GroupAcc),
                GroupLength = lists:sum([
                    element(5, Descriptor) || Descriptor <- Descriptors
                ]),
                {Chunks, NextDenseId0, NextSources, NextDomain} =
                    lists:foldl(
                        fun(Descriptor,
                            {ChunkRows, DenseId, Sources0, Domain0}) ->
                            SourceId = element(4, Descriptor),
                            Metadata = fts2_build_workspace_read(
                                Bookie,
                                Bucket,
                                Generation,
                                SourceId,
                                <<"m">>
                            ),
                            Local = length(ChunkRows),
                            ChunkId = (GroupId bsl ?CHUNK_BITS) bor Local,
                            Chunk = Metadata#{
                                chunk_id => ChunkId,
                                group_id => GroupId,
                                dense_id => DenseId,
                                group_length => GroupLength
                            },
                            ok = fts2_build_workspace_write(
                                Bookie,
                                Bucket,
                                Generation,
                                SourceId,
                                <<"m">>,
                                {
                                    ChunkId,
                                    GroupId,
                                    SourceId,
                                    maps:get(doc_length, Metadata, 0),
                                    DenseId,
                                    GroupLength
                                }
                            ),
                            {
                                [Chunk | ChunkRows],
                                DenseId + 1,
                                [{SourceId, element(6, Descriptor),
                                    element(7, Descriptor)} | Sources0],
                                fts2_build_facet_add(
                                    Schema,
                                    maps:get(candidate_record, Metadata, #{}),
                                    Domain0
                                )
                            }
                        end,
                        {[], DenseId0, SourceAcc, DomainAcc},
                        Descriptors
                    ),
                Group = #{
                    group_id => GroupId,
                    group_key => GroupKey,
                    chunks => lists:reverse(Chunks)
                },
                {
                    [Group | GroupAcc],
                    NextSources,
                    NextDenseId0,
                    ChunkAcc + length(Descriptors),
                    LengthAcc + GroupLength,
                    NextDomain
                }
            end,
            {[], [], FirstDenseId, 0, 0, #{}},
            PageGroups
        ),
    IdentityBookie = fts2_build_identity_bookie(Bookie, Schema),
    Key = fts2_codec_identity_key(Generation),
    PageNo = FirstGroupId bsr ?GENERATION_IDENTITY_PAGE_SHIFT,
    ok = fts2_build_write_slices(
        IdentityBookie,
        [{add, Bucket, Key, fts2_codec_identity_subkey(PageNo),
            fts2_codec_encode_identity_page(lists:reverse(Groups))}]
    ),
    #{
        sources => Sources,
        group_count => length(PageGroups),
        chunk_count => ChunkCount,
        total_length => TotalLength,
        facet_domain => FacetDomain,
        row_count => 1,
        next_dense_id => NextDenseId
    }.

fts2_build_facet_add(Schema, Candidate, Domain0) ->
    lists:foldl(
        fun({Column, Field}, Domain) ->
            Value = maps:get(Field, Candidate, undefined),
            case maps:get(Column, Domain, unset) of
                unset -> Domain#{Column => {uniform, Value}};
                {uniform, Value} -> Domain;
                _Other -> Domain#{Column => mixed}
            end
        end,
        Domain0,
        maps:get(candidate_filter_fields, Schema, [])
    ).

fts2_build_merge_facet_domains(Left, Right) ->
    maps:fold(
        fun(Column, Value, Acc) ->
            case {maps:get(Column, Acc, unset), Value} of
                {unset, _} -> Acc#{Column => Value};
                {{uniform, Same}, {uniform, Same}} -> Acc;
                _ -> Acc#{Column => mixed}
            end
        end,
        Left,
        Right
    ).

fts2_build_workspace_postings(
    Bookie, Schema, PreviousRoot, Generation, Sources, _BuildConcurrency
) ->
    DeltaSources = [Source || Source = {_SourceId, delta, _Old} <- Sources],
    ExistingSources = [
        Source || Source = {_SourceId, existing, _Old} <- Sources
    ],
    lists:foreach(
        fun(Batch) ->
            _ = fts2_build_run_worker(
                workspace_worker,
                fun() ->
                    lists:foreach(
                        fun({SourceId, delta, _OldChunkId}) ->
                            Delta = fts2_build_read_delta(
                                Bookie, maps:get(index, Schema), SourceId
                            ),
                            fts2_build_write_document_fragments(
                                Bookie,
                                Schema,
                                Generation,
                                SourceId,
                                fts2_build_workspace_read(
                                    Bookie,
                                    maps:get(index, Schema),
                                    Generation,
                                    SourceId,
                                    <<"m">>
                                ),
                                maps:get(posting, Delta, #{})
                            )
                        end,
                        Batch
                    ),
                    0
                end
            )
        end,
        fts2_build_chunks(DeltaSources, 1, [])
    ),
    lists:foreach(
        fun(Batch) ->
            _ = fts2_build_run_worker(
                workspace_worker,
                fun() ->
                    fts2_build_reconstruct_existing_fragments(
                        Bookie, Schema, PreviousRoot, Generation, Batch
                    )
                end
            )
        end,
        fts2_build_chunks(ExistingSources, 128, [])
    ),
    ok.

fts2_build_reconstruct_existing_fragments(
    _Bookie, _Schema, undefined, _Generation, []
) ->
    0;
fts2_build_reconstruct_existing_fragments(
    Bookie, #{index := Bucket} = Schema, Root, Generation, Sources
) ->
    Wanted = maps:from_list([
        {OldChunkId, SourceId}
     || {SourceId, existing, OldChunkId} <- Sources
    ]),
    Documents0 = maps:from_keys(
        [SourceId || {SourceId, existing, _OldChunkId} <- Sources], #{}
    ),
    OldGeneration = maps:get(generation, Root),
    Fold = fun
        (B, {Key, <<"b">>}, Value, Documents) when B =:= Bucket ->
            case Key of
                <<"f2:b:", OldGeneration:64/unsigned-big, Column:8,
                    Token/binary>> ->
                    lists:foldl(
                        fun({ChunkId, _GroupId, _StoredSourceId, _Length,
                                Tf, _DenseId}, Acc) ->
                            SourceId = maps:get(ChunkId, Wanted),
                            Posting = maps:get(SourceId, Acc),
                            Tokens = maps:get(Column, Posting, #{}),
                            Acc#{SourceId => Posting#{Column => Tokens#{
                                Token => #{count => Tf, positions => []}
                            }}}
                        end,
                        Documents,
                        fts2_codec_decode_selected_plane(Value, Wanted)
                    );
                _ ->
                    Documents
            end;
        (B, {Key, <<"p">>}, Value, Documents) when B =:= Bucket ->
            case Key of
                <<"f2:p:", OldGeneration:64/unsigned-big, Column:8,
                    Token/binary>> ->
                    Positions = fts2_codec_decode_positions(
                        Value, maps:keys(Wanted)
                    ),
                    maps:fold(
                        fun(ChunkId, ChunkPositions, Acc) ->
                            SourceId = maps:get(ChunkId, Wanted),
                            Posting = maps:get(SourceId, Acc),
                            Tokens = maps:get(Column, Posting),
                            Entry = maps:get(Token, Tokens),
                            Acc#{SourceId => Posting#{Column => Tokens#{
                                Token => Entry#{positions := ChunkPositions}
                            }}}
                        end,
                        Documents,
                        Positions
                    );
                _ ->
                    Documents
            end;
        (_B, _Key, _Value, Documents) ->
            Documents
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {
            {<<"f2:b:">>, <<>>},
            {<<"f2:u">>, <<255>>}
        }},
        {Fold, Documents0},
        false,
        true,
        false
    ),
    maps:foreach(
        fun(SourceId, Posting) ->
            fts2_build_write_document_fragments(
                Bookie,
                Schema,
                Generation,
                SourceId,
                fts2_build_workspace_read(
                    Bookie, Bucket, Generation, SourceId, <<"m">>
                ),
                Posting
            )
        end,
        Runner()
    ),
    0.

%% One row per (kind, shard, source). The row carries the source's chunk
%% metadata, so a shard worker needs no per-source lookup and no source list.
%% A shard worker therefore reads exactly its own shard once, instead of every
%% source's complete fragment map once per shard.
%% Streamed. A document's posting is the atomic delta grain, so the worker
%% must hold it; it must not hold a second reshaped copy of it, a third
%% encoded copy of it, and the bigram graph all at once. Fragments are
%% therefore emitted in bounded rounds: entries accumulate until their
%% position weight reaches the flush threshold, the round is written, and the
%% round is released before the next one starts. Terms are written and freed
%% before the bigram graph is built. Rounds are separate rows under the same
%% shard key, which the shard-major range fold reads as one stream.
fts2_build_write_document_fragments(
    Bookie, #{index := Bucket}, Generation, SourceId, Metadata, Posting
) ->
    ok = fts2_build_stream_term_fragments(
        Bookie, Bucket, Generation, SourceId, Metadata, Posting
    ),
    fts2_build_stream_bigram_fragments(
        Bookie, Bucket, Generation, SourceId, Metadata, Posting
    ).

fts2_build_stream_term_fragments(
    Bookie, Bucket, Generation, SourceId, Metadata, Posting
) ->
    {Buckets, Weight, Round} = maps:fold(
        fun(Column, Tokens, Acc0) ->
            maps:fold(
                fun(Token, Entry, {Buckets0, Weight0, Round0}) ->
                    Shard = fts2_build_term_shard({Column, Token}),
                    Buckets1 = Buckets0#{Shard => [
                        {Column, Token, Entry} | maps:get(Shard, Buckets0, [])
                    ]},
                    Weight1 = Weight0 + 1 + maps:get(count, Entry, 0),
                    case Weight1 >= ?FRAGMENT_FLUSH_WEIGHT of
                        true ->
                            ok = fts2_build_flush_fragments(
                                Bookie, Bucket, Generation, $t, SourceId,
                                Round0, Metadata, Buckets1
                            ),
                            {#{}, 0, Round0 + 1};
                        false ->
                            {Buckets1, Weight1, Round0}
                    end
                end,
                Acc0,
                Tokens
            )
        end,
        {#{}, 0, 0},
        Posting
    ),
    case Weight of
        0 -> ok;
        _ ->
            fts2_build_flush_fragments(
                Bookie, Bucket, Generation, $t, SourceId, Round, Metadata,
                Buckets
            )
    end.

fts2_build_stream_bigram_fragments(
    Bookie, Bucket, Generation, SourceId, Metadata, Posting
) ->
    %% The document's bigram graph is produced in position order and flushed
    %% in bounded rounds, so the worker never holds the whole graph. A bigram
    %% key may therefore appear in more than one round; the shard worker
    %% merges rounds, and its position lists are sorted and counted there, so
    %% the merge is exact.
    Context = {Bookie, Bucket, Generation, SourceId, Metadata},
    {Rows, Weight, Round} = maps:fold(
        fun(Column, Tokens, State0) ->
            {Streams, _Ordinal} = maps:fold(
                fun(Token, Entry, {Tree, Ordinal}) ->
                    case maps:get(positions, Entry) of
                        [] ->
                            {Tree, Ordinal + 1};
                        [Position | Rest] ->
                            {
                                gb_trees:enter(
                                    {Position, Ordinal},
                                    {Token, Entry, Rest},
                                    Tree
                                ),
                                Ordinal + 1
                            }
                    end
                end,
                {gb_trees:empty(), 0},
                Tokens
            ),
            fts2_build_document_bigram_stream(
                Context, Column, Streams, none, State0
            )
        end,
        {#{}, 0, 0},
        Posting
    ),
    case Weight of
        0 -> ok;
        _ -> fts2_build_flush_bigram_round(Context, Round, Rows)
    end.

fts2_build_flush_bigram_round(
    {Bookie, Bucket, Generation, SourceId, Metadata}, Round, Rows
) ->
    Buckets = maps:fold(
        fun(Key, {FirstTf, SecondTf, Positions}, Acc) ->
            Shard = fts2_build_bigram_shard(Key),
            Acc#{Shard => [
                {Key, FirstTf, SecondTf, Positions} | maps:get(Shard, Acc, [])
            ]}
        end,
        #{},
        Rows
    ),
    fts2_build_flush_fragments(
        Bookie, Bucket, Generation, $g, SourceId, Round, Metadata, Buckets
    ).

fts2_build_flush_fragments(
    Bookie, Bucket, Generation, Kind, SourceId, Round, Metadata, Buckets
) ->
    SubKey = <<SourceId:64/unsigned-big, Round:16/unsigned-big>>,
    fts2_build_write_slices(
        Bookie,
        [
            {add, Bucket, fts2_build_fragment_key(Generation, Kind, Shard),
                SubKey, fts2_codec_encode(delta, {Metadata, Entries})}
         || {Shard, Entries} <- maps:to_list(Buckets)
        ]
    ).

%% Shares the workspace key prefix so generation cleanup removes it unchanged.
%% The 11-byte suffix cannot collide with a 16-byte source workspace key.
fts2_build_fragment_key(Generation, Kind, Shard) ->
    <<"f2:w:", Generation:64/unsigned-big, Kind:8, Shard:16/unsigned-big>>.

fts2_build_document_bigram_stream(Context, Column, Streams, Previous, State) ->
    case gb_trees:is_empty(Streams) of
        true ->
            State;
        false ->
            {{Position, Ordinal}, {Token, Entry, Rest}, NextStreams0} =
                gb_trees:take_smallest(Streams),
            NextStreams = case Rest of
                [] -> NextStreams0;
                [NextPosition | Tail] ->
                    gb_trees:enter(
                        {NextPosition, Ordinal},
                        {Token, Entry, Tail},
                        NextStreams0
                    )
            end,
            NextState = case Previous of
                {PreviousPosition, First, FirstEntry} when
                    Position =:= PreviousPosition + 1
                ->
                    fts2_build_note_bigram(
                        Context,
                        {Column, First, Token},
                        maps:get(count, FirstEntry),
                        maps:get(count, Entry),
                        PreviousPosition,
                        State
                    );
                _ ->
                    State
            end,
            fts2_build_document_bigram_stream(
                Context, Column, NextStreams, {Position, Token, Entry},
                NextState
            )
    end.

fts2_build_note_bigram(
    Context, Key, FirstTf, SecondTf, Position, {Rows0, Weight0, Round}
) ->
    Rows = case maps:find(Key, Rows0) of
        error ->
            Rows0#{Key => {FirstTf, SecondTf, [Position]}};
        {ok, {ExistingFirstTf, ExistingSecondTf, Positions}} ->
            Rows0#{Key => {
                ExistingFirstTf, ExistingSecondTf, [Position | Positions]
            }}
    end,
    Weight = Weight0 + 1,
    case Weight >= ?FRAGMENT_FLUSH_WEIGHT of
        true ->
            ok = fts2_build_flush_bigram_round(Context, Round, Rows),
            {#{}, 0, Round + 1};
        false ->
            {Rows, Weight, Round}
    end.

fts2_build_workspace_terms(
    Bookie, Schema, Generation, GroupCount, TotalLength, _BuildConcurrency
) ->
    %% Bloom bits travel as sparse word maps and are encoded once, here. A
    %% per-shard binary OR would cost the full bloom width on every shard.
    Initial = #{
        term_count => 0,
        row_count => 0,
        frequent_terms => #{},
        bloom_words => #{},
        bloom_shard_words => #{}
    },
    State = lists:foldl(
        fun(Shard, Acc) ->
            Result = fts2_build_run_worker(
                term_shard_worker,
                fun() ->
                    fts2_build_workspace_term_shard(
                        Bookie,
                        Schema,
                        Generation,
                        Shard,
                        GroupCount,
                        TotalLength
                    )
                end
            ),
            Acc#{
                term_count := maps:get(term_count, Acc) +
                    maps:get(term_count, Result),
                row_count := maps:get(row_count, Acc) +
                    maps:get(row_count, Result),
                frequent_terms := maps:merge(
                    maps:get(frequent_terms, Acc),
                    maps:get(frequent_terms, Result)
                ),
                bloom_words := fts2_build_merge_bloom_words(
                    maps:get(bloom_words, Acc),
                    maps:get(bloom_words, Result)
                ),
                bloom_shard_words := fts2_build_merge_bloom_shard_words(
                    maps:get(bloom_shard_words, Acc),
                    maps:get(bloom_shard_words, Result)
                )
            }
        end,
        Initial,
        lists:seq(0, fts2_build_term_shards() - 1)
    ),
    State#{
        term_bloom => fts2_build_encode_bloom_words(
            maps:get(bloom_words, State), ?TERM_BLOOM_WORDS
        ),
        term_bloom_shards => fts2_build_encode_bloom_shard_words(
            maps:get(bloom_shard_words, State),
            ?TERM_BLOOM_SHARDS,
            ?TERM_BLOOM_SHARD_WORDS
        )
    }.

fts2_build_merge_bloom_words(Left, Right) ->
    maps:fold(
        fun(Word, Mask, Acc) ->
            Acc#{Word => maps:get(Word, Acc, 0) bor Mask}
        end,
        Left,
        Right
    ).

fts2_build_merge_bloom_shard_words(Left, Right) ->
    maps:fold(
        fun(Shard, Words, Acc) ->
            Acc#{Shard => fts2_build_merge_bloom_words(
                maps:get(Shard, Acc, #{}), Words
            )}
        end,
        Left,
        Right
    ).

fts2_build_encode_bloom_words(Words, WordCount) ->
    iolist_to_binary([
        <<(maps:get(Word, Words, 0)):64/unsigned-little>>
     || Word <- lists:seq(0, WordCount - 1)
    ]).

fts2_build_encode_bloom_shard_words(ShardWords, ShardCount, WordCount) ->
    [
        {Shard, fts2_build_encode_bloom_words(
            maps:get(Shard, ShardWords, #{}), WordCount
        )}
     || Shard <- lists:seq(0, ShardCount - 1)
    ].

fts2_build_workspace_term_shard(
    Bookie,
    #{index := Bucket},
    Generation,
    Shard,
    GroupCount,
    TotalLength
) ->
    TermRows = fts2_build_workspace_fold_fragments(
        Bookie,
        Bucket,
        Generation,
        $t,
        Shard,
        fun(Metadata, Entries, Acc0) ->
            lists:foldl(
                fun({Column, Token, Entry}, Acc) ->
                    fts2_build_add_term_row_entry(
                        Column, Token, Entry, Metadata, Acc
                    )
                end,
                Acc0,
                Entries
            )
        end,
        #{}
    ),
    TermCount = map_size(TermRows),
    %% The map is not referenced past this point. Each row is released once its
    %% batch is written, so the write phase never holds the whole shard and its
    %% encoded rows at the same time. Bloom bits and frequent-term keys are
    %% collected on the same single pass rather than by re-walking the shard.
    State = fts2_build_stream_term_rows(
        Bookie,
        Bucket,
        Generation,
        maps:to_list(TermRows),
        GroupCount,
        TotalLength,
        [],
        0,
        #{
            row_count => 0,
            frequent_terms => #{},
            bloom_words => #{},
            bloom_shard_words => #{}
        }
    ),
    State#{term_count => TermCount}.

fts2_build_stream_term_rows(
    _Bookie, _Bucket, _Generation, [], _GroupCount, _TotalLength, [], _Size,
    State
) ->
    State;
fts2_build_stream_term_rows(
    Bookie, Bucket, Generation, [], GroupCount, TotalLength, Batch, _Size,
    State
) ->
    Written = fts2_build_write_term_batch(
        Bookie, Bucket, Generation, Batch, GroupCount, TotalLength
    ),
    State#{row_count := maps:get(row_count, State) + Written};
fts2_build_stream_term_rows(
    Bookie, Bucket, Generation, [{Key, Row} | Rest], GroupCount, TotalLength,
    Batch, Size, State0
) ->
    State = fts2_build_note_term_row(Key, Row, State0),
    case Size + 1 >= 48 of
        true ->
            Written = fts2_build_write_term_batch(
                Bookie,
                Bucket,
                Generation,
                [{Key, Row} | Batch],
                GroupCount,
                TotalLength
            ),
            fts2_build_stream_term_rows(
                Bookie,
                Bucket,
                Generation,
                Rest,
                GroupCount,
                TotalLength,
                [],
                0,
                State#{row_count := maps:get(row_count, State) + Written}
            );
        false ->
            fts2_build_stream_term_rows(
                Bookie,
                Bucket,
                Generation,
                Rest,
                GroupCount,
                TotalLength,
                [{Key, Row} | Batch],
                Size + 1,
                State
            )
    end.

fts2_build_note_term_row({Column, Token}, Row, State) ->
    Df = length(maps:get(entries, Row)),
    Frequent = case Df >= 64 of
        true -> (maps:get(frequent_terms, State))#{{Column, Token} => Df};
        false -> maps:get(frequent_terms, State)
    end,
    BloomShard = fts2_term_bloom_shard(Column, Token),
    ShardWords = maps:get(bloom_shard_words, State),
    State#{
        frequent_terms := Frequent,
        bloom_words := fts2_term_bloom_add(
            Column, Token, 0, maps:get(bloom_words, State)
        ),
        bloom_shard_words := ShardWords#{
            BloomShard => fts2_term_bloom_shard_add(
                Column, Token, 0, maps:get(BloomShard, ShardWords, #{})
            )
        }
    }.

fts2_build_add_term_row_entry(Column, Token, Entry, Metadata, Terms) ->
    {ChunkId, GroupId, SourceId, Length, DenseId, GroupLength} = Metadata,
    Tf = maps:get(count, Entry),
    Positions = maps:get(positions, Entry),
    PlaneEntry = {ChunkId, GroupId, SourceId, Length, Tf, DenseId},
    Key = {Column, Token},
    Row0 = maps:get(Key, Terms, #{entries => [], positions => []}),
    Row = Row0#{
        entries := [PlaneEntry | maps:get(entries, Row0)],
        group_entries => [
            {ChunkId, GroupId, SourceId, GroupLength, Tf, GroupId}
            | maps:get(group_entries, Row0, [])
        ],
        positions := [{ChunkId, Positions} | maps:get(positions, Row0)]
    },
    Terms#{Key => Row}.

fts2_build_write_term_batch(
    Bookie, Bucket, Generation, Rows, GroupCount, TotalLength
) ->
    Specs = lists:append([
        fts2_build_term_row_specs(
            Bucket, Generation, Column, Token, Row, GroupCount, TotalLength
        )
     || {{Column, Token}, Row} <- Rows
    ]),
    ok = fts2_build_write_slices(Bookie, Specs),
    length(Specs).

fts2_build_workspace_bigrams(
    Bookie, Schema, Generation, FrequentTerms, ChunkCount, TotalLength,
    _BuildConcurrency
) ->
    Initial = #{
        bigram_count => 0,
        row_count => 0,
        bloom_shard_words => #{}
    },
    State = lists:foldl(
        fun(Shard, Acc) ->
            Result = fts2_build_run_worker(
                bigram_shard_worker,
                fun() ->
                    fts2_build_workspace_bigram_shard(
                        Bookie,
                        Schema,
                        Generation,
                        Shard,
                        FrequentTerms,
                        ChunkCount,
                        TotalLength
                    )
                end
            ),
            Acc#{
                bigram_count := maps:get(bigram_count, Acc) +
                    maps:get(bigram_count, Result),
                row_count := maps:get(row_count, Acc) +
                    maps:get(row_count, Result),
                bloom_shard_words := fts2_build_merge_bloom_shard_words(
                    maps:get(bloom_shard_words, Acc),
                    maps:get(bloom_shard_words, Result)
                )
            }
        end,
        Initial,
        lists:seq(0, fts2_build_bigram_shards() - 1)
    ),
    State#{bigram_bloom_shards => fts2_build_encode_bloom_shard_words(
        maps:get(bloom_shard_words, State),
        ?BIGRAM_BLOOM_SHARDS,
        ?BIGRAM_BLOOM_SHARD_WORDS
    )}.

fts2_build_workspace_bigram_shard(
    Bookie,
    #{index := Bucket},
    Generation,
    Shard,
    FrequentTerms,
    ChunkCount,
    TotalLength
) ->
    {AllRows, BloomWords} = fts2_build_workspace_fold_fragments(
        Bookie,
        Bucket,
        Generation,
        $g,
        Shard,
        fun(Metadata, Entries, {Rows0, Bloom0}) ->
            lists:foldl(
                fun({{Column, First, Second} = Key, FirstTf, SecondTf,
                        Positions}, {Rows, Bloom}) ->
                    NextBloom = fts2_build_bigram_bloom_add(
                        Column, First, Second, Bloom
                    ),
                    case maps:is_key({Column, First}, FrequentTerms) andalso
                        maps:is_key({Column, Second}, FrequentTerms)
                    of
                        true ->
                            {ChunkId, GroupId, SourceId, Length,
                                _DenseId, _GroupLength} = Metadata,
                            PlaneEntry = {
                                ChunkId, GroupId, SourceId, Length,
                                FirstTf, 0
                            },
                            Row0 = maps:get(Key, Rows, #{}),
                            %% A document emits its bigram graph in bounded
                            %% rounds, so one key can arrive more than once
                            %% for the same chunk. Positions concatenate; the
                            %% row encoder sorts and counts them.
                            Merged = case maps:find(ChunkId, Row0) of
                                error ->
                                    {PlaneEntry, SecondTf, Positions};
                                {ok, {Existing, ExistingSecondTf, Prior}} ->
                                    {Existing, ExistingSecondTf,
                                        Positions ++ Prior}
                            end,
                            {
                                Rows#{Key => Row0#{ChunkId => Merged}},
                                NextBloom
                            };
                        false ->
                            {Rows, NextBloom}
                    end
                end,
                {Rows0, Bloom0},
                Entries
            )
        end,
        {#{}, #{}}
    ),
    BigramRows = maps:filter(
        fun(_Key, Chunks) -> map_size(Chunks) >= ?BIGRAM_MIN_CHUNKS end,
        AllRows
    ),
    BigramCount = map_size(BigramRows),
    %% As for terms: the surviving rows are consumed as a list, so a written
    %% batch is released before the next one is encoded.
    RowCount = fts2_build_write_bigram_rows(
        Bookie,
        Bucket,
        Generation,
        maps:to_list(BigramRows),
        FrequentTerms,
        ChunkCount,
        TotalLength
    ),
    #{
        bigram_count => BigramCount,
        row_count => RowCount,
        bloom_shard_words => BloomWords
    }.

fts2_build_write_bigram_rows(
    Bookie, Bucket, Generation, BigramRows, FrequentTerms, ChunkCount,
    TotalLength
) ->
    {Batch, BatchSize, Count} = lists:foldl(
        fun({{Column, First, Second}, ByChunk}, {Rows, Size, RowCount}) ->
            Item = {
                {Column, First, Second},
                ByChunk,
                maps:get({Column, First}, FrequentTerms),
                maps:get({Column, Second}, FrequentTerms)
            },
            Next = [Item | Rows],
            case Size + 1 >= 96 of
                true ->
                    Written = fts2_build_write_bigram_batch(
                        Bookie,
                        Bucket,
                        Generation,
                        Next,
                        ChunkCount,
                        TotalLength
                    ),
                    {[], 0, RowCount + Written};
                false ->
                    {Next, Size + 1, RowCount}
            end
        end,
        {[], 0, 0},
        BigramRows
    ),
    Count + case BatchSize of
        0 -> 0;
        _ -> fts2_build_write_bigram_batch(
            Bookie, Bucket, Generation, Batch, ChunkCount, TotalLength
        )
    end.

fts2_build_write_bigram_batch(
    Bookie, Bucket, Generation, Rows, ChunkCount, TotalLength
) ->
    Specs = lists:append([
        fts2_build_bigram_row_specs(
            Bucket,
            Generation,
            Column,
            First,
            Second,
            ByChunk,
            FirstDf,
            SecondDf,
            ChunkCount,
            TotalLength
        )
     || {{Column, First, Second}, ByChunk, FirstDf, SecondDf} <- Rows
    ]),
    ok = fts2_build_write_slices(Bookie, Specs),
    length(Specs).

%% One range fold over the shard's own key. Nothing outside the shard is read
%% and nothing outside the shard is decoded.
fts2_build_workspace_fold_fragments(
    Bookie, Bucket, Generation, Kind, Shard, Fun, Acc0
) ->
    Key = fts2_build_fragment_key(Generation, Kind, Shard),
    Fold = fun
        (B, {K, _SubKey}, Value, Acc) when B =:= Bucket, K =:= Key ->
            {Metadata, Entries} = fts2_codec_decode(delta, Value),
            Fun(Metadata, Entries, Acc);
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {
            {Key, <<>>},
            {Key, <<255, 255, 255, 255, 255, 255, 255, 255, 255, 255>>}
        }},
        {Fold, Acc0},
        false,
        true,
        false
    ),
    Runner().

fts2_build_term_shard(Key) ->
    erlang:phash2(Key, fts2_build_term_shards()).

fts2_build_bigram_shard(Key) ->
    erlang:phash2(Key, fts2_build_bigram_shards()).

fts2_build_workspace_key(Generation, SourceId) ->
    <<"f2:w:", Generation:64/unsigned-big, SourceId:64/unsigned-big>>.

fts2_build_workspace_write(
    Bookie, Bucket, Generation, SourceId, SubKey, Value
) ->
    fts2_build_write_slices(
        Bookie,
        [{add, Bucket, fts2_build_workspace_key(Generation, SourceId),
            SubKey, fts2_codec_encode(delta, Value)}]
    ).

fts2_build_workspace_read(
    Bookie, Bucket, Generation, SourceId, SubKey
) ->
    case leveled_bookie:book_headonly(
        Bookie,
        Bucket,
        fts2_build_workspace_key(Generation, SourceId),
        SubKey
    ) of
        {ok, Value} -> fts2_codec_decode(delta, Value);
        not_found -> erlang:error({fts2_workspace_row_missing, SourceId, SubKey})
    end.

fts2_build_read_delta(Bookie, Bucket, SourceId) ->
    case leveled_bookie:book_headonly(
        Bookie, Bucket, <<"f2:d">>, <<SourceId:64/unsigned-big>>
    ) of
        {ok, Value} -> fts2_codec_decode(delta, Value);
        not_found -> erlang:error({fts2_delta_disappeared, SourceId})
    end.

fts2_build_run_worker(Role, Fun) ->
    Parent = self(),
    Ref = make_ref(),
    0 = fts2_build_parallel(
        Role,
        [run],
        1,
        fun(run) ->
            Result = Fun(),
            Parent ! {fts2_build_worker_result, Ref, Result},
            0
        end
    ),
    receive
        {fts2_build_worker_result, Ref, Result} -> Result
    after 1000 ->
        erlang:error({fts2_build_worker_result_missing, Role})
    end.

fts2_build_reclaim_ledgers(Bookies, Timeout) when
    is_integer(Timeout), Timeout > 0
->
    Started = erlang:monotonic_time(millisecond),
    lists:foreach(
        fun(Bookie) ->
            Elapsed = erlang:monotonic_time(millisecond) - Started,
            Remaining = erlang:max(Timeout - Elapsed, 1),
            case leveled_bookie:book_reclaimledger(Bookie, Remaining) of
                ok -> ok;
                {error, Reason} -> erlang:error({fts2_reclaim_failed, Reason})
            end
        end,
        Bookies
    ).

fts2_build_tail_presence_cleanup(Bookie, #{index := Bucket}) ->
    Fold = fun
        (B, {<<"f2:p">>, SubKey}, _Value, Acc) when B =:= Bucket ->
            [{remove, Bucket, <<"f2:p">>, SubKey, <<>>} | Acc];
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{<<"f2:p">>, <<>>}, {<<"f2:p">>, <<255, 255, 255, 255, 255>>}}},
        {Fold, []},
        false,
        true,
        false
    ),
    Runner().

fts2_build_generation_id() ->
    <<Generation:64/unsigned-big, _/binary>> = crypto:hash(
        sha256,
        term_to_binary(
            {erlang:system_time(nanosecond), erlang:unique_integer([positive])},
            [deterministic]
        )
    ),
    Generation.

fts2_build_current_root(Bookie, Bucket) ->
    {Key, SubKey} = fts2_codec_root_key(),
    case leveled_bookie:book_sqn(Bookie, Bucket, {Key, SubKey}, ?HEAD_TAG) of
        not_found ->
            absent;
        {ok, SQN} ->
            case leveled_bookie:book_headonly(Bookie, Bucket, Key, SubKey) of
                {ok, Value} -> {SQN, fts2_codec_decode_root(Value)};
                not_found -> absent
            end
    end.

fts2_build_previous_generation(absent) -> undefined;
fts2_build_previous_generation({_SQN, Root}) -> maps:get(generation, Root).

fts2_build_publish_root(
    Bookie, Bucket, Previous, Root, TermBloom, TermBloomShards,
    BigramBloomShards,
    CommitSpecs, CommitConditions
) ->
    {Key, SubKey} = fts2_codec_root_key(),
    Condition =
        case Previous of
            absent -> {Bucket, Key, SubKey, absent};
            {SQN, _Root} -> {Bucket, Key, SubKey, {sqn, SQN}}
    end,
    RootValue = fts2_codec_encode_root(Root),
    BloomValue = <<1, (maps:get(generation, Root)):64/unsigned-big,
        TermBloom/binary>>,
    Spec = {add, Bucket, Key, SubKey, RootValue},
    StateSpec = {add, Bucket, <<"f2:state">>, <<"current">>,
        <<1, RootValue/binary>>},
    BloomSpec = {add, Bucket, <<"f2:bloom">>, <<"current">>,
        BloomValue},
    BloomShardSpecs = [
        {add, Bucket, <<"f2:bloom">>, <<"s", Shard:8>>,
            <<1, (maps:get(generation, Root)):64/unsigned-big,
                Bloom/binary>>}
     || {Shard, Bloom} <- TermBloomShards
    ],
    BigramBloomShardSpecs = [
        {add, Bucket, <<"f2:bloom">>, <<"g", Shard:8>>,
            <<1, (maps:get(generation, Root)):64/unsigned-big,
                Bloom/binary>>}
     || {Shard, Bloom} <- BigramBloomShards
    ],
    case
        leveled_bookie:book_casmput(
            Bookie,
            [Spec, StateSpec, BloomSpec | BloomShardSpecs] ++
                BigramBloomShardSpecs ++ CommitSpecs,
            [Condition | CommitConditions]
        )
    of
        ok ->
            fts2_build_after_write(Bookie, false);
        pause ->
            fts2_build_after_write(Bookie, true);
        {error, {precondition_failed, _}} ->
            erlang:error(fts2_generation_raced);
        {error, Reason} ->
            erlang:error({fts2_root_publish_failed, Reason})
    end.

fts2_build_epoch_conditions(Bookie, #{index := Bucket, shards := Shards}) ->
    [
        begin
            Key = <<Shard:16/unsigned-big>>,
            case leveled_bookie:book_sqn(
                Bookie, Bucket, {Key, <<"epoch">>}, ?HEAD_TAG
            ) of
                not_found -> {Bucket, Key, <<"epoch">>, absent};
                {ok, SQN} -> {Bucket, Key, <<"epoch">>, {sqn, SQN}}
            end
        end
     || Shard <- lists:seq(0, Shards - 1)
    ].

-ifdef(TEST).
fts2_build_read_deltas(Bookie, #{index := Bucket}) ->
    Fold = fun
        (B, {<<"f2:d">>, <<SourceId:64/unsigned-big>>}, _Value, Acc) when
            B =:= Bucket
        ->
            [SourceId | Acc];
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {
            {<<"f2:d">>, <<>>},
            {<<"f2:d">>, <<16#FFFFFFFFFFFFFFFF:64/unsigned-big>>}
        }},
        {Fold, []},
        false,
        true,
        false
    ),
    lists:filtermap(
        fun(SourceId) ->
            SubKey = <<SourceId:64/unsigned-big>>,
            case leveled_bookie:book_sqn(
                Bookie, Bucket, {<<"f2:d">>, SubKey}, ?HEAD_TAG
            ) of
                {ok, SQN} ->
                    case leveled_bookie:book_headonly(
                        Bookie, Bucket, <<"f2:d">>, SubKey
                    ) of
                        {ok, Value} ->
                            {true, {SourceId, SQN,
                                fts2_codec_decode(delta, Value)}};
                        not_found ->
                            false
                    end;
                not_found ->
                    false
            end
        end,
        lists:usort(Runner())
    ).
-endif.

fts2_build_read_delta_refs(Bookie, #{index := Bucket}) ->
    Fold = fun
        (B, {<<"f2:d">>, <<SourceId:64/unsigned-big>>}, _Value, Acc) when
            B =:= Bucket
        ->
            [SourceId | Acc];
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {
            {<<"f2:d">>, <<>>},
            {<<"f2:d">>, <<16#FFFFFFFFFFFFFFFF:64/unsigned-big>>}
        }},
        {Fold, []},
        false,
        true,
        false
    ),
    lists:filtermap(
        fun(SourceId) ->
            case leveled_bookie:book_sqn(
                Bookie,
                Bucket,
                {<<"f2:d">>, <<SourceId:64/unsigned-big>>},
                ?HEAD_TAG
            ) of
                {ok, SQN} -> {true, {SourceId, SQN}};
                not_found -> false
            end
        end,
        lists:usort(Runner())
    ).

-ifdef(TEST).
fts2_build_apply_deltas(Existing, Deltas) ->
    lists:foldl(
        fun({_SourceId, _SQN, Delta}, Acc) ->
            WithoutRetired = maps:without(
                maps:get(retired_ids, Delta, []), Acc
            ),
            SourceId = maps:get(source_id, Delta),
            case maps:get(status, Delta) of
                live -> WithoutRetired#{SourceId => Delta};
                remove -> maps:remove(SourceId, WithoutRetired)
            end
        end,
        Existing,
        Deltas
    ).
-endif.

fts2_build_remove_deltas(_Bookie, _Bucket, []) ->
    ok;
fts2_build_remove_deltas(Bookie, Bucket, Deltas) ->
    Count = erlang:min(128, length(Deltas)),
    {Batch, Rest} = lists:split(Count, Deltas),
    Specs = [
        {remove, Bucket, <<"f2:d">>, <<SourceId:64/unsigned-big>>, <<>>}
     || Delta <- Batch,
        SourceId <- [element(1, Delta)]
    ],
    Conditions = [
        {Bucket, <<"f2:d">>, <<SourceId:64/unsigned-big>>, {sqn, SQN}}
     || Delta <- Batch,
        SourceId <- [element(1, Delta)],
        SQN <- [element(2, Delta)]
    ],
    case leveled_bookie:book_casmput(Bookie, Specs, Conditions) of
        ok ->
            fts2_build_after_write(Bookie, false),
            fts2_build_remove_deltas(Bookie, Bucket, Rest);
        pause ->
            fts2_build_after_write(Bookie, true),
            fts2_build_remove_deltas(Bookie, Bucket, Rest);
        {error, {precondition_failed, _}} ->
            fts2_build_remove_deltas(Bookie, Bucket, Rest);
        {error, Reason} ->
            erlang:error({fts2_delta_cleanup_failed, Reason})
    end.

-ifdef(TEST).
fts2_build_source_lengths(TokenDocs) ->
    maps:fold(
        fun(_Token, Docs, Acc) ->
            maps:fold(
                fun(SourceId, {_StoredId, Length, _Posting}, Inner) ->
                    Inner#{SourceId => Length}
                end,
                Acc,
                Docs
            )
        end,
        #{},
        TokenDocs
    ).

fts2_build_group_sources(Schema, CandidateRecords, HitRecords, SourceLengths) ->
    GroupFields = maps:get(candidate_group_fields, Schema, []),
    VersionField = maps:get(candidate_version_field, Schema, undefined),
    Grouped0 = maps:fold(
        fun
            (_SourceId, deleted, Acc) ->
                Acc;
            (SourceId, {DocKey, DocVersion, Candidate}, Acc) ->
                case maps:find(SourceId, HitRecords) of
                    {ok, {DocKey, DocVersion, HitRecord}} ->
                        GroupKey = fts2_build_group_key(GroupFields, SourceId, Candidate),
                        Version = fts2_build_group_version(VersionField, Candidate),
                        Row = #{
                            source_id => SourceId,
                            doc_key => DocKey,
                            doc_version => DocVersion,
                            candidate_record => Candidate,
                            hit_record => HitRecord,
                            doc_length => maps:get(SourceId, SourceLengths, 0)
                        },
                        fts2_build_keep_group_version(GroupKey, Version, Row, Acc);
                    _ ->
                        Acc
                end
        end,
        #{},
        CandidateRecords
    ),
    OrderedGroups = lists:sort(
        fun({KeyA, _}, {KeyB, _}) ->
            KeyA =< KeyB
        end,
        maps:to_list(Grouped0)
    ),
    {SourceMap, Groups, _NextGroup, _NextDenseId} = lists:foldl(
        fun(
            {GroupKey, {_Version, Rows0}},
            {Sources, Acc, GroupId, NextDenseId0}
        ) ->
            Rows = lists:sort(
                fun(A, B) ->
                    {maps:get(doc_key, A), maps:get(source_id, A)} =<
                        {maps:get(doc_key, B), maps:get(source_id, B)}
                end,
                Rows0
            ),
            GroupLength = lists:sum([
                maps:get(doc_length, Row, 0) || Row <- Rows
            ]),
            true = length(Rows) =< ?CHUNKS_PER_GROUP,
            {ChunkRows, NextSources, _Local, NextDenseId} = lists:foldl(
                fun(Row, {ChunkAcc, SourceAcc, Local, DenseId}) ->
                    ChunkId = (GroupId bsl ?CHUNK_BITS) bor Local,
                    Chunk = Row#{
                        chunk_id => ChunkId,
                        group_id => GroupId,
                        dense_id => DenseId,
                        group_length => GroupLength
                    },
                    {
                        [Chunk | ChunkAcc],
                        SourceAcc#{maps:get(source_id, Row) => Chunk},
                        Local + 1,
                        DenseId + 1
                    }
                end,
                {[], Sources, 0, NextDenseId0},
                Rows
            ),
            Group = #{
                group_id => GroupId,
                group_key => GroupKey,
                chunks => lists:reverse(ChunkRows)
            },
            {NextSources, [Group | Acc], GroupId + 1, NextDenseId}
        end,
        {#{}, [], 0, 0},
        OrderedGroups
    ),
    {SourceMap, lists:reverse(Groups)}.
-endif.

fts2_build_group_key([], SourceId, _Candidate) ->
    {source, SourceId};
fts2_build_group_key(Fields, _SourceId, Candidate) ->
    {group, [maps:get(Field, Candidate, undefined) || Field <- Fields]}.

fts2_build_group_version(undefined, _Candidate) -> 0;
fts2_build_group_version(Field, Candidate) -> maps:get(Field, Candidate, 0).

-ifdef(TEST).
fts2_build_keep_group_version(Key, Version, Row, Acc) ->
    case maps:find(Key, Acc) of
        error ->
            Acc#{Key => {Version, [Row]}};
        {ok, {Older, _Rows}} when Version > Older ->
            Acc#{Key => {Version, [Row]}};
        {ok, {Version, Rows}} ->
            Acc#{Key => {Version, [Row | Rows]}};
        {ok, {_Newer, _Rows}} ->
            Acc
    end.
-endif.

-ifdef(TEST).
fts2_build_build_term_rows(TokenDocs, SourceMap) ->
    maps:fold(
        fun(Token, Docs, {TermAcc, ChunkAcc}) ->
            maps:fold(
                fun(SourceId, {_StoredId, _Length, Posting}, {Terms, Chunks}) ->
                    case maps:find(SourceId, SourceMap) of
                        error ->
                            {Terms, Chunks};
                        {ok, Chunk} ->
                            maps:fold(
                                fun(Column, Tokens, {TA, CA}) ->
                                    case maps:find(Token, Tokens) of
                                        error ->
                                            {TA, CA};
                                        {ok, Entry} ->
                                            fts2_build_add_term_entry(
                                                Column,
                                                Token,
                                                Entry,
                                                Chunk,
                                                TA,
                                                CA
                                            )
                                    end
                                end,
                                {Terms, Chunks},
                                Posting
                            )
                    end
                end,
                {TermAcc, ChunkAcc},
                Docs
            )
        end,
        {#{}, #{}},
        TokenDocs
    ).
-endif.

%% Compact immutable vocabulary proof. It is fetched in the same Bookie call
%% as the active state row, so an impossible exact term/phrase avoids all
%% generation-qualified header reads. False positives only select the normal
%% path; the bitset can never create a false negative.
fts2_term_bloom_shard_add(_Column, _Token, 4, Words) ->
    Words;
fts2_term_bloom_shard_add(Column, Token, Salt, Words) ->
    BitIndex = erlang:phash2(
        {Salt, Column, Token}, ?TERM_BLOOM_SHARD_BITS
    ),
    Word = BitIndex bsr 6,
    Mask = 1 bsl (BitIndex band 63),
    fts2_term_bloom_shard_add(
        Column, Token, Salt + 1,
        Words#{Word => maps:get(Word, Words, 0) bor Mask}
    ).

fts2_term_bloom_add(_Column, _Token, 4, Words) ->
    Words;
fts2_term_bloom_add(Column, Token, Salt, Words) ->
    BitIndex = erlang:phash2({Salt, Column, Token}, ?TERM_BLOOM_BITS),
    Word = BitIndex bsr 6,
    Mask = 1 bsl (BitIndex band 63),
    fts2_term_bloom_add(
        Column, Token, Salt + 1,
        Words#{Word => maps:get(Word, Words, 0) bor Mask}
    ).

-ifdef(TEST).
fts2_build_add_term_entry(Column, Token, Entry, Chunk, Terms, Chunks) ->
    ChunkId = maps:get(chunk_id, Chunk),
    GroupId = maps:get(group_id, Chunk),
    SourceId = maps:get(source_id, Chunk),
    Length = maps:get(doc_length, Chunk),
    Tf = maps:get(count, Entry),
    DenseId = maps:get(dense_id, Chunk),
    GroupLength = maps:get(group_length, Chunk),
    Positions = maps:get(positions, Entry),
    PlaneEntry = {ChunkId, GroupId, SourceId, Length, Tf, DenseId},
    Key = {Column, Token},
    Row0 = maps:get(Key, Terms, #{entries => [], positions => []}),
    Row = Row0#{
        entries := [PlaneEntry | maps:get(entries, Row0)],
        group_entries => [
            {ChunkId, GroupId, SourceId, GroupLength, Tf, GroupId}
            | maps:get(group_entries, Row0, [])
        ],
        positions := [{ChunkId, Positions} | maps:get(positions, Row0)]
    },
    ChunkTerms = maps:get({Column, ChunkId}, Chunks, []),
    {
        Terms#{Key => Row},
        Chunks#{
            {Column, ChunkId} => [{Token, Positions, PlaneEntry} | ChunkTerms]
        }
    }.

fts2_build_build_bigram_rows(ByChunk, FrequentTerms) ->
    {BigramRows, BloomShardWords} = maps:fold(
        fun({Column, _ChunkId}, Terms, {RowsAcc, BloomAcc}) ->
            PositionTokens = lists:foldl(
                fun({Token, Positions, PlaneEntry}, PosAcc) ->
                    lists:foldl(
                        fun(Pos, PA) -> PA#{Pos => {Token, PlaneEntry}} end,
                        PosAcc,
                        Positions
                    )
                end,
                #{},
                Terms
            ),
            lists:foldl(
                fun(Pos, {BigramAcc, BigramBloomAcc}) ->
                    case
                        {
                            maps:find(Pos, PositionTokens),
                            maps:find(Pos + 1, PositionTokens)
                        }
                    of
                        {{ok, {First, PlaneEntry}}, {ok, {Second, SecondEntry}}} ->
                            NextBigramAcc = case
                                maps:is_key({Column, First}, FrequentTerms) andalso
                                    maps:is_key({Column, Second}, FrequentTerms)
                            of
                                true ->
                                    fts2_build_add_bigram(
                                        Column,
                                        First,
                                        Second,
                                        Pos,
                                        PlaneEntry,
                                        SecondEntry,
                                        BigramAcc
                                    );
                                false ->
                                    BigramAcc
                            end,
                            {
                                NextBigramAcc,
                                fts2_build_bigram_bloom_add(
                                    Column, First, Second, BigramBloomAcc
                                )
                            };
                        _ ->
                            {BigramAcc, BigramBloomAcc}
                    end
                end,
                {RowsAcc, BloomAcc},
                lists:sort(maps:keys(PositionTokens))
            )
        end,
        {#{}, #{}},
        ByChunk
    ),
    {
        BigramRows,
        fts2_build_encode_bigram_bloom_shards(BloomShardWords)
    }.
-endif.

fts2_build_bigram_bloom_add(Column, First, Second, ShardWords) ->
    Shard = fts2_bigram_bloom_shard(Column, First, Second),
    Words0 = maps:get(Shard, ShardWords, #{}),
    ShardWords#{Shard => fts2_bigram_bloom_shard_add(
        Column, First, Second, 0, Words0
    )}.

fts2_bigram_bloom_shard_add(_Column, _First, _Second, 4, Words) ->
    Words;
fts2_bigram_bloom_shard_add(Column, First, Second, Salt, Words) ->
    BitIndex = erlang:phash2(
        {Salt, Column, First, Second}, ?BIGRAM_BLOOM_SHARD_BITS
    ),
    Word = BitIndex bsr 6,
    Mask = 1 bsl (BitIndex band 63),
    fts2_bigram_bloom_shard_add(
        Column,
        First,
        Second,
        Salt + 1,
        Words#{Word => maps:get(Word, Words, 0) bor Mask}
    ).

-ifdef(TEST).
fts2_build_encode_bigram_bloom_shards(ShardWords) ->
    fts2_build_encode_bloom_shard_words(
        ShardWords, ?BIGRAM_BLOOM_SHARDS, ?BIGRAM_BLOOM_SHARD_WORDS
    ).
-endif.

-ifdef(TEST).
fts2_build_add_bigram(
    Column, First, Second, Position, PlaneEntry, SecondPlaneEntry, Acc
) ->
    Key = {Column, First, Second},
    Row0 = maps:get(Key, Acc, #{}),
    ChunkId = element(1, PlaneEntry),
    case maps:find(ChunkId, Row0) of
        error ->
            Acc#{Key => Row0#{ChunkId => {
                PlaneEntry, element(5, SecondPlaneEntry), [Position]
            }}};
        {ok, {Existing, SecondTf, Positions}} ->
            Acc#{Key => Row0#{ChunkId => {
                Existing, SecondTf, [Position | Positions]
            }}}
    end.
-endif.

fts2_build_term_row_specs(
    Bucket, Generation, Column, Token, Row, GroupCount, TotalLength
) ->
    Entries = lists:sort(maps:get(entries, Row)),
    GroupEntries = fts2_build_group_entries(
        maps:get(group_entries, Row)
    ),
    Positions = lists:sort(maps:get(positions, Row)),
    PositionMap = maps:from_list(Positions),
    Header = fts2_build_header(
        Entries, GroupEntries, GroupCount, TotalLength
    ),
    Anchor = [
        Entry
     || Entry <- Entries,
        lists:member(0, maps:get(element(1, Entry), PositionMap, []))
    ],
    {Key, _} = fts2_codec_term_key(Generation, Column, Token),
    BooleanKey = fts2_codec_term_plane_key(Key, boolean),
    PositionKey = fts2_codec_term_plane_key(Key, positions),
    [
        {add, Bucket, Key, <<"h">>, fts2_codec_encode_header(Header)},
        {add, Bucket, BooleanKey, <<"b">>, fts2_codec_encode_plane(Entries)},
        {add, Bucket, BooleanKey, <<"g">>,
            fts2_codec_encode_plane(GroupEntries)},
        {add, Bucket, PositionKey, <<"p">>,
            fts2_codec_encode_positions(maps:to_list(PositionMap))}
    ] ++ case Anchor of
        [] -> [];
        _ -> [{add, Bucket, Key, <<"a">>,
            fts2_codec_encode_plane(Anchor)}]
    end.

fts2_build_parallel(_Role, [], _Concurrency, _Fun) ->
    0;
fts2_build_parallel(Role, Items, Concurrency0, Fun) ->
    Concurrency =
        case Concurrency0 of
            N when is_integer(N), N > 0 ->
                erlang:min(
                    ?FTS2_MAX_BUILD_CONCURRENCY, erlang:min(N, length(Items))
                );
            _ ->
                1
        end,
    fts2_build_parallel_with_limit(
        Role, Items, Concurrency, Fun, fts2_build_max_heap_words()
    ).

-ifdef(TEST).
fts2_build_parallel_for_test(Items, Concurrency0, Fun, MaxHeapWords) ->
    fts2_build_parallel_for_test(
        inner_worker, Items, Concurrency0, Fun, MaxHeapWords
    ).

fts2_build_parallel_for_test(
    Role, Items, Concurrency0, Fun, MaxHeapWords
) ->
    Concurrency =
        case Concurrency0 of
            N when is_integer(N), N > 0 ->
                erlang:min(N, erlang:max(length(Items), 1));
            _ ->
                1
        end,
    fts2_build_parallel_with_limit(
        Role, Items, Concurrency, Fun, MaxHeapWords
    ).

fts2_build_parallel_for_test(
    Role, Items, Concurrency0, Fun, MaxHeapWords, Observer
) ->
    PreviousObserver = put('$fts2_build_report_observer', Observer),
    try
        fts2_build_parallel_for_test(
            Role, Items, Concurrency0, Fun, MaxHeapWords
        )
    after
        case PreviousObserver of
            undefined -> erase('$fts2_build_report_observer');
            _ -> put('$fts2_build_report_observer', PreviousObserver)
        end
    end.

fts2_outer_bound_for_test(Fun, MaxHeapWords, Observer) ->
    PreviousObserver = put('$fts2_build_report_observer', Observer),
    try
        {Pid, Monitor} = spawn_opt(
            fun() ->
                Result =
                    try Fun() of
                        Value -> {fts2_outer_ok, Value}
                    catch
                        Class:Reason:Stacktrace ->
                            {fts2_outer_error, Class, Reason, Stacktrace}
                    end,
                exit(Result)
            end,
            fts2_build_outer_options(MaxHeapWords)
        ),
        case Observer of
            Sampler when is_pid(Sampler) ->
                Sampler ! {fts2_build_outer, Pid};
            _ ->
                ok
        end,
        receive
            {'DOWN', Monitor, process, Pid, {fts2_outer_ok, Value}} ->
                Value;
            {'DOWN', Monitor, process, Pid,
                {fts2_outer_error, Class, Reason, Stacktrace}} ->
                erlang:raise(Class, Reason, Stacktrace);
            {'DOWN', Monitor, process, Pid, Reason} ->
                fts2_build_report_bound(outer_build, Reason),
                erlang:error({fts2_parallel_worker_lost, outer_build, Reason})
        end
    after
        case PreviousObserver of
            undefined -> erase('$fts2_build_report_observer');
            _ -> put('$fts2_build_report_observer', PreviousObserver)
        end
    end.

fts2_build_outer_options(infinity) ->
    [monitor];
fts2_build_outer_options(MaxHeapWords) ->
    [
        monitor,
        {max_heap_size, #{
            size => MaxHeapWords,
            kill => true,
            %% Mirrors the production outer bound owned by the supervised
            %% build task, including its runtime report.
            error_logger => true,
            include_shared_binaries => true
        }}
    ].
-endif.

fts2_build_parallel_with_limit(
    _Role, [], _Concurrency, _Fun, _MaxHeapWords
) ->
    0;
fts2_build_parallel_with_limit(
    Role, Items, Concurrency, Fun, MaxHeapWords
) ->
    PreviousTrapExit = process_flag(trap_exit, true),
    try
        {Pending, Monitors} = fts2_build_start_parallel(
            Role, Items, Concurrency, Fun, MaxHeapWords, #{}
        ),
        fts2_build_collect_parallel(
            {Role, Pending, Monitors, Fun, MaxHeapWords}, 0
        )
    after
        process_flag(trap_exit, PreviousTrapExit)
    end.

fts2_build_start_parallel(
    _Role, Items, 0, _Fun, _MaxHeapWords, Monitors
) ->
    {Items, Monitors};
fts2_build_start_parallel(
    _Role, [], _Slots, _Fun, _MaxHeapWords, Monitors
) ->
    {[], Monitors};
fts2_build_start_parallel(
    Role, [Item | Rest], Slots, Fun, MaxHeapWords, Monitors
) ->
    Context = fts2_build_worker_context(),
    {Pid, Monitor} = spawn_opt(
        fun() ->
            fts2_build_apply_worker_context(Context),
            Result =
                try Fun(Item) of
                    Count -> {fts2_parallel_ok, Count}
                catch
                    Class:Reason:Stacktrace ->
                        {fts2_parallel_error, Class, Reason, Stacktrace}
                end,
            exit(Result)
        end,
        [link | fts2_build_worker_options(MaxHeapWords)]
    ),
    fts2_build_observe_worker(Role, Pid),
    fts2_build_start_parallel(
        Role,
        Rest,
        Slots - 1,
        Fun,
        MaxHeapWords,
        Monitors#{Monitor => {Pid, Role}}
    ).

fts2_build_worker_options(infinity) ->
    [monitor];
fts2_build_worker_options(MaxHeapWords) ->
    [
        monitor,
        {max_heap_size, #{
            size => MaxHeapWords,
            kill => true,
            %% The runtime report carries the heap sizes and the stack at the
            %% breach. That evidence localised the live build defect and is
            %% kept. It does not name a role, so the coordinator adds exactly
            %% one role-attributed report after the DOWN.
            error_logger => true,
            include_shared_binaries => false
        }}
    ].

-ifdef(TEST).
fts2_build_max_heap_words() ->
    case get('$fts2_build_max_heap_words') of
        undefined -> ?FTS2_MAX_BUILD_HEAP_WORDS;
        MaxHeapWords -> MaxHeapWords
    end.

%% The gate builds the same corpus at the superseded shard counts to show the
%% coarse partition breaching a bound the fine partition clears. Shard counts
%% are read inside build workers too, so the override travels with the worker.
fts2_build_term_shards() ->
    case get('$fts2_build_shards') of
        undefined -> ?FTS2_BUILD_TERM_SHARDS;
        Shards -> Shards
    end.

fts2_build_bigram_shards() ->
    case get('$fts2_build_shards') of
        undefined -> ?FTS2_BUILD_BIGRAM_SHARDS;
        Shards -> Shards
    end.

fts2_build_worker_context() ->
    {get('$fts2_build_shards'), get('$fts2_build_pressure')}.

fts2_build_apply_worker_context({Shards, Pressure}) ->
    case Shards of
        undefined -> ok;
        _ -> _ = put('$fts2_build_shards', Shards)
    end,
    case Pressure of
        undefined -> ok;
        _ -> _ = put('$fts2_build_pressure', Pressure)
    end,
    ok.

fts2_build_observe_worker(Role, Pid) ->
    case get('$fts2_build_observer') of
        Observer when is_pid(Observer) ->
            Observer ! {fts2_build_worker, Role, Pid},
            ok;
        _ ->
            ok
    end.

fts2_build_report_bound(Role, Reason) ->
    case get('$fts2_build_report_observer') of
        Observer when is_pid(Observer) ->
            Observer ! {fts2_bound_report, Role, Reason};
        _ ->
            ok
    end,
    error_logger:error_report([
        {fts2_build_role, Role},
        {reason, Reason},
        {outcome, worker_lost}
    ]).
-else.
fts2_build_max_heap_words() ->
    ?FTS2_MAX_BUILD_HEAP_WORDS.

fts2_build_term_shards() ->
    ?FTS2_BUILD_TERM_SHARDS.

fts2_build_bigram_shards() ->
    ?FTS2_BUILD_BIGRAM_SHARDS.

fts2_build_worker_context() ->
    get('$fts2_build_pressure').

fts2_build_apply_worker_context(undefined) ->
    ok;
fts2_build_apply_worker_context(Pressure) ->
    _ = put('$fts2_build_pressure', Pressure),
    ok.

fts2_build_observe_worker(_Role, _Pid) ->
    ok.

%% One report per lost worker, naming the role. A worker killed by its own
%% max_heap_size is also reported by the runtime with heap sizes and a stack.
%% A worker killed by a propagated signal has no runtime report, so this is
%% the only record of it; the reason is therefore stated, not assumed.
fts2_build_report_bound(Role, Reason) ->
    error_logger:error_report([
        {fts2_build_role, Role},
        {reason, Reason},
        {outcome, worker_lost}
    ]).
-endif.

fts2_build_chunks([], _ChunkSize, Acc) ->
    lists:reverse(Acc);
fts2_build_chunks(Items, ChunkSize, Acc) ->
    Count = erlang:min(ChunkSize, length(Items)),
    {Chunk, Rest} = lists:split(Count, Items),
    fts2_build_chunks(Rest, ChunkSize, [Chunk | Acc]).

fts2_build_collect_parallel(
    {_Role, [], Monitors, _Fun, _MaxHeapWords}, Count
) when
    map_size(Monitors) =:= 0
->
    Count;
fts2_build_collect_parallel(
    {Role, Pending, Monitors, Fun, MaxHeapWords}, Count
) ->
    receive
        {'DOWN', Monitor, process, _Pid, {fts2_parallel_ok, Rows}} when
            is_map_key(Monitor, Monitors)
        ->
            {Pid, Role} = maps:get(Monitor, Monitors),
            fts2_build_drain_parallel_exit(Pid),
            {NextPending, NextMonitors} = fts2_build_start_parallel(
                Role,
                Pending,
                1,
                Fun,
                MaxHeapWords,
                maps:remove(Monitor, Monitors)
            ),
            fts2_build_collect_parallel(
                {Role, NextPending, NextMonitors, Fun, MaxHeapWords},
                Count + Rows
            );
        {'DOWN', Monitor, process, _Pid,
            {fts2_parallel_error, Class, Reason, Stacktrace}} when
            is_map_key(Monitor, Monitors)
        ->
            {Pid, Role} = maps:get(Monitor, Monitors),
            fts2_build_drain_parallel_exit(Pid),
            fts2_build_drain_parallel(maps:remove(Monitor, Monitors)),
            erlang:raise(Class, Reason, Stacktrace);
        {'DOWN', Monitor, process, _Pid, Reason} when
            is_map_key(Monitor, Monitors)
        ->
            {Pid, WorkerRole} = maps:get(Monitor, Monitors),
            fts2_build_drain_parallel_exit(Pid),
            %% A worker died without reporting a result tuple — e.g. killed
            %% by its max_heap_size bound. Waiting would deadlock the build.
            fts2_build_drain_parallel(maps:remove(Monitor, Monitors)),
            fts2_build_report_bound(WorkerRole, Reason),
            erlang:error({fts2_parallel_worker_lost, WorkerRole, Reason})
    end.

fts2_build_drain_parallel(Monitors) when map_size(Monitors) =:= 0 ->
    ok;
fts2_build_drain_parallel(Monitors) ->
    receive
        {'DOWN', Monitor, process, Pid, _Reason} when
            is_map_key(Monitor, Monitors)
        ->
            fts2_build_drain_parallel_exit(Pid),
            fts2_build_drain_parallel(maps:remove(Monitor, Monitors))
    end.

fts2_build_drain_parallel_exit(Pid) ->
    receive
        {'EXIT', Pid, _Reason} -> ok
    after 1000 ->
        erlang:error({fts2_parallel_exit_signal_missing, Pid})
    end.

fts2_build_bigram_row_specs(
    Bucket,
    Generation,
    Column,
    First,
    Second,
    ByChunk,
    FirstDf,
    SecondDf,
    ChunkCount,
    TotalLength
) ->
    Entries = lists:sort([
        {element(1, PlaneEntry), element(2, PlaneEntry),
            element(3, PlaneEntry), element(4, PlaneEntry),
            length(Positions), element(5, PlaneEntry), SecondTf}
     || {_ChunkId, {PlaneEntry, SecondTf, Positions}} <- maps:to_list(ByChunk)
    ]),
    PositionEntries = lists:sort([
        {ChunkId, lists:sort(Positions)}
     || {ChunkId, {_PlaneEntry, _SecondTf, Positions}} <- maps:to_list(ByChunk)
    ]),
    Avg = TotalLength / erlang:max(ChunkCount, 1),
    FirstIdf = fts2_build_bm25_idf(ChunkCount, FirstDf),
    SecondIdf = fts2_build_bm25_idf(ChunkCount, SecondDf),
    BestByGroup = lists:foldl(
        fun(Entry, Acc) ->
            GroupId = element(2, Entry),
            case maps:find(GroupId, Acc) of
                error -> Acc#{GroupId => Entry};
                {ok, Existing} ->
                    case fts2_build_bigram_score(Entry, Avg, FirstIdf, SecondIdf) >
                        fts2_build_bigram_score(Existing, Avg, FirstIdf, SecondIdf) of
                        true -> Acc#{GroupId => Entry};
                        false -> Acc
                    end
            end
        end,
        #{},
        Entries
    ),
    BigramScore = fun(Entry) ->
        fts2_build_bigram_score(Entry, Avg, FirstIdf, SecondIdf)
    end,
    Champions = fts2_build_champion_window(
        lists:sort(
            fun(A, B) ->
                {-BigramScore(A), element(3, A)} =<
                    {-BigramScore(B), element(3, B)}
            end,
            maps:values(BestByGroup)
        ),
        BigramScore
    ),
    {Key, _} = fts2_codec_bigram_key(
        Generation, Column, First, Second
    ),
    [
        {add, Bucket, Key, <<"x">>, fts2_codec_encode_bigram(
            #{
                group_df => map_size(BestByGroup),
                first_df => FirstDf,
                second_df => SecondDf,
                champions => Champions,
                entries => Entries,
                positions => PositionEntries
            }
        )}
    ].

fts2_build_bigram_score(Entry, Avg, FirstIdf, SecondIdf) ->
    Length = element(4, Entry),
    fts2_build_bm25_tf(element(6, Entry), Length, Avg) * FirstIdf +
        fts2_build_bm25_tf(element(7, Entry), Length, Avg) * SecondIdf.

fts2_build_bm25_idf(DocCount, Df) ->
    erlang:max(
        math:log((DocCount - Df + 0.5) / (Df + 0.5)), 1.0e-6
    ).

fts2_build_header(Entries, GroupEntries, GroupCount, TotalLength) ->
    Avg = case GroupCount of
        0 -> 0.0;
        _ -> TotalLength / GroupCount
    end,
    TermScore = fun(Entry) ->
        fts2_build_bm25_tf(element(5, Entry), element(4, Entry), Avg)
    end,
    Champions = fts2_build_champion_window(
        lists:sort(
            fun(A, B) ->
                {-TermScore(A), element(2, A), element(3, A)} =<
                    {-TermScore(B), element(2, B), element(3, B)}
            end,
            GroupEntries
        ),
        TermScore
    ),
    #{
        group_df => length(GroupEntries),
        chunk_df => length(Entries),
        collection_frequency => lists:sum([element(5, E) || E <- Entries]),
        chunk_bitmap => case length(Entries) >= 64 of
            true -> lists:foldl(
                fun(Entry, Bitmap) ->
                    Bitmap bor (1 bsl element(6, Entry))
                end,
                0,
                Entries
            );
            false -> undefined
        end,
        group_bitmap => case length(Entries) >= 64 of
            true -> lists:foldl(
                fun(Entry, Bitmap) ->
                    Bitmap bor (1 bsl element(2, Entry))
                end,
                0,
                Entries
            );
            false -> undefined
        end,
        champions => Champions
    }.

fts2_build_champion_window(Ordered, ScoreFun) ->
    {Head, Tail} = lists:split(erlang:min(?HEAD_WINDOW, length(Ordered)), Ordered),
    case {Head, Tail} of
        {[], _} -> [];
        {_, []} -> Head;
        _ ->
            BoundaryScore = ScoreFun(lists:last(Head)),
            Head ++ lists:takewhile(
                fun(Entry) -> ScoreFun(Entry) =:= BoundaryScore end,
                Tail
            )
    end.

%% The immutable group plane is document-grain: one deterministic identity
%% coordinate, full document length, and total term frequency per logical
%% group.  It is the candidate/scoring plane for grouped searches.  The chunk
%% plane remains unchanged for explicit ungrouped retrieval and positions.
fts2_build_group_entries(Entries) ->
    Groups = lists:foldl(
        fun(Entry, Acc) ->
            GroupId = element(2, Entry),
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => Entry};
                {ok, Existing} ->
                    Anchor = case {element(1, Entry), element(3, Entry)} <
                        {element(1, Existing), element(3, Existing)} of
                        true -> Entry;
                        false -> Existing
                    end,
                    Acc#{GroupId => setelement(
                        5, Anchor, element(5, Existing) + element(5, Entry)
                    )}
            end
        end,
        #{},
        Entries
    ),
    lists:sort(
        fun(A, B) -> element(2, A) =< element(2, B) end,
        maps:values(Groups)
    ).

fts2_build_bm25_tf(Tf, Length, Avg) ->
    Ratio = case Avg > 0.0 of
        true -> Length / Avg;
        false -> 1.0
    end,
    (Tf * 2.2) / (Tf + 1.2 * (0.25 + 0.75 * Ratio)).

fts2_build_identity_page_count(0, _Shift) ->
    0;
fts2_build_identity_page_count(GroupCount, Shift) ->
    ((GroupCount - 1) div (1 bsl Shift)) + 1.

fts2_build_write_slices(_Bookie, []) ->
    ok;
fts2_build_write_slices(Bookie, Specs) ->
    Count = erlang:min(?WRITE_SLICE, length(Specs)),
    {Batch, Rest} = lists:split(Count, Specs),
    case leveled_bookie:book_mput(Bookie, Batch) of
        ok ->
            fts2_build_after_write(Bookie, false),
            fts2_build_write_slices(Bookie, Rest);
        pause ->
            fts2_build_after_write(Bookie, true),
            fts2_build_write_slices(Bookie, Rest);
        {error, Reason} -> erlang:error({fts2_generation_write_failed, Reason})
    end.

fts2_build_identity_bookie(Bookie, Schema) ->
    maps:get(identity_bookie, Schema, Bookie).

fts2_build_cleanup_generation(Bookie, Schema, Generation) ->
    Bucket = maps:get(index, Schema),
    fts2_build_remove_workspace(Bookie, Schema, Generation),
    fts2_build_remove_generation_rows(Bookie, Bucket, term, Generation),
    fts2_build_remove_generation_rows(Bookie, Bucket, bigram, Generation),
    fts2_build_remove_generation_rows(
        fts2_build_identity_bookie(Bookie, Schema), Bucket, identity, Generation
    ).

fts2_build_remove_workspace(Bookie, #{index := Bucket}, Generation) ->
    fts2_build_remove_generation_rows(
        Bookie, Bucket, workspace, Generation
    ).

fts2_build_remove_generation_rows(Bookie, Bucket, Kind, Generation) ->
    {Start, Finish} = case Kind of
        term -> {<<"f2:b:">>, <<"f2:u">>};
        bigram -> {<<"f2:g:">>, <<"f2:h">>};
        workspace -> {<<"f2:w:">>, <<"f2:x">>};
        identity ->
            Key = fts2_codec_identity_key(Generation),
            {Key, Key}
    end,
    fts2_build_remove_generation_page(
        Bookie, Bucket, Kind, Generation, {Start, <<>>},
        {Finish, <<255, 255, 255, 255>>}
    ).

fts2_build_remove_generation_page(
    Bookie, Bucket, Kind, Generation, Start, Finish
) ->
    Fold = fun(B, {Key, SubKey} = FullKey, _Value, {Specs, _LastKey}) when
        B =:= Bucket
    ->
        NextSpecs = case fts2_build_generation_key(Kind, Key, Generation) of
            true -> [{remove, Bucket, Key, SubKey, <<>>} | Specs];
            false -> Specs
        end,
        {NextSpecs, FullKey};
        (_B, _Key, _Value, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {Start, Finish}},
        {Fold, {[], undefined}},
        false,
        true,
        false,
        false,
        ?WRITE_SLICE
    ),
    {RemainingCount, {Specs, LastKey}} = Runner(),
    fts2_build_remove_specs(Bookie, lists:reverse(Specs)),
    case {RemainingCount, LastKey} of
        {0, NextStart} when NextStart =/= undefined ->
            fts2_build_remove_generation_page(
                Bookie, Bucket, Kind, Generation, NextStart, Finish
            );
        _Exhausted ->
            ok
    end.

fts2_build_generation_key(term, <<"f2:t:", Generation:64/unsigned-big, _/binary>>,
    Generation) -> true;
fts2_build_generation_key(term, <<"f2:b:", Generation:64/unsigned-big, _/binary>>,
    Generation) -> true;
fts2_build_generation_key(term, <<"f2:p:", Generation:64/unsigned-big, _/binary>>,
    Generation) -> true;
fts2_build_generation_key(bigram, <<"f2:g:", Generation:64/unsigned-big, _/binary>>,
    Generation) -> true;
fts2_build_generation_key(workspace,
    <<"f2:w:", Generation:64/unsigned-big, _/binary>>, Generation) -> true;
fts2_build_generation_key(identity, <<"f2:i:", Generation:64/unsigned-big>>, Generation) ->
    true;
fts2_build_generation_key(_Kind, _Key, _Generation) -> false.

fts2_build_remove_specs(_Bookie, []) -> ok;
fts2_build_remove_specs(Bookie, Specs) ->
    Count = erlang:min(?WRITE_SLICE, length(Specs)),
    {Batch, Rest} = lists:split(Count, Specs),
    case leveled_bookie:book_mput(Bookie, Batch) of
        ok ->
            fts2_build_after_write(Bookie, false),
            fts2_build_remove_specs(Bookie, Rest);
        pause ->
            fts2_build_after_write(Bookie, true),
            fts2_build_remove_specs(Bookie, Rest);
        {error, Reason} -> erlang:error({fts2_generation_cleanup_failed, Reason})
    end.

%% Every committed slice consumes a permit shared by all build workers for
%% this bookie. The fixed barrier makes admission deterministic even though a
%% bookie's pause replies are intentionally jittered.
fts2_build_after_write(Bookie, ForceBarrier) ->
    Action = case get('$fts2_build_pressure') of
        Pressure when is_pid(Pressure) ->
            Ref = make_ref(),
            Pressure ! {write_slice, self(), Ref, Bookie, ForceBarrier},
            receive
                {write_slice, Ref, NextAction} -> NextAction
            after 5000 ->
                erlang:error(fts2_backpressure_coordinator_timeout)
            end;
        undefined when ForceBarrier ->
            barrier;
        undefined ->
            continue
    end,
    case Action of
        continue -> ok;
        barrier ->
            case leveled_bookie:book_reclaimledger(Bookie, 300000) of
                ok -> ok;
                {error, Reason} ->
                    erlang:error({fts2_backpressure_failed, Reason})
            end
    end.

fts2_build_pressure_loop(Counts) ->
    receive
        {write_slice, From, Ref, Bookie, true} ->
            From ! {write_slice, Ref, barrier},
            fts2_build_pressure_loop(Counts#{Bookie => 0});
        {write_slice, From, Ref, Bookie, false} ->
            Count = maps:get(Bookie, Counts, 0) + 1,
            case Count >= ?WRITE_BARRIER_SLICES of
                true ->
                    From ! {write_slice, Ref, barrier},
                    fts2_build_pressure_loop(Counts#{Bookie => 0});
                false ->
                    From ! {write_slice, Ref, continue},
                    fts2_build_pressure_loop(Counts#{Bookie => Count})
            end;
        stop ->
            ok
    end.

fts2_build_trim_journals(Bookie, Schema) ->
    case maps:get(trim_journal, Schema, false) of
        true ->
            Bookies = lists:usort([
                Bookie, fts2_build_identity_bookie(Bookie, Schema)
            ]),
            %% Trimming is only safe after the bookie's residual ledger cache
            %% has crossed the persisted-SQN barrier.  In particular, parallel
            %% generation writes commonly leave a final under-sized cache.
            fts2_build_reclaim_ledgers(Bookies, 300000),
            lists:foreach(
                fun leveled_bookie:book_trimjournal/1,
                Bookies
            );
        false ->
            ok
    end.


%% ===========================================================================
%% INTERNAL FTS-2 CODEC
%% ===========================================================================

%% Immutable FTS2 row formats and key construction.
%%
%% Every generation-qualified row is write-once.  The only mutable row is the
%% root manifest; readers fetch it once and thereafter address one immutable
%% generation.  Values use the upstream zstd module directly.

-define(ROOT_VERSION, 1).
-define(ROW_VERSION, 1).
-define(HEADER_VERSION, 4).
-define(BIGRAM_VERSION, 1).
-define(PLANE_VERSION, 1).
-define(POSITION_VERSION, 1).
-define(IDENTITY_PAGE_VERSION, 4).

fts2_codec_root_key() ->
    {<<"f2:root">>, <<"manifest">>}.

fts2_codec_term_key(Generation, Column, Token) ->
    {<<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>>, <<>>}.

fts2_codec_term_plane_key(<<"f2:t:", Rest/binary>>, boolean) ->
    <<"f2:b:", Rest/binary>>;
fts2_codec_term_plane_key(<<"f2:t:", Rest/binary>>, positions) ->
    <<"f2:p:", Rest/binary>>.

fts2_codec_term_range(Generation, Column, Prefix) ->
    Start = <<"f2:t:", Generation:64/unsigned-big, Column:8, Prefix/binary>>,
    {Start, <<Start/binary, 255>>}.

fts2_codec_bigram_key(Generation, Column, First, Second) ->
    {
        <<"f2:g:", Generation:64/unsigned-big, Column:8,
            (byte_size(First)):16/unsigned-big, First/binary, Second/binary>>,
        <<>>
    }.

fts2_codec_identity_key(Generation) ->
    <<"f2:i:", Generation:64/unsigned-big>>.

fts2_codec_identity_subkey(PageNo) ->
    <<PageNo:32/unsigned-big>>.

fts2_codec_encode(Type, Term) when is_atom(Type) ->
    Raw = term_to_binary(Term, [deterministic]),
    Compressed = iolist_to_binary(zstd:compress(Raw)),
    <<?ROW_VERSION:8, (fts2_codec_type_id(Type)):8, (byte_size(Raw)):32/unsigned-big,
        Compressed/binary>>.

fts2_codec_decode(
    Type,
    <<?ROW_VERSION:8, TypeId:8, RawBytes:32/unsigned-big, Compressed/binary>>
) ->
    case TypeId =:= fts2_codec_type_id(Type) of
        true ->
            _ = fts2_codec_row_wire_atoms(),
            try iolist_to_binary(zstd:decompress(Compressed)) of
                Raw when byte_size(Raw) =:= RawBytes ->
                    binary_to_term(Raw, [safe]);
                _ ->
                    erlang:error({invalid_fts2_row, Type})
            catch
                error:{zstd_error, _} ->
                    erlang:error({invalid_fts2_row, Type})
            end;
        false ->
            erlang:error({invalid_fts2_row, Type})
    end;
fts2_codec_decode(Type, _Bad) ->
    erlang:error({invalid_fts2_row, Type}).

%% Term headers are read on every query. Keep their fixed numeric metadata and
%% champion tuples directly pattern-matchable. New generations always keep the
%% complete Boolean and position planes in separate row-addressable keys; the
%% inline tail decoder below remains solely for pre-break generations.
fts2_codec_encode_header(Header) ->
    Champions = maps:get(champions, Header),
    ChampionPayload = iolist_to_binary([
        <<ChunkId:32/unsigned-big, GroupLength:32/unsigned-big,
            GroupTf:64/unsigned-big>>
     || {ChunkId, _GroupId, _SourceId, GroupLength, GroupTf, _DenseId} <-
            Champions
    ]),
    InlineEntries = case maps:find(entries, Header) of
        {ok, Entries} -> fts2_codec_encode_header_tail(
            fts2_codec_encode_plane(Entries)
        );
        error -> <<>>
    end,
    InlinePositions = case maps:find(positions, Header) of
        {ok, Positions} -> fts2_codec_encode_header_tail(
            fts2_codec_encode_positions(maps:to_list(Positions))
        );
        error -> <<>>
    end,
    ChunkBitmap = case maps:get(chunk_bitmap, Header, undefined) of
        undefined -> <<>>;
        ChunkBitmapValue -> binary:encode_unsigned(ChunkBitmapValue, little)
    end,
    GroupBitmap = case maps:get(group_bitmap, Header, undefined) of
        undefined -> <<>>;
        GroupBitmapValue -> binary:encode_unsigned(GroupBitmapValue, little)
    end,
    GroupDf = maps:get(group_df, Header),
    ChunkDf = maps:get(chunk_df, Header),
    CollectionFrequency = maps:get(collection_frequency, Header),
    true = GroupDf =< ?MAX_U32,
    true = ChunkDf =< ?MAX_U32,
    true = CollectionFrequency =< ?MAX_U64,
    <<?HEADER_VERSION:8, GroupDf:32/unsigned-big, ChunkDf:32/unsigned-big,
        CollectionFrequency:64/unsigned-big,
        (length(Champions)):32/unsigned-big,
        (byte_size(ChampionPayload)):32/unsigned-big,
        (byte_size(InlineEntries)):32/unsigned-big,
        (byte_size(InlinePositions)):32/unsigned-big,
        (byte_size(ChunkBitmap)):32/unsigned-big,
        (byte_size(GroupBitmap)):32/unsigned-big,
        ChampionPayload/binary, InlineEntries/binary, InlinePositions/binary,
        ChunkBitmap/binary, GroupBitmap/binary>>.

fts2_codec_decode_header(
    Binary
) ->
    Header0 = fts2_codec_decode_header_metadata(Binary),
    ChampionCount = maps:get(champion_count, Header0),
    ChampionPayload = maps:get(champions_packed, Header0),
    (maps:without([champion_count, champions_packed], Header0))#{
        champions => fts2_codec_decode_header_champions(
            ChampionCount, ChampionPayload, []
        )
    }.

%% Boolean, prefix, and positional paths do not consume the champion prefix.
%% Leave it as a binary so those paths do not allocate 256 five-tuples per
%% term merely to reach the packed posting tails.
fts2_codec_decode_header_metadata(
    <<?HEADER_VERSION:8, GroupDf:32/unsigned-big, ChunkDf:32/unsigned-big,
        CollectionFrequency:64/unsigned-big, ChampionCount:32/unsigned-big,
        ChampionBytes:32/unsigned-big,
        EntryBytes:32/unsigned-big, PositionBytes:32/unsigned-big,
        BitmapBytes:32/unsigned-big,
        GroupBitmapBytes:32/unsigned-big,
        Rest/binary>>
) when byte_size(Rest) =:=
    ChampionBytes + EntryBytes + PositionBytes + BitmapBytes + GroupBitmapBytes
->
    <<ChampionPayload:ChampionBytes/binary, EntryPayload:EntryBytes/binary,
        PositionPayload:PositionBytes/binary,
        BitmapPayload:BitmapBytes/binary,
        GroupBitmapPayload:GroupBitmapBytes/binary>> = Rest,
    Header0 = #{
        group_df => GroupDf,
        chunk_df => ChunkDf,
        collection_frequency => CollectionFrequency,
        champion_count => ChampionCount,
        champions_packed => ChampionPayload
    },
    Header1 = case BitmapPayload of
        <<>> -> Header0;
        _ -> Header0#{
            chunk_bitmap => binary:decode_unsigned(BitmapPayload, little)
        }
    end,
    Header2 = case GroupBitmapPayload of
        <<>> -> Header1;
        _ -> Header1#{
            group_bitmap => binary:decode_unsigned(GroupBitmapPayload, little)
        }
    end,
    Header3 = case EntryPayload of
        <<>> -> Header2;
        _ -> Header2#{entries_packed => EntryPayload}
    end,
    case PositionPayload of
        <<>> -> Header3;
        _ -> Header3#{positions_packed => PositionPayload}
    end;
fts2_codec_decode_header_metadata(Bad) ->
    erlang:error({invalid_fts2_header, Bad}).

fts2_codec_decode_header_champions(0, <<>>, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_header_champions(
    Count,
    <<ChunkId:32/unsigned-big, GroupLength:32/unsigned-big,
        GroupTf:64/unsigned-big, Rest/binary>>,
    Acc
) when Count > 0 ->
    GroupId = ChunkId bsr ?CHUNK_BITS,
    fts2_codec_decode_header_champions(
        Count - 1,
        Rest,
        [{ChunkId, GroupId, 0, GroupLength, GroupTf, GroupTf} | Acc]
    );
fts2_codec_decode_header_champions(_Count, Bad, _Acc) ->
    erlang:error({invalid_fts2_header_champions, Bad}).

fts2_codec_encode_header_tail(Raw) ->
    Compressed = iolist_to_binary(zstd:compress(Raw)),
    case byte_size(Compressed) + 5 < byte_size(Raw) + 1 of
        true -> <<1, (byte_size(Raw)):32/unsigned-big, Compressed/binary>>;
        false -> <<0, Raw/binary>>
    end.

fts2_codec_decode_header_tail(<<0, Raw/binary>>) ->
    Raw;
fts2_codec_decode_header_tail(
    <<1, RawBytes:32/unsigned-big, Compressed/binary>>
) ->
    Raw = iolist_to_binary(zstd:decompress(Compressed)),
    case byte_size(Raw) of
        RawBytes -> Raw;
        _ -> erlang:error(invalid_fts2_header_tail)
    end;
fts2_codec_decode_header_tail(Bad) ->
    erlang:error({invalid_fts2_header_tail, Bad}).


fts2_codec_header_has_entries(Header) ->
    maps:is_key(entries_packed, Header).

fts2_codec_header_entries(Header) ->
    fts2_codec_decode_plane(
        fts2_codec_decode_header_tail(maps:get(entries_packed, Header))
    ).

fts2_codec_header_entry_plane(Header) ->
    fts2_codec_plane_payload(
        fts2_codec_decode_header_tail(maps:get(entries_packed, Header))
    ).

fts2_codec_plane_payload(<<?PLANE_VERSION:8, Count:32/unsigned-big, Payload/binary>>) when
    byte_size(Payload) =:= Count * 28
->
    Payload;
fts2_codec_plane_payload(Bad) ->
    erlang:error({invalid_fts2_plane, Bad}).

fts2_codec_header_has_positions(Header) ->
    maps:is_key(positions_packed, Header).

fts2_codec_header_positions(Header) ->
    fts2_codec_decode_positions(
        fts2_codec_decode_header_tail(maps:get(positions_packed, Header))
    ).

fts2_codec_header_positions(Header, WantedChunkIds) ->
    <<?POSITION_VERSION:8, Count:32/unsigned-big, Payload/binary>> =
        fts2_codec_decode_header_tail(maps:get(positions_packed, Header)),
    fts2_codec_decode_selected_positions(
        Count, Payload, maps:from_keys(WantedChunkIds, true), #{}
    ).

%% Payload selection for generations whose position rows are not chunk
%% ordered: the wanted chunks keep their encoded payload so the phrase and
%% NEAR verifiers can stream them, instead of becoming position lists.
fts2_codec_select_position_payloads(
    <<?POSITION_VERSION:8, Count:32/unsigned-big, Payload/binary>>,
    WantedChunkIds
) ->
    fts2_codec_select_position_payloads(
        Count, Payload, maps:from_keys(WantedChunkIds, true), #{}
    );
fts2_codec_select_position_payloads(Bad, _WantedChunkIds) ->
    erlang:error({invalid_fts2_positions, Bad}).

fts2_codec_select_position_payloads(0, <<>>, _Wanted, Acc) ->
    Acc;
fts2_codec_select_position_payloads(
    Count,
    <<ChunkId:32/unsigned-big, PositionCount:32/unsigned-big,
        Bytes:32/unsigned-big, Encoded:Bytes/binary, Rest/binary>>,
    Wanted,
    Acc
) when Count > 0 ->
    NextAcc = case maps:is_key(ChunkId, Wanted) of
        true -> Acc#{ChunkId => {PositionCount, Encoded}};
        false -> Acc
    end,
    fts2_codec_select_position_payloads(Count - 1, Rest, Wanted, NextAcc);
fts2_codec_select_position_payloads(_Count, Bad, _Wanted, _Acc) ->
    erlang:error({invalid_fts2_positions, Bad}).

fts2_codec_decode_selected_positions(0, <<>>, _Wanted, Acc) ->
    Acc;
fts2_codec_decode_selected_positions(
    Count,
    <<ChunkId:32/unsigned-big, PositionCount:32/unsigned-big,
        Bytes:32/unsigned-big, Encoded:Bytes/binary, Rest/binary>>,
    Wanted,
    Acc
) when Count > 0 ->
    NextAcc = case maps:is_key(ChunkId, Wanted) of
        true -> Acc#{ChunkId => fts2_codec_decode_position_list(
            PositionCount, Encoded, 0, []
        )};
        false -> Acc
    end,
    fts2_codec_decode_selected_positions(
        Count - 1, Rest, Wanted, NextAcc
    );
fts2_codec_decode_selected_positions(_Count, Bad, _Wanted, _Acc) ->
    erlang:error({invalid_fts2_positions, Bad}).

fts2_codec_encode_bigram(Bundle) ->
    Champions = maps:get(champions, Bundle),
    ChampionPayload = iolist_to_binary([
        <<ChunkId:32/unsigned-big, Length:32/unsigned-big,
            PhraseTf:32/unsigned-big, FirstTf:32/unsigned-big,
            SecondTf:32/unsigned-big>>
     || {ChunkId, _GroupId, _SourceId, Length, PhraseTf, FirstTf, SecondTf} <-
            Champions
    ]),
    EntryPayload = fts2_codec_encode_header_tail(
        fts2_codec_encode_bigram_entries(maps:get(entries, Bundle))
    ),
    PositionPayload = fts2_codec_encode_header_tail(
        fts2_codec_encode_positions(maps:get(positions, Bundle))
    ),
    <<?BIGRAM_VERSION:8,
        (maps:get(group_df, Bundle)):32/unsigned-big,
        (maps:get(first_df, Bundle)):32/unsigned-big,
        (maps:get(second_df, Bundle)):32/unsigned-big,
        (length(Champions)):32/unsigned-big,
        (byte_size(ChampionPayload)):32/unsigned-big,
        (byte_size(EntryPayload)):32/unsigned-big,
        (byte_size(PositionPayload)):32/unsigned-big,
        ChampionPayload/binary, EntryPayload/binary, PositionPayload/binary>>.

fts2_codec_decode_bigram(
    <<?BIGRAM_VERSION:8, GroupDf:32/unsigned-big,
        FirstDf:32/unsigned-big, SecondDf:32/unsigned-big,
        ChampionCount:32/unsigned-big, ChampionBytes:32/unsigned-big,
        EntryBytes:32/unsigned-big, PositionBytes:32/unsigned-big,
        Rest/binary>>
) when
    ChampionBytes =:= ChampionCount * 20,
    byte_size(Rest) =:= ChampionBytes + EntryBytes + PositionBytes
->
    <<ChampionPayload:ChampionBytes/binary, EntryPayload:EntryBytes/binary,
        PositionPayload:PositionBytes/binary>> = Rest,
    #{
        group_df => GroupDf,
        first_df => FirstDf,
        second_df => SecondDf,
        champions => fts2_codec_decode_bigram_champions(ChampionPayload, []),
        entries_packed => EntryPayload,
        positions_packed => PositionPayload
    };
fts2_codec_decode_bigram(Bad) ->
    erlang:error({invalid_fts2_bigram, Bad}).

fts2_codec_encode_bigram_entries(Entries) ->
    iolist_to_binary([
        <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
            SourceId:64/unsigned-big, Length:32/unsigned-big,
            PhraseTf:32/unsigned-big, FirstTf:32/unsigned-big,
            SecondTf:32/unsigned-big>>
     || {ChunkId, GroupId, SourceId, Length, PhraseTf, FirstTf, SecondTf} <-
            Entries
    ]).

fts2_codec_decode_bigram_champions(<<>>, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_bigram_champions(
    <<ChunkId:32/unsigned-big, Length:32/unsigned-big,
        PhraseTf:32/unsigned-big, FirstTf:32/unsigned-big,
        SecondTf:32/unsigned-big, Rest/binary>>,
    Acc
) ->
    fts2_codec_decode_bigram_champions(
        Rest,
        [{ChunkId, ChunkId bsr ?CHUNK_BITS, 0, Length,
            PhraseTf, FirstTf, SecondTf} | Acc]
    ).

fts2_codec_decode_bigram_entries(<<>>, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_bigram_entries(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        PhraseTf:32/unsigned-big, FirstTf:32/unsigned-big,
        SecondTf:32/unsigned-big, Rest/binary>>,
    Acc
) ->
    fts2_codec_decode_bigram_entries(
        Rest,
        [{ChunkId, GroupId, SourceId, Length, PhraseTf, FirstTf, SecondTf} | Acc]
    ).

fts2_codec_bigram_entries(Bundle) ->
    fts2_codec_decode_bigram_entries(
        fts2_codec_decode_header_tail(maps:get(entries_packed, Bundle)), []
    ).

fts2_codec_bigram_positions(Bundle) ->
    fts2_codec_decode_positions(
        fts2_codec_decode_header_tail(maps:get(positions_packed, Bundle))
    ).

fts2_codec_bigram_positions(Bundle, WantedChunkIds) ->
    <<?POSITION_VERSION:8, Count:32/unsigned-big, Payload/binary>> =
        fts2_codec_decode_header_tail(maps:get(positions_packed, Bundle)),
    fts2_codec_decode_selected_positions(
        Count, Payload, maps:from_keys(WantedChunkIds, true), #{}
    ).

fts2_codec_encode_plane_entries(Entries) ->
    iolist_to_binary([
        <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
            SourceId:64/unsigned-big, DocLength:32/unsigned-big,
            Tf:32/unsigned-big, DenseId:32/unsigned-big>>
     || {ChunkId, GroupId, SourceId, DocLength, Tf, DenseId} <- Entries
    ]).

fts2_codec_encode_root(Root) ->
    Payload = term_to_binary(Root, [deterministic]),
    <<?ROOT_VERSION:8, (byte_size(Payload)):32/unsigned-big, Payload/binary>>.

fts2_codec_decode_root(<<?ROOT_VERSION:8, Bytes:32/unsigned-big, Payload:Bytes/binary>>) ->
    _ = fts2_codec_root_wire_atoms(),
    binary_to_term(Payload, [safe]);
fts2_codec_decode_root(Bad) ->
    erlang:error({invalid_fts2_root, Bad}).

%% Boolean/anchor planes are fixed-width and deliberately uncompressed.  They
%% are already dense integer streams and avoiding decompression keeps the full
%% result/hash path predictable.  Header/champion and identity rows use
%% fts2_codec_encode/2 because those payloads contain arbitrary projected Ash values.
fts2_codec_encode_plane(Entries) ->
    Payload = fts2_codec_encode_plane_entries(Entries),
    <<?PLANE_VERSION:8, (length(Entries)):32/unsigned-big, Payload/binary>>.

fts2_codec_decode_plane(<<?PLANE_VERSION:8, Count:32/unsigned-big, Payload/binary>>) when
    byte_size(Payload) =:= Count * 28
->
    fts2_codec_decode_plane_entries(Payload, []);
fts2_codec_decode_plane(Bad) ->
    erlang:error({invalid_fts2_plane, Bad}).

fts2_codec_decode_selected_plane(
    <<?PLANE_VERSION:8, Count:32/unsigned-big, Payload/binary>>, Wanted
) when byte_size(Payload) =:= Count * 28, is_map(Wanted) ->
    fts2_codec_decode_selected_plane_entries(Payload, Wanted, []);
fts2_codec_decode_selected_plane(Bad, _Wanted) ->
    erlang:error({invalid_fts2_plane, Bad}).

fts2_codec_decode_selected_plane_entries(<<>>, _Wanted, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_selected_plane_entries(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, DocLength:32/unsigned-big,
        Tf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    Wanted,
    Acc
) ->
    Next = case maps:is_key(ChunkId, Wanted) of
        true -> [
            {ChunkId, GroupId, SourceId, DocLength, Tf, DenseId} | Acc
        ];
        false -> Acc
    end,
    fts2_codec_decode_selected_plane_entries(Rest, Wanted, Next).

fts2_codec_decode_plane_entries(<<>>, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_plane_entries(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, DocLength:32/unsigned-big, Tf:32/unsigned-big,
        DenseId:32/unsigned-big,
        Rest/binary>>,
    Acc
) ->
    fts2_codec_decode_plane_entries(
        Rest, [{ChunkId, GroupId, SourceId, DocLength, Tf, DenseId} | Acc]
    ).

fts2_codec_encode_positions(Entries) ->
    Payload = iolist_to_binary([
        begin
            Encoded = fts2_codec_encode_position_list(Positions),
            <<ChunkId:32/unsigned-big, (length(Positions)):32/unsigned-big,
                (byte_size(Encoded)):32/unsigned-big, Encoded/binary>>
        end
     || {ChunkId, Positions} <- lists:sort(Entries)
    ]),
    <<?POSITION_VERSION:8, (length(Entries)):32/unsigned-big, Payload/binary>>.

fts2_codec_decode_positions(
    <<?POSITION_VERSION:8, Count:32/unsigned-big, Payload/binary>>
) ->
    fts2_codec_decode_position_entries(Count, Payload, #{});
fts2_codec_decode_positions(Bad) ->
    erlang:error({invalid_fts2_positions, Bad}).

fts2_codec_decode_positions(
    <<?POSITION_VERSION:8, Count:32/unsigned-big, Payload/binary>>,
    WantedChunkIds
) ->
    fts2_codec_decode_selected_positions(
        Count, Payload, maps:from_keys(WantedChunkIds, true), #{}
    );
fts2_codec_decode_positions(Bad, _WantedChunkIds) ->
    erlang:error({invalid_fts2_positions, Bad}).

fts2_codec_decode_position_entries(0, <<>>, Acc) ->
    Acc;
fts2_codec_decode_position_entries(
    Count,
    <<ChunkId:32/unsigned-big, PositionCount:32/unsigned-big,
        Bytes:32/unsigned-big, Encoded:Bytes/binary, Rest/binary>>,
    Acc
) when Count > 0 ->
    Positions = fts2_codec_decode_position_list(PositionCount, Encoded, 0, []),
    fts2_codec_decode_position_entries(Count - 1, Rest, Acc#{ChunkId => Positions});
fts2_codec_decode_position_entries(_Count, Bad, _Acc) ->
    erlang:error({invalid_fts2_positions, Bad}).

fts2_codec_encode_position_list(Positions) ->
    {_, Encoded} = lists:foldl(
        fun(Position, {Previous, Acc}) when
            is_integer(Position), Position >= Previous, Position =< 16#FFFFFFFF
        ->
            {Position, [fts2_codec_encode_varint(Position - Previous) | Acc]}
        end,
        {0, []},
        Positions
    ),
    iolist_to_binary(lists:reverse(Encoded)).

fts2_codec_decode_position_list(0, <<>>, _Previous, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_position_list(Count, Encoded, Previous, Acc) when Count > 0 ->
    {Delta, Rest} = fts2_codec_decode_varint(Encoded, 0, 0),
    Position = Previous + Delta,
    fts2_codec_decode_position_list(Count - 1, Rest, Position, [Position | Acc]);
fts2_codec_decode_position_list(_Count, Bad, _Previous, _Acc) ->
    erlang:error({invalid_fts2_position_list, Bad}).

%% Identity pages are deliberately uncompressed fixed-stride directories.
%% Page version 4 carries one layout dictionary: candidate field names plus
%% hit-record ordinals into that same field list. Chunks then store positional
%% values only, so a served page decodes names once and can project individual
%% tie/filter columns without materialising every candidate record.
fts2_codec_encode_identity_page([]) ->
    Dict = fts2_codec_encode_identity_dict(none),
    <<?IDENTITY_PAGE_VERSION:8, 0:32, 0:16,
        (byte_size(Dict)):32/unsigned-big, Dict/binary, 4:32, 0:32>>;
fts2_codec_encode_identity_page(Groups) ->
    Ordered = lists:sort(
        fun(A, B) -> maps:get(group_id, A) < maps:get(group_id, B) end,
        Groups
    ),
    FirstGroupId = maps:get(group_id, hd(Ordered)),
    true = [FirstGroupId + I || I <- lists:seq(0, length(Ordered) - 1)] =:=
        [maps:get(group_id, Group) || Group <- Ordered],
    Layout = fts2_codec_identity_layout(Ordered),
    Dict = fts2_codec_encode_identity_dict(Layout),
    EncodedGroups = [
        fts2_codec_encode_identity_group(Group, Layout)
     || Group <- Ordered
    ],
    {Directory, Payload} = fts2_codec_encode_identity_offsets(EncodedGroups),
    <<?IDENTITY_PAGE_VERSION:8, FirstGroupId:32/unsigned-big,
        (length(Ordered)):16/unsigned-big,
        (byte_size(Dict)):32/unsigned-big, Dict/binary,
        (byte_size(Directory)):32/unsigned-big, Directory/binary,
        Payload/binary>>.

fts2_codec_identity_layout(Groups) ->
    case [Chunk || Group <- Groups, Chunk <- maps:get(chunks, Group)] of
        [] ->
            none;
        [Chunk | _] ->
            case {maps:get(candidate_record, Chunk), maps:get(hit_record, Chunk)} of
                {
                    #{'$fts_text_blocks' := _, '$fts_text_bytes' := _} = Candidate,
                    #{doc_key := _, record := Record, text_blocks := _,
                        text_bytes := _} = Hit
                } when map_size(Hit) =:= 4, is_map(Record) ->
                    CandKeys = lists:sort(
                        maps:keys(Candidate) --
                            ['$fts_text_blocks', '$fts_text_bytes']
                    ),
                    HitKeys = lists:sort(maps:keys(Record)),
                    case CandKeys =/= [] andalso
                        lists:all(fun is_atom/1, CandKeys) andalso
                        lists:all(
                            fun(Key) -> lists:member(Key, CandKeys) end,
                            HitKeys
                        ) andalso length(CandKeys) =< 16#FFFF
                    of
                        true -> {CandKeys, HitKeys};
                        false -> none
                    end;
                _ ->
                    none
            end
    end.

fts2_codec_encode_identity_dict(none) ->
    <<0:16/unsigned-big, 0:16/unsigned-big>>;
fts2_codec_encode_identity_dict({CandKeys, HitKeys}) ->
    Names = <<
        <<(byte_size(Name)):16/unsigned-big, Name/binary>>
     || Name <- [atom_to_binary(Key, utf8) || Key <- CandKeys]
    >>,
    Ordinals = <<
        <<(fts2_codec_identity_ordinal(Key, CandKeys)):16/unsigned-big>>
     || Key <- HitKeys
    >>,
    <<(length(CandKeys)):16/unsigned-big, Names/binary,
        (length(HitKeys)):16/unsigned-big, Ordinals/binary>>.

fts2_codec_decode_identity_dict(<<0:16/unsigned-big, 0:16/unsigned-big>>) ->
    none;
fts2_codec_decode_identity_dict(<<Count:16/unsigned-big, Rest0/binary>>) ->
    {CandKeys, Rest1} = fts2_codec_decode_identity_names(Count, Rest0, []),
    <<HitCount:16/unsigned-big, Ordinals:(HitCount * 2)/binary>> = Rest1,
    Indexed = list_to_tuple(CandKeys),
    HitKeys = [element(Ordinal + 1, Indexed) ||
        <<Ordinal:16/unsigned-big>> <= Ordinals],
    {CandKeys, HitKeys}.

fts2_codec_decode_identity_names(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
fts2_codec_decode_identity_names(
    Count, <<Bytes:16/unsigned-big, Name:Bytes/binary, Rest/binary>>, Acc
) when Count > 0 ->
    fts2_codec_decode_identity_names(
        Count - 1, Rest, [binary_to_existing_atom(Name, utf8) | Acc]
    ).

fts2_codec_identity_ordinal(Key, Keys) ->
    fts2_codec_identity_ordinal(Key, Keys, 0).

fts2_codec_identity_ordinal(Key, [Key | _Rest], Ordinal) ->
    Ordinal;
fts2_codec_identity_ordinal(Key, [_Other | Rest], Ordinal) ->
    fts2_codec_identity_ordinal(Key, Rest, Ordinal + 1).

fts2_codec_encode_identity_group(Group, Layout) ->
    GroupKey = fts2_codec_encode_identity_value(maps:get(group_key, Group)),
    Chunks = lists:sort(
        fun(A, B) -> maps:get(chunk_id, A) < maps:get(chunk_id, B) end,
        maps:get(chunks, Group)
    ),
    FirstChunkId = maps:get(chunk_id, hd(Chunks)),
    true = [FirstChunkId + I || I <- lists:seq(0, length(Chunks) - 1)] =:=
        [maps:get(chunk_id, Chunk) || Chunk <- Chunks],
    EncodedChunks = [
        fts2_codec_encode_identity_chunk(Chunk, Layout)
     || Chunk <- Chunks
    ],
    {Directory, Payload} = fts2_codec_encode_identity_offsets(EncodedChunks),
    <<(byte_size(GroupKey)):32/unsigned-big, GroupKey/binary,
        FirstChunkId:32/unsigned-big, (length(Chunks)):16/unsigned-big,
        (byte_size(Directory)):32/unsigned-big, Directory/binary,
        Payload/binary>>.

fts2_codec_encode_identity_chunk(Chunk, Layout) ->
    DocKey = maps:get(doc_key, Chunk),
    DocVersion = maps:get(doc_version, Chunk),
    CandidateRecord = maps:get(candidate_record, Chunk),
    HitRecord = maps:get(hit_record, Chunk),
    case fts2_codec_identity_public_row(
        Layout, DocKey, CandidateRecord, HitRecord
    ) of
        {ok, Blocks, TextBytes, Values} ->
            TextDirectory = fts2_codec_encode_identity_text_directory(Blocks),
            {ValueDirectory, ValuePayload} = fts2_codec_encode_identity_offsets(
                [fts2_codec_encode_identity_value(Value) || Value <- Values]
            ),
            <<2, (maps:get(source_id, Chunk)):64/unsigned-big,
                (maps:get(doc_length, Chunk)):32/unsigned-big,
                (byte_size(DocKey)):32/unsigned-big, DocKey/binary,
                (byte_size(DocVersion)):16/unsigned-big, DocVersion/binary,
                TextBytes:32/unsigned-big, (length(Blocks)):16/unsigned-big,
                TextDirectory/binary,
                (byte_size(ValueDirectory)):32/unsigned-big,
                ValueDirectory/binary, ValuePayload/binary>>;
        no ->
            Candidate = fts2_codec_encode_identity_value(CandidateRecord),
            Hit = fts2_codec_encode_identity_value(HitRecord),
            <<0, (maps:get(source_id, Chunk)):64/unsigned-big,
                (maps:get(doc_length, Chunk)):32/unsigned-big,
                (byte_size(DocKey)):32/unsigned-big, DocKey/binary,
                (byte_size(DocVersion)):16/unsigned-big, DocVersion/binary,
                (byte_size(Candidate)):32/unsigned-big, Candidate/binary,
                (byte_size(Hit)):32/unsigned-big, Hit/binary>>
    end.

fts2_codec_identity_public_row(none, _DocKey, _Candidate, _Hit) ->
    no;
fts2_codec_identity_public_row(
    {CandKeys, HitKeys}, DocKey,
    #{'$fts_text_blocks' := Blocks, '$fts_text_bytes' := TextBytes} = Candidate,
    #{doc_key := HitDocKey, record := Record, text_blocks := HitBlocks,
        text_bytes := HitTextBytes} = Hit
) when
    map_size(Hit) =:= 4,
    map_size(Candidate) =:= length(CandKeys) + 2,
    map_size(Record) =:= length(HitKeys),
    DocKey =:= HitDocKey,
    Blocks =:= HitBlocks,
    TextBytes =:= HitTextBytes,
    is_integer(TextBytes),
    is_list(Blocks),
    length(Blocks) =< 16#FFFF
->
    case lists:all(fun(Key) -> maps:is_key(Key, Candidate) end, CandKeys) andalso
        lists:all(
            fun(Key) ->
                maps:get(Key, Record, '$fts_absent') =:= maps:get(Key, Candidate)
            end,
            HitKeys
        )
    of
        true ->
            {ok, Blocks, TextBytes,
                [maps:get(Key, Candidate) || Key <- CandKeys]};
        false ->
            no
    end;
fts2_codec_identity_public_row(_Layout, _DocKey, _Candidate, _Hit) ->
    no.

fts2_codec_encode_identity_offsets(Binaries) ->
    {Offsets, Payload, FinalOffset} = lists:foldl(
        fun(Binary, {OffsetAcc, PayloadAcc, Offset}) ->
            {[<<Offset:32/unsigned-big>> | OffsetAcc],
                [Binary | PayloadAcc], Offset + byte_size(Binary)}
        end,
        {[], [], 0},
        Binaries
    ),
    {
        iolist_to_binary(
            lists:reverse([<<FinalOffset:32/unsigned-big>> | Offsets])
        ),
        iolist_to_binary(lists:reverse(Payload))
    }.

fts2_codec_identity_page_handle(
    <<?IDENTITY_PAGE_VERSION:8, FirstGroupId:32/unsigned-big,
        Count:16/unsigned-big, DictBytes:32/unsigned-big,
        Dict:DictBytes/binary, DirectoryBytes:32/unsigned-big,
        Directory:DirectoryBytes/binary, Payload/binary>>
) when DirectoryBytes =:= (Count + 1) * 4 ->
    {ipage, FirstGroupId, Count, Directory, Payload,
        fts2_codec_decode_identity_dict(Dict)};
fts2_codec_identity_page_handle(Bad) ->
    erlang:error({invalid_fts2_identity_page, Bad}).

fts2_codec_identity_page_layout({ipage, _First, _Count, _Dir, _Payload, Layout}) ->
    Layout.

fts2_codec_identity_page_ordinals({ipage, _F, _C, _D, _P, none}, _Names) ->
    fallback;
fts2_codec_identity_page_ordinals(
    {ipage, _F, _C, _D, _P, {CandKeys, _HitKeys}}, Names
) ->
    case lists:all(fun(Name) -> lists:member(Name, CandKeys) end, Names) of
        true -> [fts2_codec_identity_ordinal(Name, CandKeys) || Name <- Names];
        false -> fallback
    end.

fts2_codec_decode_identity_page(Binary, Wanted) when is_list(Wanted) ->
    Handle = fts2_codec_identity_page_handle(Binary),
    lists:append([
        fts2_codec_decode_identity_request(Request, Handle)
     || Request <- Wanted
    ]).

fts2_codec_decode_identity_request(GroupId, Handle) when is_integer(GroupId) ->
    fts2_codec_identity_group_slice(GroupId, Handle, all);
fts2_codec_decode_identity_request({GroupId, ChunkId}, Handle) ->
    fts2_codec_identity_group_slice(GroupId, Handle, {chunk, ChunkId});
fts2_codec_decode_identity_request({serve, GroupId, ChunkId}, Handle) ->
    fts2_codec_identity_group_slice(GroupId, Handle, {serve_chunk, ChunkId}).

fts2_codec_identity_group_slice(
    GroupId, {ipage, FirstGroupId, Count, Directory, Payload, Layout}, Wanted
) ->
    case GroupId - FirstGroupId of
        Ordinal when Ordinal >= 0, Ordinal < Count ->
            Group = fts2_codec_identity_slice(Ordinal, Directory, Payload),
            fts2_codec_decode_identity_group(GroupId, Group, Wanted, Layout);
        _ ->
            []
    end.

fts2_codec_identity_page_row(Handle, GroupId, ChunkId) ->
    case fts2_codec_identity_group_slice(
        GroupId, Handle, {serve_chunk, ChunkId}
    ) of
        [Row] -> Row;
        [] -> undefined
    end.

fts2_codec_identity_page_project(Handle, GroupId, ChunkId, Ordinals) ->
    case fts2_codec_identity_chunk_slice(Handle, GroupId, ChunkId) of
        undefined ->
            undefined;
        <<2, _SourceId:64/unsigned-big, _DocLength:32/unsigned-big,
            DocKeyBytes:32/unsigned-big, DocKey:DocKeyBytes/binary,
            VersionBytes:16/unsigned-big, _DocVersion:VersionBytes/binary,
            _TextBytes:32/unsigned-big, BlockCount:16/unsigned-big,
            _TextDirectory:(BlockCount * 12)/binary,
            ValueDirectoryBytes:32/unsigned-big,
            ValueDirectory:ValueDirectoryBytes/binary,
            ValuePayload/binary>> ->
            {ok, DocKey, [
                begin
                    {Value, <<>>} = fts2_codec_decode_identity_value(
                        fts2_codec_identity_slice(
                            Ordinal, ValueDirectory, ValuePayload
                        )
                    ),
                    Value
                end
             || Ordinal <- Ordinals
            ]};
        Slice ->
            Row = fts2_codec_decode_identity_chunk(
                GroupId, undefined, ChunkId, Slice,
                fts2_codec_identity_page_layout(Handle)
            ),
            {fallback, element(5, Row), element(8, Row)}
    end.

fts2_codec_identity_chunk_slice(
    {ipage, FirstGroupId, Count, Directory, Payload, _Layout}, GroupId, ChunkId
) ->
    case GroupId - FirstGroupId of
        GroupOrdinal when GroupOrdinal >= 0, GroupOrdinal < Count ->
            <<_GroupKeyBytes:32/unsigned-big, _GroupKey:_GroupKeyBytes/binary,
                FirstChunkId:32/unsigned-big, ChunkCount:16/unsigned-big,
                ChunkDirectoryBytes:32/unsigned-big,
                ChunkDirectory:ChunkDirectoryBytes/binary,
                ChunkPayload/binary>> =
                fts2_codec_identity_slice(GroupOrdinal, Directory, Payload),
            case ChunkId - FirstChunkId of
                ChunkOrdinal when ChunkOrdinal >= 0, ChunkOrdinal < ChunkCount ->
                    fts2_codec_identity_slice(
                        ChunkOrdinal, ChunkDirectory, ChunkPayload
                    );
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

fts2_codec_decode_identity_group(
    GroupId,
    <<GroupKeyBytes:32/unsigned-big, GroupKeyEncoded:GroupKeyBytes/binary,
        FirstChunkId:32/unsigned-big, Count:16/unsigned-big,
        DirectoryBytes:32/unsigned-big, Directory:DirectoryBytes/binary,
        Payload/binary>>,
    Wanted,
    Layout
) when DirectoryBytes =:= (Count + 1) * 4 ->
    GroupKey = case Wanted of
        {serve_chunk, _ChunkId} -> undefined;
        _ ->
            {DecodedGroupKey, <<>>} = fts2_codec_decode_identity_value(
                GroupKeyEncoded
            ),
            DecodedGroupKey
    end,
    Ordinals = case Wanted of
        all -> lists:seq(0, Count - 1);
        {chunk, ChunkId} -> [ChunkId - FirstChunkId];
        {serve_chunk, ChunkId} -> [ChunkId - FirstChunkId]
    end,
    [
        fts2_codec_decode_identity_chunk(
            GroupId, GroupKey, FirstChunkId + Ordinal,
            fts2_codec_identity_slice(Ordinal, Directory, Payload), Layout
        )
     || Ordinal <- Ordinals, Ordinal >= 0, Ordinal < Count
    ];
fts2_codec_decode_identity_group(_GroupId, Bad, _Wanted, _Layout) ->
    erlang:error({invalid_fts2_identity_group, Bad}).

fts2_codec_decode_identity_chunk(
    GroupId, GroupKey, ChunkId,
    <<2, SourceId:64/unsigned-big, DocLength:32/unsigned-big,
        DocKeyBytes:32/unsigned-big, DocKey:DocKeyBytes/binary,
        VersionBytes:16/unsigned-big, DocVersion:VersionBytes/binary,
        TextBytes:32/unsigned-big, BlockCount:16/unsigned-big,
        TextDirectory:(BlockCount * 12)/binary,
        ValueDirectoryBytes:32/unsigned-big,
        _ValueDirectory:ValueDirectoryBytes/binary, ValuePayload/binary>>,
    {CandKeys, HitKeys}
) ->
    Blocks = fts2_codec_decode_identity_text_directory(TextDirectory),
    Base = maps:from_list(lists:zip(
        CandKeys,
        fts2_codec_decode_identity_values(length(CandKeys), ValuePayload, [])
    )),
    Candidate = Base#{
        '$fts_text_blocks' => Blocks, '$fts_text_bytes' => TextBytes
    },
    Record = case HitKeys =:= CandKeys of
        true -> Base;
        false -> maps:with(HitKeys, Base)
    end,
    Hit = #{
        doc_key => DocKey, record => Record,
        text_blocks => Blocks, text_bytes => TextBytes
    },
    {GroupId, GroupKey, ChunkId, SourceId, DocKey, DocVersion, DocLength,
        Candidate, Hit};
fts2_codec_decode_identity_chunk(
    GroupId, GroupKey, ChunkId,
    <<0, SourceId:64/unsigned-big, DocLength:32/unsigned-big,
        DocKeyBytes:32/unsigned-big, DocKey:DocKeyBytes/binary,
        VersionBytes:16/unsigned-big, DocVersion:VersionBytes/binary,
        CandidateBytes:32/unsigned-big, CandidateEncoded:CandidateBytes/binary,
        HitBytes:32/unsigned-big, HitEncoded:HitBytes/binary>>,
    _Layout
) ->
    {Candidate, <<>>} = fts2_codec_decode_identity_value(CandidateEncoded),
    {Hit, <<>>} = fts2_codec_decode_identity_value(HitEncoded),
    {GroupId, GroupKey, ChunkId, SourceId, DocKey, DocVersion, DocLength,
        Candidate, Hit};
fts2_codec_decode_identity_chunk(_GroupId, _GroupKey, _ChunkId, Bad, _Layout) ->
    erlang:error({invalid_fts2_identity_chunk, Bad}).

fts2_codec_decode_identity_values(0, _Rest, Acc) ->
    lists:reverse(Acc);
fts2_codec_decode_identity_values(Count, Encoded, Acc) when Count > 0 ->
    {Value, Rest} = fts2_codec_decode_identity_value(Encoded),
    fts2_codec_decode_identity_values(Count - 1, Rest, [Value | Acc]).

fts2_codec_identity_slice(Ordinal, Directory, Payload) ->
    <<Start:32/unsigned-big, Finish:32/unsigned-big>> =
        binary:part(Directory, Ordinal * 4, 8),
    binary:part(Payload, Start, Finish - Start).

fts2_codec_encode_identity_value(Binary) when is_binary(Binary) ->
    <<1, (byte_size(Binary)):32/unsigned-big, Binary/binary>>;
fts2_codec_encode_identity_value(Integer) when is_integer(Integer) ->
    Sign = case Integer < 0 of true -> 1; false -> 0 end,
    Magnitude = binary:encode_unsigned(abs(Integer)),
    <<2, Sign:8, (byte_size(Magnitude)):16/unsigned-big, Magnitude/binary>>;
fts2_codec_encode_identity_value(Float) when is_float(Float) ->
    <<3, Float:64/float>>;
fts2_codec_encode_identity_value(Atom) when is_atom(Atom) ->
    Name = atom_to_binary(Atom, utf8),
    <<4, (byte_size(Name)):16/unsigned-big, Name/binary>>;
fts2_codec_encode_identity_value(List) when is_list(List) ->
    Encoded = [fts2_codec_encode_identity_framed(Item) || Item <- List],
    <<5, (length(List)):32/unsigned-big, (iolist_to_binary(Encoded))/binary>>;
fts2_codec_encode_identity_value(Tuple) when is_tuple(Tuple) ->
    Items = tuple_to_list(Tuple),
    Encoded = [fts2_codec_encode_identity_framed(Item) || Item <- Items],
    <<6, (length(Items)):16/unsigned-big, (iolist_to_binary(Encoded))/binary>>;
fts2_codec_encode_identity_value(#{
    '$fts_text_blocks' := Blocks,
    '$fts_text_bytes' := TextBytes,
    udi := Udi,
    content_version := Version
} = Map) when
    map_size(Map) =:= 4,
    is_binary(Udi),
    is_integer(Version),
    is_integer(TextBytes),
    is_list(Blocks),
    length(Blocks) =< 16#FFFF
->
    Directory = fts2_codec_encode_identity_text_directory(Blocks),
    <<8, (byte_size(Udi)):32/unsigned-big, Udi/binary,
        Version:64/signed-big, TextBytes:32/unsigned-big,
        (length(Blocks)):16/unsigned-big, Directory/binary>>;
fts2_codec_encode_identity_value(#{
    doc_key := DocKey,
    record := #{udi := Udi} = Record,
    text_blocks := Blocks,
    text_bytes := TextBytes
} = Map) when
    map_size(Map) =:= 4,
    map_size(Record) =:= 1,
    is_binary(DocKey),
    is_binary(Udi),
    is_integer(TextBytes),
    is_list(Blocks),
    length(Blocks) =< 16#FFFF
->
    Directory = fts2_codec_encode_identity_text_directory(Blocks),
    <<9, (byte_size(DocKey)):32/unsigned-big, DocKey/binary,
        (byte_size(Udi)):32/unsigned-big, Udi/binary,
        TextBytes:32/unsigned-big, (length(Blocks)):16/unsigned-big,
        Directory/binary>>;
fts2_codec_encode_identity_value(Map) when is_map(Map) ->
    Pairs = lists:sort(maps:to_list(Map)),
    Encoded = [
        [fts2_codec_encode_identity_framed(Key), fts2_codec_encode_identity_framed(Value)]
     || {Key, Value} <- Pairs
    ],
    <<7, (length(Pairs)):32/unsigned-big, (iolist_to_binary(Encoded))/binary>>;
fts2_codec_encode_identity_value(Value) ->
    erlang:error({unsupported_fts2_identity_value, Value}).

fts2_codec_encode_identity_framed(Value) ->
    Encoded = fts2_codec_encode_identity_value(Value),
    <<(byte_size(Encoded)):32/unsigned-big, Encoded/binary>>.

fts2_codec_decode_identity_value(<<1, Bytes:32/unsigned-big, Value:Bytes/binary,
    Rest/binary>>) ->
    {Value, Rest};
fts2_codec_decode_identity_value(<<2, Sign:8, Bytes:16/unsigned-big,
    Magnitude:Bytes/binary, Rest/binary>>) ->
    Unsigned = binary:decode_unsigned(Magnitude),
    {case Sign of 0 -> Unsigned; 1 -> -Unsigned end, Rest};
fts2_codec_decode_identity_value(<<3, Value:64/float, Rest/binary>>) ->
    {Value, Rest};
fts2_codec_decode_identity_value(<<4, Bytes:16/unsigned-big, Name:Bytes/binary,
    Rest/binary>>) ->
    {binary_to_existing_atom(Name, utf8), Rest};
fts2_codec_decode_identity_value(<<5, Count:32/unsigned-big, Rest/binary>>) ->
    fts2_codec_decode_identity_sequence(Count, Rest, [], list);
fts2_codec_decode_identity_value(<<6, Count:16/unsigned-big, Rest/binary>>) ->
    fts2_codec_decode_identity_sequence(Count, Rest, [], tuple);
fts2_codec_decode_identity_value(<<7, Count:32/unsigned-big, Rest/binary>>) ->
    fts2_codec_decode_identity_pairs(Count, Rest, #{});
fts2_codec_decode_identity_value(<<8, UdiBytes:32/unsigned-big,
    Udi:UdiBytes/binary, Version:64/signed-big, TextBytes:32/unsigned-big,
    Count:16/unsigned-big, Directory:(Count * 12)/binary, Rest/binary>>) ->
    {#{
        '$fts_text_blocks' => fts2_codec_decode_identity_text_directory(
            Directory
        ),
        '$fts_text_bytes' => TextBytes,
        udi => Udi,
        content_version => Version
    }, Rest};
fts2_codec_decode_identity_value(<<9, DocKeyBytes:32/unsigned-big,
    DocKey:DocKeyBytes/binary, UdiBytes:32/unsigned-big,
    Udi:UdiBytes/binary, TextBytes:32/unsigned-big,
    Count:16/unsigned-big, Directory:(Count * 12)/binary, Rest/binary>>) ->
    {#{
        doc_key => DocKey,
        record => #{udi => Udi},
        text_blocks => fts2_codec_decode_identity_text_directory(Directory),
        text_bytes => TextBytes
    }, Rest};
fts2_codec_decode_identity_value(Bad) ->
    erlang:error({invalid_fts2_identity_value, Bad}).

fts2_codec_decode_identity_sequence(0, Rest, Acc, list) ->
    {lists:reverse(Acc), Rest};
fts2_codec_decode_identity_sequence(0, Rest, Acc, tuple) ->
    {list_to_tuple(lists:reverse(Acc)), Rest};
fts2_codec_decode_identity_sequence(Count, <<Bytes:32/unsigned-big,
    Encoded:Bytes/binary, Rest/binary>>, Acc, Type) when Count > 0 ->
    {Value, <<>>} = fts2_codec_decode_identity_value(Encoded),
    fts2_codec_decode_identity_sequence(Count - 1, Rest, [Value | Acc], Type).

fts2_codec_decode_identity_pairs(0, Rest, Acc) ->
    {Acc, Rest};
fts2_codec_decode_identity_pairs(Count, <<KeyBytes:32/unsigned-big,
    KeyEncoded:KeyBytes/binary, ValueBytes:32/unsigned-big,
    ValueEncoded:ValueBytes/binary, Rest/binary>>, Acc) when Count > 0 ->
    {Key, <<>>} = fts2_codec_decode_identity_value(KeyEncoded),
    {Value, <<>>} = fts2_codec_decode_identity_value(ValueEncoded),
    fts2_codec_decode_identity_pairs(Count - 1, Rest, Acc#{Key => Value}).

fts2_codec_encode_identity_text_directory(Blocks) ->
    <<
        <<BlockNo:32/unsigned-big, Ordinal:32/unsigned-big,
            Offset:32/unsigned-big>>
     || {BlockNo, Ordinal, Offset} <- Blocks
    >>.

fts2_codec_decode_identity_text_directory(Directory) ->
    [
        {BlockNo, Ordinal, Offset}
     || <<BlockNo:32/unsigned-big, Ordinal:32/unsigned-big,
            Offset:32/unsigned-big>> <= Directory
    ].

fts2_codec_encode_varint(Value) when Value < 128 ->
    <<Value>>;
fts2_codec_encode_varint(Value) ->
    <<((Value band 127) bor 128), (fts2_codec_encode_varint(Value bsr 7))/binary>>.

fts2_codec_decode_varint(<<Byte, Rest/binary>>, Shift, Acc) when Shift =< 63 ->
    Value = Acc bor ((Byte band 127) bsl Shift),
    case Byte band 128 of
        0 -> {Value, Rest};
        _ -> fts2_codec_decode_varint(Rest, Shift + 7, Value)
    end;
fts2_codec_decode_varint(Bad, _Shift, _Acc) ->
    erlang:error({invalid_fts2_varint, Bad}).

fts2_codec_type_id(header) -> 1;
fts2_codec_type_id(delta) -> 5;
fts2_codec_type_id(bigram_bundle) -> 6.

fts2_codec_root_wire_atoms() ->
    [
        version,
        generation,
        fingerprint,
        chunk_bits,
        group_count,
        chunk_count,
        term_count,
        bigram_count,
        identity_page_count,
        identity_page_shift,
        total_length,
        phrase_strategy,
        previous_generation,
        facet_domain,
        position_order,
        chunk,
        uniform,
        mixed,
        bigram,
        skip,
        undefined
    ].

fts2_codec_row_wire_atoms() ->
    [
        group_df,
        chunk_df,
        collection_frequency,
        champions,
        entries,
        positions,
        group_id,
        group_key,
        group_version,
        chunks,
        chunk_id,
        source_id,
        doc_key,
        doc_version,
        doc_length,
        base_length,
        candidate_record,
        hit_record,
        status,
        retired_ids,
        posting,
        live,
        remove
    ].


%% ===========================================================================
%% INTERNAL FTS-2 DELTA
%% ===========================================================================

%% Exact doc-major FTS2 delta reader/evaluator.

fts2_delta_read(Bookie, #{index := Bucket}) ->
    fts2_delta_read(Bookie, #{index => Bucket}, undefined).

fts2_delta_read(Bookie, #{index := Bucket}, Hook) ->
    Fold = fun
        (B, {<<"f2:d">>, <<SourceId:64/unsigned-big>>}, Value, Acc) when
            B =:= Bucket
        ->
            [{SourceId, fts2_codec_decode(delta, Value)} | Acc];
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {
            {<<"f2:d">>, <<>>},
            {<<"f2:d">>, <<16#FFFFFFFFFFFFFFFF:64/unsigned-big>>}
        }},
        {Fold, []},
        false,
        true,
        false
    ),
    Deltas = lists:reverse(Runner()),
    case Hook of
        undefined -> ok;
        Fun when is_function(Fun, 1) -> Fun({fts2, Deltas});
        Fun when is_function(Fun, 0) -> Fun()
    end,
    Deltas.

fts2_delta_read_for_search(Bookie, #{index := Bucket} = Schema, AST, Hook) ->
    case leveled_bookie:book_headonly_many(
        Bookie,
        Bucket,
        [{<<"f2:p">>, <<"ready">>}, {<<"f2:p">>, <<"mutated">>}]
    ) of
        [{ok, _Ready}, not_found] ->
            Tokens = lists:usort(fts2_delta_presence_tokens(AST)),
            Results = leveled_bookie:book_headonly_many(
                Bookie,
                Bucket,
                [
                    {<<"f2:p">>, <<(erlang:crc32(Token) band 16#FFFF):16/unsigned-big>>}
                 || Token <- Tokens
                ]
            ),
            Present = maps:from_list([
                {Token, Result =/= not_found}
             || {Token, Result} <- lists:zip(Tokens, Results)
            ]),
            case fts2_delta_presence_possible(AST, Present) of
                true -> fts2_delta_read(Bookie, Schema, Hook);
                false ->
                    fts2_delta_presence_hook(Hook, []),
                    []
            end;
        _LegacyOrMutatedTail ->
            %% A tail created before presence markers were introduced cannot
            %% be skipped safely.  Updates/removals also need the full overlay
            %% to suppress retired immutable matches.
            fts2_delta_read(Bookie, Schema, Hook)
    end.

fts2_delta_presence_tokens({term, Token, false, _Columns}) -> [Token];
fts2_delta_presence_tokens({term, _Token, true, _Columns}) -> [];
fts2_delta_presence_tokens({phrase, Specs, _Columns}) ->
    [Token || {Token, false, _Offset} <- Specs];
fts2_delta_presence_tokens({near, Items, _Distance, _Columns}) ->
    lists:append([fts2_delta_presence_tokens(Item) || Item <- Items]);
fts2_delta_presence_tokens({anchor, Child}) ->
    fts2_delta_presence_tokens(Child);
fts2_delta_presence_tokens({_Op, A, B}) ->
    fts2_delta_presence_tokens(A) ++ fts2_delta_presence_tokens(B);
fts2_delta_presence_tokens(_Other) -> [].

fts2_delta_presence_possible({empty}, _Present) -> false;
fts2_delta_presence_possible({all_docs}, _Present) -> true;
fts2_delta_presence_possible({term, Token, false, _Columns}, Present) ->
    maps:get(Token, Present, false);
fts2_delta_presence_possible({term, _Token, true, _Columns}, _Present) ->
    true;
fts2_delta_presence_possible({phrase, Specs, _Columns}, Present) ->
    lists:all(
        fun
            ({Token, false, _Offset}) -> maps:get(Token, Present, false);
            ({_Prefix, true, _Offset}) -> true
        end,
        Specs
    );
fts2_delta_presence_possible({near, Items, _Distance, _Columns}, Present) ->
    lists:all(
        fun(Item) -> fts2_delta_presence_possible(Item, Present) end,
        Items
    );
fts2_delta_presence_possible({anchor, Child}, Present) ->
    fts2_delta_presence_possible(Child, Present);
fts2_delta_presence_possible({'and', A, B}, Present) ->
    fts2_delta_presence_possible(A, Present) andalso
        fts2_delta_presence_possible(B, Present);
fts2_delta_presence_possible({'or', A, B}, Present) ->
    fts2_delta_presence_possible(A, Present) orelse
        fts2_delta_presence_possible(B, Present);
fts2_delta_presence_possible({'not', A, _B}, Present) ->
    fts2_delta_presence_possible(A, Present);
fts2_delta_presence_possible(_Other, _Present) -> true.

fts2_delta_presence_hook(undefined, _Deltas) -> ok;
fts2_delta_presence_hook(Fun, Deltas) when is_function(Fun, 1) ->
    Fun({fts2, Deltas});
fts2_delta_presence_hook(Fun, _Deltas) when is_function(Fun, 0) ->
    Fun().

fts2_delta_affected_sources(Deltas) ->
    maps:from_list([
        {SourceId, true}
     || {_RowId, Delta} <- Deltas,
        SourceId <- [maps:get(source_id, Delta) | maps:get(retired_ids, Delta, [])]
    ]).

fts2_delta_live_documents(Deltas) ->
    fts2_delta_overlay_documents(#{}, Deltas).

fts2_delta_overlay_documents(Documents, Deltas) ->
    lists:foldl(
        fun({_RowId, Delta}, Acc) ->
            WithoutRetired = maps:without(maps:get(retired_ids, Delta, []), Acc),
            SourceId = maps:get(source_id, Delta),
            case maps:get(status, Delta) of
                live -> WithoutRetired#{SourceId => Delta};
                remove -> maps:remove(SourceId, WithoutRetired)
            end
        end,
        Documents,
        Deltas
    ).

fts2_delta_evaluate(Deltas, Schema, AST) ->
    Documents = fts2_delta_live_documents(Deltas),
    Dfs = fts2_delta_delta_dfs(Documents, Schema, AST),
    maps:fold(
        fun(SourceId, Document, Acc) ->
            Posting = maps:get(posting, Document, #{}),
            case fts2_delta_eval(AST, Posting, Schema) of
                false ->
                    Acc;
                {true, Positions, TermTfs, Terms} ->
                    Candidate = maps:get(candidate_record, Document),
                    GroupFields = maps:get(candidate_group_fields, Schema, []),
                    VersionField = maps:get(
                        candidate_version_field, Schema, undefined
                    ),
                    LogicalGroup = case GroupFields of
                        [] -> {source, SourceId};
                        _ -> {group, [
                            maps:get(Field, Candidate, undefined)
                         || Field <- GroupFields
                        ]}
                    end,
                    GroupVersion = case VersionField of
                        undefined -> 0;
                        _ -> maps:get(VersionField, Candidate, 0)
                    end,
                    Acc#{{delta, SourceId} => #fts2_match{
                        chunk_id = {delta, SourceId},
                        source_id = SourceId,
                        doc_length = maps:get(doc_length, Document),
                        tf = lists:sum(maps:values(TermTfs)),
                        term_stats = [
                            {Term, Tf, delta, maps:get(Term, Dfs, 0)}
                         || {Term, Tf} <- maps:to_list(TermTfs)
                        ],
                        terms = Terms,
                        match_positions = maps:to_list(Positions),
                        match_count = fts2_delta_position_count(Positions),
                        logical_group = LogicalGroup,
                        group_version = GroupVersion,
                        delta_document = Document
                    }}
            end
        end,
        #{},
        Documents
    ).

%% Dirty-tail Boolean evaluation must use the same logical-document grain as
%% the immutable group plane.  Positional leaves are still checked per chunk,
%% so phrase and NEAR never bridge a chunk boundary; only their verified
%% results are collapsed to the logical group.
fts2_delta_evaluate_grouped(Deltas, Schema, AST) ->
    Documents = fts2_delta_live_documents(Deltas),
    Groups = fts2_delta_document_groups(Documents, Schema),
    Dfs = fts2_delta_group_dfs(Groups, Schema, AST),
    fts2_delta_eval_grouped(AST, Groups, Schema, Dfs).

fts2_delta_eval_grouped({empty}, _Groups, _Schema, _Dfs) ->
    #{};
fts2_delta_eval_grouped({'and', A, B}, Groups, Schema, Dfs) ->
    fts2_search_intersect(
        fts2_delta_eval_grouped(A, Groups, Schema, Dfs),
        fts2_delta_eval_grouped(B, Groups, Schema, Dfs)
    );
fts2_delta_eval_grouped({'or', A, B}, Groups, Schema, Dfs) ->
    fts2_search_union(
        fts2_delta_eval_grouped(A, Groups, Schema, Dfs),
        fts2_delta_eval_grouped(B, Groups, Schema, Dfs)
    );
fts2_delta_eval_grouped({'not', A, B}, Groups, Schema, Dfs) ->
    Positive = fts2_delta_eval_grouped(A, Groups, Schema, Dfs),
    Negative = fts2_delta_eval_grouped(B, Groups, Schema, Dfs),
    maps:without(maps:keys(Negative), Positive);
fts2_delta_eval_grouped(Leaf, Groups, Schema, Dfs) ->
    maps:fold(
        fun(LogicalGroup, {Version, Documents}, Acc) ->
            Matches = lists:filtermap(
                fun({SourceId, Document}) ->
                    case fts2_delta_eval(
                        Leaf, maps:get(posting, Document, #{}), Schema
                    ) of
                        false -> false;
                        Result -> {true, {SourceId, Document, Result}}
                    end
                end,
                Documents
            ),
            case Matches of
                [] ->
                    Acc;
                [{AnchorSource, AnchorDocument, _} | _] ->
                    StatTerms = fts2_delta_group_stat_terms(
                        Leaf, Schema, Documents
                    ),
                    TermTfs = [{Term, lists:sum([
                        fts2_delta_term_tf(
                            Term, maps:get(posting, Document, #{})
                        )
                     || {_Source, Document} <- Documents
                    ])} || Term <- StatTerms],
                    MatchCount = lists:sum([
                        fts2_delta_position_count(Positions)
                     || {_Source, _Document,
                            {true, Positions, _Tfs, _Terms}} <- Matches
                    ]),
                    GroupLength = lists:sum([
                        maps:get(doc_length, Document, 0)
                     || {_Source, Document} <- Documents
                    ]),
                    Acc#{LogicalGroup => #fts2_match{
                        chunk_id = {delta, AnchorSource},
                        source_id = AnchorSource,
                        doc_length = GroupLength,
                        tf = lists:sum([Tf || {_Term, Tf} <- TermTfs]),
                        term_stats = [
                            {Term, Tf, delta, maps:get(Term, Dfs, 0)}
                         || {Term, Tf} <- TermTfs,
                            Tf > 0
                        ],
                        terms = lists:usort([
                            Token
                         || {{_Column, Token, _Prefix}, Tf} <- TermTfs,
                            Tf > 0
                        ]),
                        match_positions = lists:append([
                            maps:to_list(Positions)
                         || {_Source, _Document,
                                {true, Positions, _Tfs, _Terms}} <- Matches
                        ]),
                        match_count = MatchCount,
                        group_match_count = MatchCount,
                        logical_group = LogicalGroup,
                        group_version = Version,
                        delta_document = AnchorDocument
                    }}
            end
        end,
        #{},
        Groups
    ).

fts2_delta_document_groups(Documents, Schema) ->
    GroupFields = maps:get(candidate_group_fields, Schema, []),
    VersionField = maps:get(candidate_version_field, Schema, undefined),
    maps:fold(
        fun(SourceId, Document, Acc) ->
            Candidate = maps:get(candidate_record, Document),
            LogicalGroup = case GroupFields of
                [] -> {source, SourceId};
                _ -> {group, [
                    maps:get(Field, Candidate, undefined)
                 || Field <- GroupFields
                ]}
            end,
            Version = case VersionField of
                undefined -> 0;
                _ -> maps:get(VersionField, Candidate, 0)
            end,
            case maps:find(LogicalGroup, Acc) of
                error ->
                    Acc#{LogicalGroup => {Version, [{SourceId, Document}]}};
                {ok, {Older, _}} when Version > Older ->
                    Acc#{LogicalGroup => {Version, [{SourceId, Document}]}};
                {ok, {Version, Existing}} ->
                    Acc#{LogicalGroup => {Version,
                        lists:sort([{SourceId, Document} | Existing])}};
                {ok, {_Newer, _}} ->
                    Acc
            end
        end,
        #{},
        Documents
    ).

fts2_delta_group_dfs(Groups, Schema, AST) ->
    Terms = lists:usort(lists:append([
        fts2_delta_group_stat_terms(AST, Schema, Documents)
     || {_Version, Documents} <- maps:values(Groups)
    ])),
    maps:from_list([
        {Term, length([
            ok
         || {_Version, Documents} <- maps:values(Groups),
            lists:any(
                fun({_Source, Document}) ->
                    fts2_delta_term_tf(
                        Term, maps:get(posting, Document, #{})
                    ) > 0
                end,
                Documents
            )
        ])}
     || Term <- Terms
    ]).

fts2_delta_group_stat_terms(AST, Schema, Documents) ->
    lists:usort(lists:append([
        case Prefix of
            false -> [{Column, Token, false}];
            true -> lists:usort(lists:append([
                [
                    {Column, Actual, false}
                 || Actual <- maps:keys(maps:get(
                        Column, maps:get(posting, Document, #{}), #{}
                    )),
                    fts2_delta_binary_prefix(Actual, Token)
                ]
             || {_Source, Document} <- Documents
            ]))
        end
     || {Column, Token, Prefix} <- fts2_delta_ast_terms(AST, Schema)
    ])).

fts2_delta_delta_dfs(Documents, Schema, AST) ->
    Terms = lists:usort(fts2_delta_ast_terms(AST, Schema)),
    maps:from_list([
        {Term, length([
            ok
         || Document <- maps:values(Documents),
            fts2_delta_term_tf(Term, maps:get(posting, Document, #{})) > 0
        ])}
     || Term <- Terms
    ]).

fts2_delta_ast_terms({term, Token, Prefix, Columns}, Schema) ->
    [
        {Column, Token, Prefix}
     || Column <- fts2_delta_selector_ids(Columns, Schema)
    ];
fts2_delta_ast_terms({phrase, Specs, Columns}, Schema) ->
    lists:append([
        [{Column, Token, Prefix} || {Token, Prefix, _Offset} <- Specs]
     || Column <- fts2_delta_selector_ids(Columns, Schema)
    ]);
fts2_delta_ast_terms({near, Items, _Distance, _Columns}, Schema) ->
    lists:append([fts2_delta_ast_terms(Item, Schema) || Item <- Items]);
fts2_delta_ast_terms({anchor, Child}, Schema) -> fts2_delta_ast_terms(Child, Schema);
fts2_delta_ast_terms({_Op, A, B}, Schema) -> fts2_delta_ast_terms(A, Schema) ++ fts2_delta_ast_terms(B, Schema);
fts2_delta_ast_terms(_Other, _Schema) -> [].

fts2_delta_term_tf({Column, Token, false}, Posting) ->
    case maps:find(Token, maps:get(Column, Posting, #{})) of
        {ok, Entry} -> maps:get(count, Entry);
        error -> 0
    end;
fts2_delta_term_tf({Column, Prefix, true}, Posting) ->
    lists:sum([
        maps:get(count, Entry)
     || {Token, Entry} <- maps:to_list(maps:get(Column, Posting, #{})),
        fts2_delta_binary_prefix(Token, Prefix)
    ]).

fts2_delta_eval({empty}, _Posting, _Schema) -> false;
fts2_delta_eval({all_docs}, _Posting, _Schema) -> {true, #{}, #{}, []};
fts2_delta_eval({term, Token, Prefix, Columns}, Posting, Schema) ->
    Matches = lists:append([
        [
            {{Column, Actual, false}, Actual, maps:get(positions, Entry)}
         || {Actual, Entry} <- maps:to_list(maps:get(Column, Posting, #{})),
            fts2_delta_token_matches(Actual, Token, Prefix)
        ]
     || Column <- fts2_delta_selector_ids(Columns, Schema)
    ]),
    case Matches of
        [] -> false;
        _ ->
            TermTfs = maps:from_list([
                {Term, length(Positions)}
             || {Term, _Actual, Positions} <- Matches
            ]),
            Positions = lists:append([Ps || {_Term, _Actual, Ps} <- Matches]),
            {true, #{Token => Positions}, TermTfs,
                lists:usort([Actual || {_Term, Actual, _Ps} <- Matches])}
    end;
fts2_delta_eval({phrase, Specs, Columns}, Posting, Schema) ->
    Starts = lists:append([
        fts2_delta_phrase_starts(Posting, Column, Specs)
     || Column <- fts2_delta_selector_ids(Columns, Schema)
    ]),
    case Starts of
        [] -> false;
        _ ->
            Terms = fts2_delta_phrase_term_tfs(Posting, Schema, Specs, Columns),
            {true, #{phrase => Starts}, Terms,
                [Token || {Token, _Prefix, _Offset} <- Specs]}
    end;
fts2_delta_eval({near, Items, Distance, Columns}, Posting, Schema) ->
    Starts = lists:append([
        fts2_delta_near_positions(
            [fts2_delta_item_spans(Posting, Schema, Column, Item) || Item <- Items],
            Distance
        )
     || Column <- fts2_delta_selector_ids(Columns, Schema)
    ]),
    case Starts of
        [] -> false;
        _ ->
            Tfs = maps:from_list([
                {Term, fts2_delta_term_tf(Term, Posting)}
             || Term <- fts2_delta_ast_terms({near, Items, Distance, Columns}, Schema)
            ]),
            {true, #{near => Starts}, Tfs,
                lists:usort([Token || {_Column, Token, _Prefix} <- maps:keys(Tfs)])}
    end;
fts2_delta_eval({anchor, Child}, Posting, Schema) ->
    case fts2_delta_eval(Child, Posting, Schema) of
        {true, Positions, Tfs, Terms} ->
            case lists:member(0, fts2_delta_flatten_positions(Positions)) of
                true -> {true, Positions, Tfs, Terms};
                false -> false
            end;
        false -> false
    end;
fts2_delta_eval({'and', A, B}, Posting, Schema) ->
    case {fts2_delta_eval(A, Posting, Schema), fts2_delta_eval(B, Posting, Schema)} of
        {{true, PA, TA, TermsA}, {true, PB, TB, TermsB}} ->
            {true, maps:merge(PA, PB), maps:merge(TA, TB),
                lists:usort(TermsA ++ TermsB)};
        _ -> false
    end;
fts2_delta_eval({'or', A, B}, Posting, Schema) ->
    case {fts2_delta_eval(A, Posting, Schema), fts2_delta_eval(B, Posting, Schema)} of
        {{true, PA, TA, TermsA}, {true, PB, TB, TermsB}} ->
            {true, maps:merge(PA, PB), maps:merge(TA, TB),
                lists:usort(TermsA ++ TermsB)};
        {{true, PA, TA, TermsA}, false} -> {true, PA, TA, TermsA};
        {false, {true, PB, TB, TermsB}} -> {true, PB, TB, TermsB};
        _ -> false
    end;
fts2_delta_eval({'not', A, B}, Posting, Schema) ->
    case {fts2_delta_eval(A, Posting, Schema), fts2_delta_eval(B, Posting, Schema)} of
        {{true, PA, TA, TermsA}, false} -> {true, PA, TA, TermsA};
        _ -> false
    end.

fts2_delta_phrase_term_tfs(Posting, Schema, Specs, Columns) ->
    maps:from_list([
        begin
            Term = {Column, Token, Prefix},
            {Term, fts2_delta_term_tf(Term, Posting)}
        end
     || Column <- fts2_delta_selector_ids(Columns, Schema),
        {Token, Prefix, _Offset} <- Specs
    ]).

fts2_delta_phrase_starts(_Posting, _Column, []) -> [];
fts2_delta_phrase_starts(Posting, Column, [{First, FirstPrefix, FirstOffset} | Rest]) ->
    FirstPositions = fts2_delta_token_positions(Posting, Column, First, FirstPrefix),
    [
        Position - FirstOffset
     || Position <- FirstPositions,
        fts2_delta_phrase_rest(Posting, Column, Rest, Position - FirstOffset)
    ].

fts2_delta_phrase_rest(_Posting, _Column, [], _Start) -> true;
fts2_delta_phrase_rest(Posting, Column, [{Token, Prefix, Offset} | Rest], Start) ->
    lists:member(Start + Offset, fts2_delta_token_positions(Posting, Column, Token, Prefix))
        andalso fts2_delta_phrase_rest(Posting, Column, Rest, Start).

fts2_delta_item_spans(Posting, _Schema, Column, {term, Token, Prefix, _Columns}) ->
    [{P, P} || P <- fts2_delta_token_positions(Posting, Column, Token, Prefix)];
fts2_delta_item_spans(Posting, _Schema, Column, {phrase, Specs, _Columns}) ->
    Starts = fts2_delta_phrase_starts(Posting, Column, Specs),
    Last = lists:max([Offset || {_Token, _Prefix, Offset} <- Specs]),
    [{Start, Start + Last} || Start <- Starts];
fts2_delta_item_spans(Posting, Schema, Column, {anchor, Item}) ->
    [Span || {Start, _End} = Span <- fts2_delta_item_spans(Posting, Schema, Column, Item),
        Start =:= 0];
fts2_delta_item_spans(_Posting, _Schema, _Column, _Item) -> [].

fts2_delta_near_positions([], _Distance) -> [];
fts2_delta_near_positions([[] | _], _Distance) -> [];
fts2_delta_near_positions([First | Rest], Distance) ->
    [
        Start
     || {Start, _End} = Span <- First,
        fts2_delta_near_match([Span], Rest, Distance)
    ].

fts2_delta_near_match(_Chosen, [], _Distance) -> true;
fts2_delta_near_match(Chosen, [Spans | Rest], Distance) ->
    lists:any(
        fun(Span) ->
            lists:all(fun(Other) -> fts2_delta_span_distance(Span, Other) =< Distance end,
                Chosen) andalso fts2_delta_near_match([Span | Chosen], Rest, Distance)
        end,
        Spans
    ).

fts2_delta_span_distance({_SA, EA}, {SB, _EB}) when EA < SB -> SB - EA - 1;
fts2_delta_span_distance({SA, _EA}, {_SB, EB}) when EB < SA -> SA - EB - 1;
fts2_delta_span_distance(_A, _B) -> 0.

fts2_delta_token_positions(Posting, Column, Token, false) ->
    case maps:find(Token, maps:get(Column, Posting, #{})) of
        {ok, Entry} -> maps:get(positions, Entry);
        error -> []
    end;
fts2_delta_token_positions(Posting, Column, Prefix, true) ->
    lists:append([
        maps:get(positions, Entry)
     || {Token, Entry} <- maps:to_list(maps:get(Column, Posting, #{})),
        fts2_delta_binary_prefix(Token, Prefix)
    ]).

fts2_delta_token_matches(Actual, Token, false) -> Actual =:= Token;
fts2_delta_token_matches(Actual, Prefix, true) -> fts2_delta_binary_prefix(Actual, Prefix).

fts2_delta_binary_prefix(Binary, Prefix) when byte_size(Binary) >= byte_size(Prefix) ->
    binary:part(Binary, 0, byte_size(Prefix)) =:= Prefix;
fts2_delta_binary_prefix(_Binary, _Prefix) -> false.

fts2_delta_selector_ids(all, Schema) -> lists:seq(0, length(maps:get(columns, Schema)) - 1);
fts2_delta_selector_ids({not_columns, Excluded}, Schema) ->
    fts2_delta_selector_ids([C || C <- maps:get(columns, Schema), not lists:member(C, Excluded)],
        Schema);
fts2_delta_selector_ids(Columns, Schema) ->
    Names = maps:get(columns, Schema),
    [Index || {Name, Index} <- lists:zip(Names, lists:seq(0, length(Names) - 1)),
        lists:member(Name, Columns)].

fts2_delta_position_count(Value) when is_map(Value) ->
    lists:sum([fts2_delta_position_count(V) || V <- maps:values(Value)]);
fts2_delta_position_count(Value) when is_list(Value) -> length(Value);
fts2_delta_position_count(_Value) -> 0.

fts2_delta_flatten_positions(Value) when is_map(Value) ->
    lists:append([fts2_delta_flatten_positions(V) || V <- maps:values(Value)]);
fts2_delta_flatten_positions(Value) when is_list(Value) -> Value;
fts2_delta_flatten_positions(_Value) -> [].


%% ===========================================================================
%% INTERNAL FTS-2 PHRASE
%% ===========================================================================

%% Frequent-bigram accelerator with an exact positional fallback.

fts2_phrase_evaluate(
    bigram,
    Bookie,
    Schema,
    Root,
    [{First, false, _}, {Second, false, _}],
    Column,
    Wanted
) ->
    case fts2_phrase_read_bundle(Bookie, Schema, Root, Column, First, Second) of
        not_found -> {positions, Wanted};
        Bundle ->
            {matches,
                fts2_phrase_matches(
                    fts2_codec_bigram_entries(Bundle),
                    Bundle,
                    First,
                    Second,
                    Column,
                    Wanted,
                    true
                )}
    end;
fts2_phrase_evaluate(_Strategy, _Bookie, _Schema, _Root, _Specs, _Column, Wanted) ->
    {positions, Wanted}.

fts2_phrase_fast(
    Bookie,
    Schema,
    Root,
    [{First, false, _}, {Second, false, _}],
    Columns,
    Opts,
    all
) ->
    ColumnIds = fts2_phrase_selector_ids(Columns, Schema),
    Window = maps:get(offset, Opts, 0) + maps:get(limit, Opts, 10000),
    case
        {
            ColumnIds,
            maps:get(rank, Opts, none),
            maps:get(impact_facet, Opts, nil),
            Window =< 256
        }
    of
        {[Column], bm25, nil, true} ->
            %% Bigram champions are chunk-grain accelerators.  Document-grain
            %% ranking verifies candidates on the positional chunk plane, then
            %% scores the matching groups from the complete group term planes.
            fts2_phrase_fast_from_terms(
                Bookie, Schema, Root, Column, First, Second, Opts
            );
        _ ->
            no
    end;
fts2_phrase_fast(_Bookie, _Schema, _Root, _Specs, _Columns, _Opts, _Wanted) ->
    no.

fts2_phrase_ranked_champions(
    Bundle, First, Second, Column, Root, NeedPositions, Window
) ->
    DocCount = erlang:max(maps:get(chunk_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    FirstDf = maps:get(first_df, Bundle),
    SecondDf = maps:get(second_df, Bundle),
    FirstIdf = fts2_build_bm25_idf(DocCount, FirstDf),
    SecondIdf = fts2_build_bm25_idf(DocCount, SecondDf),
    Champions = fts2_search_impact_window(
        maps:get(champions, Bundle),
        Window,
        fun({_ChunkId, _GroupId, _SourceId, Length, _PhraseTf, FirstTf,
                SecondTf}) ->
            fts2_search_bm25(FirstTf, FirstIdf, Avg, Length) +
                fts2_search_bm25(SecondTf, SecondIdf, Avg, Length)
        end
    ),
    Positions = case NeedPositions of
        true -> fts2_codec_bigram_positions(
            Bundle, [element(1, Entry) || Entry <- Champions]
        );
        false -> #{}
    end,
    [
        begin
            Starts = maps:get(ChunkId, Positions, []),
            #fts2_match{
                chunk_id = ChunkId,
                group_id = GroupId,
                source_id = SourceId,
                doc_length = Length,
                tf = FirstTf + SecondTf,
                term_stats = [
                    {{Column, First}, FirstTf, base, FirstDf},
                    {{Column, Second}, SecondTf, base, SecondDf}
                ],
                terms = [First, Second],
                columns = [{{Column, First}, Starts}],
                match_positions = [{phrase, Starts}],
                match_count = PhraseTf,
                score =
                    fts2_search_bm25(FirstTf, FirstIdf, Avg, Length) +
                    fts2_search_bm25(SecondTf, SecondIdf, Avg, Length)
            }
        end
     || {ChunkId, GroupId, SourceId, Length, PhraseTf, FirstTf, SecondTf} <-
            Champions
    ].

fts2_phrase_fast_from_terms(
    Bookie, Schema, Root, Column, First, Second, Opts
) ->
    NeedPositions = maps:get(return_positions, Opts, false),
    case fts2_search_positional_pair(
        Bookie, Schema, Root, Column, First, Second, phrase, NeedPositions
    ) of
        not_found ->
            {ranked_champions, [], 0};
        {FirstHeader, SecondHeader, Rows} ->
            fts2_search_rank_positional_groups(
                Bookie, Schema, Root, Column, First, Second,
                FirstHeader, SecondHeader, Rows, phrase
            )
    end.

fts2_search_rank_positional_groups(
    Bookie, #{index := Bucket}, Root, Column, First, Second,
    FirstHeader, SecondHeader, Rows, Kind
) ->
    Generation = maps:get(generation, Root),
    {FirstKey, _} = fts2_codec_term_key(Generation, Column, First),
    {SecondKey, _} = fts2_codec_term_key(Generation, Column, Second),
    [{ok, FirstGroupsValue}, {ok, SecondGroupsValue}] =
        leveled_fts_residency:headonly_many(
            Bookie,
            Bucket,
            [
                {fts2_codec_term_plane_key(FirstKey, boolean), <<"g">>},
                {fts2_codec_term_plane_key(SecondKey, boolean), <<"g">>}
            ]
        ),
    FirstGroups = maps:from_list([
        {element(2, Entry), Entry}
     || Entry <- fts2_codec_decode_plane(FirstGroupsValue)
    ]),
    SecondGroups = maps:from_list([
        {element(2, Entry), Entry}
     || Entry <- fts2_codec_decode_plane(SecondGroupsValue)
    ]),
    PositionalGroups = lists:foldl(
        fun({ChunkId, GroupId, SourceId, _Length, _FirstTf, _SecondTf,
                MatchCount, Starts}, Acc) ->
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => {
                        ChunkId, SourceId, MatchCount, Starts
                    }};
                {ok, {AnchorChunk, AnchorSource, ExistingCount,
                        ExistingStarts}} ->
                    Acc#{GroupId => {
                        AnchorChunk, AnchorSource,
                        ExistingCount + MatchCount,
                        ExistingStarts ++ Starts
                    }}
            end
        end,
        #{},
        Rows
    ),
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    FirstDf = maps:get(group_df, FirstHeader),
    SecondDf = maps:get(group_df, SecondHeader),
    FirstIdf = fts2_build_bm25_idf(DocCount, FirstDf),
    SecondIdf = fts2_build_bm25_idf(DocCount, SecondDf),
    Matches = maps:fold(
        fun(GroupId, {ChunkId, SourceId, MatchCount, Starts}, Acc) ->
            {_, GroupId, _, GroupLength, FirstTf, _} =
                maps:get(GroupId, FirstGroups),
            {_, GroupId, _, GroupLength, SecondTf, _} =
                maps:get(GroupId, SecondGroups),
            [#fts2_match{
                chunk_id = ChunkId,
                group_id = GroupId,
                source_id = SourceId,
                doc_length = GroupLength,
                tf = FirstTf + SecondTf,
                term_stats = [
                    {{Column, First}, FirstTf, base, FirstDf},
                    {{Column, Second}, SecondTf, base, SecondDf}
                ],
                terms = [First, Second],
                columns = [{{Column, First}, Starts}],
                match_positions = [{Kind, Starts}],
                match_count = MatchCount,
                group_match_count = MatchCount,
                score =
                    fts2_search_bm25(FirstTf, FirstIdf, Avg, GroupLength) +
                    fts2_search_bm25(SecondTf, SecondIdf, Avg, GroupLength)
            } | Acc]
        end,
        [],
        PositionalGroups
    ),
    {ranked_champions, Matches, map_size(PositionalGroups)}.

fts2_phrase_adjacent_positions(FirstPositions, SecondPositions) ->
    fts2_phrase_adjacent_positions(
        FirstPositions, SecondPositions, []
    ).

fts2_phrase_adjacent_positions([], _Seconds, Acc) ->
    lists:reverse(Acc);
fts2_phrase_adjacent_positions(_Firsts, [], Acc) ->
    lists:reverse(Acc);
fts2_phrase_adjacent_positions(
    [First | FirstRest] = Firsts,
    [Second | SecondRest] = Seconds,
    Acc
) ->
    case (First + 1) - Second of
        Difference when Difference < 0 ->
            fts2_phrase_adjacent_positions(FirstRest, Seconds, Acc);
        Difference when Difference > 0 ->
            fts2_phrase_adjacent_positions(Firsts, SecondRest, Acc);
        0 ->
            fts2_phrase_adjacent_positions(
                FirstRest, SecondRest, [First | Acc]
            )
    end.

fts2_phrase_read_bundle(Bookie, #{index := Bucket}, Root, Column, First, Second) ->
    {Key, _} = fts2_codec_bigram_key(
        maps:get(generation, Root), Column, First, Second
    ),
    case leveled_fts_residency:headonly(Bookie, Bucket, Key, <<"x">>) of
        {ok, Value} -> fts2_codec_decode_bigram(Value);
        not_found -> not_found
    end.

fts2_phrase_matches(Entries, Bundle, First, Second, Column, Wanted, NeedPositions) ->
    Positions = case NeedPositions of
        true -> fts2_codec_bigram_positions(Bundle);
        false -> #{}
    end,
    FirstDf = maps:get(first_df, Bundle),
    SecondDf = maps:get(second_df, Bundle),
    lists:foldl(
        fun(
            {ChunkId, GroupId, SourceId, Length, PhraseTf, FirstTf, SecondTf},
            Acc
        ) ->
            case fts2_phrase_wanted(SourceId, Wanted) of
                false ->
                    Acc;
                true ->
                    Starts = maps:get(ChunkId, Positions, []),
                    FirstTerm = {Column, First},
                    SecondTerm = {Column, Second},
                    Acc#{ChunkId => #fts2_match{
                        chunk_id = ChunkId,
                        group_id = GroupId,
                        source_id = SourceId,
                        doc_length = Length,
                        tf = FirstTf + SecondTf,
                        term_stats = [
                            {FirstTerm, FirstTf, base, FirstDf},
                            {SecondTerm, SecondTf, base, SecondDf}
                        ],
                        terms = [First, Second],
                        columns = [{{Column, First}, Starts}],
                        match_positions = [{phrase, Starts}],
                        match_count = PhraseTf
                    }}
            end
        end,
        #{},
        Entries
    ).

fts2_phrase_selector_ids(all, Schema) ->
    lists:seq(0, length(maps:get(columns, Schema)) - 1);
fts2_phrase_selector_ids({not_columns, Excluded}, Schema) ->
    fts2_phrase_selector_ids(
        [C || C <- maps:get(columns, Schema), not lists:member(C, Excluded)],
        Schema
    );
fts2_phrase_selector_ids(Columns, Schema) ->
    Names = maps:get(columns, Schema),
    [
        Index
     || {Name, Index} <- lists:zip(Names, lists:seq(0, length(Names) - 1)),
        lists:member(Name, Columns)
    ].

fts2_phrase_wanted(_SourceId, all) -> true;
fts2_phrase_wanted(SourceId, {all_except, Excluded}) -> not maps:is_key(SourceId, Excluded);
fts2_phrase_wanted(SourceId, Wanted) -> maps:is_key(SourceId, Wanted).


%% ===========================================================================
%% INTERNAL FTS-2 SEARCH
%% ===========================================================================

%% FTS2 generation reader and chunk-grained evaluator.

-define(MAX_RETURN_POSITIONS, 4096).

fts2_search_root(Bookie, #{index := Bucket, fingerprint := Fingerprint}) ->
    {Key, SubKey} = fts2_codec_root_key(),
    case leveled_fts_residency:headonly(Bookie, Bucket, Key, SubKey) of
        {ok, Value} ->
            Root = fts2_codec_decode_root(Value),
            case maps:get(fingerprint, Root) of
                Fingerprint -> {ok, Root};
                _ -> not_found
            end;
        not_found ->
            not_found
    end.

fts2_search_search(Bookie, Schema, Root, AST, Opts) ->
    fts2_search_run(Bookie, Schema, Root, AST, Opts, all, []).

fts2_search_search_dirty(Bookie, Schema, Root, AST, Opts, Hook) ->
    Deltas = fts2_delta_read_for_search(Bookie, Schema, AST, Hook),
    Affected = fts2_delta_affected_sources(Deltas),
    fts2_search_run(
        Bookie,
        Schema,
        Root,
        AST,
        Opts,
        {all_except, Affected},
        Deltas
    ).

fts2_search_posting_read(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    fts2_search_run(
        Bookie,
        Schema,
        Root,
        AST,
        Opts#{
            offset => 0, limit => length(SourceIds), return_positions => true
        },
        maps:from_list([{SourceId, true} || SourceId <- SourceIds]),
        []
    ).

fts2_search_posting_read_dirty(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    Deltas = fts2_delta_read_for_search(Bookie, Schema, AST, undefined),
    Wanted = maps:from_list([{SourceId, true} || SourceId <- SourceIds]),
    fts2_search_run(
        Bookie,
        Schema,
        Root,
        AST,
        Opts#{
            offset => 0, limit => length(SourceIds), return_positions => true
        },
        Wanted,
        Deltas
    ).

fts2_search_export_documents(Bookie, #{index := Bucket} = Schema, Root) ->
    GroupCount = maps:get(group_count, Root),
    GroupIds = case GroupCount of
        0 -> [];
        _ -> lists:seq(0, GroupCount - 1)
    end,
    Identities = fts2_search_read_identities(Bookie, Schema, Root, GroupIds),
    {Documents0, ByChunk} = maps:fold(
        fun(_GroupId, Group, {Docs, Chunks}) ->
            lists:foldl(
                fun(Chunk, {DocAcc, ChunkAcc}) ->
                    SourceId = maps:get(source_id, Chunk),
                    Document = maps:with(
                        [
                            source_id,
                            doc_key,
                            doc_version,
                            doc_length,
                            candidate_record,
                            hit_record
                        ],
                        Chunk
                    ),
                    {
                        DocAcc#{SourceId => Document#{
                            status => live, retired_ids => [], posting => #{}
                        }},
                        ChunkAcc#{maps:get(chunk_id, Chunk) => SourceId}
                    }
                end,
                {Docs, Chunks},
                maps:get(chunks, Group)
            )
        end,
        {#{}, #{}},
        Identities
    ),
    Generation = maps:get(generation, Root),
    Prefix = <<"f2:b:">>,
    Fold = fun
        (B, {Key, <<"h">>}, Value, Acc) when B =:= Bucket ->
            case Key of
                <<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
                    Row = maps:get({Column, Token}, Acc, #{}),
                    Acc#{{Column, Token} => Row#{
                        header => fts2_codec_decode_header(Value)
                    }};
                _ -> Acc
            end;
        (B, {Key, <<"b">>}, Value, Acc) when B =:= Bucket ->
            case Key of
                <<"f2:b:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
                    Row = maps:get({Column, Token}, Acc, #{}),
                    Acc#{{Column, Token} => Row#{
                        entries => fts2_codec_decode_plane(Value)
                    }};
                _ -> Acc
            end;
        (B, {Key, <<"p">>}, Value, Acc) when B =:= Bucket ->
            case Key of
                <<"f2:p:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
                    Row = maps:get({Column, Token}, Acc, #{}),
                    Acc#{{Column, Token} => Row#{
                        positions => fts2_codec_decode_positions(Value)
                    }};
                _ -> Acc
            end;
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Prefix, <<>>}, {<<"f2:u">>, <<255>>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    TermRows = Runner(),
    maps:fold(
        fun({Column, Token}, Row, Docs) ->
            Header = maps:get(header, Row, #{}),
            Positions = case maps:find(positions, Row) of
                {ok, StoredPositions} -> StoredPositions;
                error ->
                    {TermKey, _} = fts2_codec_term_key(
                        Generation, Column, Token
                    ),
                    fts2_search_read_positions(
                        Bookie, Bucket, TermKey, Header
                    )
            end,
            Entries = case maps:find(entries, Row) of
                {ok, StoredEntries} -> StoredEntries;
                error -> case fts2_codec_header_has_entries(Header) of
                    true -> fts2_codec_header_entries(Header);
                    false -> []
                end
            end,
            lists:foldl(
                fun(
                    {ChunkId, _GroupId, _StoredSourceId, _Length, Tf, _DenseId},
                    Acc
                ) ->
                    SourceId = maps:get(ChunkId, ByChunk),
                    Document = maps:get(SourceId, Acc),
                    Posting = maps:get(posting, Document),
                    Tokens = maps:get(Column, Posting, #{}),
                    Entry = #{
                        count => Tf,
                        positions => maps:get(ChunkId, Positions, [])
                    },
                    Acc#{SourceId => Document#{posting => Posting#{
                        Column => Tokens#{Token => Entry}
                    }}}
                end,
                Docs,
                Entries
            )
        end,
        Documents0,
        TermRows
    ).

fts2_search_lookup_documents(Bookie, Schema, SourceIds) ->
    Existing = case fts2_search_root(Bookie, Schema) of
        {ok, Root} -> fts2_search_export_documents(Bookie, Schema, Root);
        not_found -> #{}
    end,
    Current = fts2_delta_overlay_documents(
        Existing, fts2_delta_read(Bookie, Schema)
    ),
    maps:with(SourceIds, Current).

fts2_search_run(Bookie, Schema, Root, AST, Opts, WantedSources, Deltas) ->
    case maps:get(count_only, Opts, false) of
        true ->
            fts2_search_count_only(
                Bookie, Schema, Root, AST, Opts, WantedSources, Deltas
            );
        false ->
            fts2_search_page(
                Bookie, Schema, Root, AST, Opts, WantedSources, Deltas
            )
    end.

%% Count-only serving settles a facet without scoring, ordering, positions or
%% identity hydration. The common uniform facet is proved from the immutable
%% root and returns a sentinel so the caller can use the page call's exact
%% grouped count. A selective facet becomes an ordinary verbatim conjunction.
fts2_search_count_only(Bookie, Schema, Root, AST, Opts, WantedSources, Deltas) ->
    fts2_search_check_cancellation(),
    Facet = maps:get(impact_facet, Opts, nil),
    case fts2_search_facet_uniform(Schema, Root, Facet) of
        true ->
            {ok, #{hits => [], count => undefined, count_kind => grouped,
                facet_universal => true}};
        false ->
            Count = fts2_search_count_matches(
                Bookie, Schema, Root,
                fts2_search_facet_ast(Schema, Facet, AST),
                WantedSources, Deltas
            ),
            {ok, #{hits => [], count => Count, count_kind => grouped,
                facet_universal => false}}
    end.

fts2_search_facet_uniform(_Schema, _Root, nil) ->
    true;
fts2_search_facet_uniform(_Schema, undefined, _Facet) ->
    false;
fts2_search_facet_uniform(Schema, Root, Facet) when is_list(Facet) ->
    Fields = maps:get(candidate_filter_fields, Schema, []),
    Domain = maps:get(facet_domain, Root, #{}),
    length(Fields) =:= length(Facet) andalso
        lists:all(
            fun({{Column, _Field}, Value}) ->
                maps:get(Column, Domain, mixed) =:= {uniform, Value}
            end,
            lists:zip(Fields, Facet)
        );
fts2_search_facet_uniform(_Schema, _Root, _Facet) ->
    false.

fts2_search_facet_ast(_Schema, nil, AST) ->
    AST;
fts2_search_facet_ast(Schema, Facet, AST) when is_list(Facet) ->
    Fields = maps:get(candidate_filter_fields, Schema, []),
    case length(Fields) =:= length(Facet) of
        false ->
            AST;
        true ->
            lists:foldl(
                fun
                    ({{Column, _Field}, Value}, Acc) when is_binary(Value) ->
                        {'and', Acc, {term, Value, false, [Column]}};
                    (_Unsupported, Acc) ->
                        Acc
                end,
                AST,
                lists:zip(Fields, Facet)
            )
    end.

fts2_search_count_matches(Bookie, Schema, Root, AST, WantedSources, Deltas) ->
    Base0 = case Root of
        undefined -> #{};
        _ -> fts2_search_eval_grouped(
            Bookie, Schema, Root, AST, false, WantedSources
        )
    end,
    Base = case Deltas of
        [] -> Base0;
        _ -> fts2_search_enrich_base_groups(Bookie, Schema, Root, Base0)
    end,
    DeltaMatches = maps:filter(
        fun(_Key, #fts2_match{source_id = SourceId}) ->
            fts2_search_wanted_delta(SourceId, WantedSources)
        end,
        fts2_delta_evaluate_grouped(Deltas, Schema, AST)
    ),
    length(fts2_search_collapse_scored_groups(
        fts2_search_score_matches(maps:merge(Base, DeltaMatches), undefined, false),
        false
    )).

fts2_search_page(Bookie, Schema, Root, AST, Opts, WantedSources, Deltas) ->
    fts2_search_check_cancellation(),
    NeedPositions = maps:get(return_positions, Opts, false),
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    FastOrBase = case Root of
        undefined -> {base, #{}, undefined};
        _ ->
            case fts2_search_fast_single_term(
                Bookie, Schema, Root, AST, Opts, WantedSources, Deltas
            ) of
                {ranked_champions, FastMatches, FastCount} ->
                    {ranked_champions, FastMatches, FastCount};
                {ok, FastMatches, FastCount} ->
                    {base, FastMatches, FastCount};
                no ->
                    {base,
                        case maps:get(grouping, Opts, grouped) of
                            grouped -> fts2_search_eval_grouped(
                                Bookie, Schema, Root, AST, NeedPositions,
                                WantedSources
                            );
                            ungrouped -> fts2_search_eval(
                                Bookie, Schema, Root, AST, NeedPositions,
                                WantedSources
                            )
                        end,
                        undefined}
            end
    end,
    case FastOrBase of
        {ranked_champions, [], 0} ->
            case maps:get(return_count, Opts, false) of
                true -> {ok, #{hits => [], count => 0,
                    count_kind => fts2_search_count_kind(Opts)}};
                false -> {ok, []}
            end;
        _ ->
    {Scored0, BaseCountHint} = case FastOrBase of
        {ranked_champions, ChampionMatches, ChampionCount} ->
            {ChampionMatches, ChampionCount};
        {base, BaseMatches0, CountHint} ->
            BaseMatches = case Deltas of
                [] -> BaseMatches0;
                _ -> fts2_search_enrich_base_groups(
                    Bookie, Schema, Root, BaseMatches0
                )
            end,
            DeltaMatches0 = case maps:get(grouping, Opts, grouped) of
                grouped -> fts2_delta_evaluate_grouped(Deltas, Schema, AST);
                ungrouped -> fts2_delta_evaluate(Deltas, Schema, AST)
            end,
            DeltaMatches = maps:filter(
                fun(_Key, #fts2_match{source_id = SourceId}) ->
                    fts2_search_wanted_delta(SourceId, WantedSources)
                end,
                DeltaMatches0
            ),
            Matches0 = maps:merge(BaseMatches, DeltaMatches),
            ScoreRoot = (fts2_search_score_root(Root, Deltas, Schema))#{
                scoring_grouping => maps:get(grouping, Opts, grouped)
            },
            ScoredMatches = fts2_search_score_matches(
                Matches0, ScoreRoot, Ranked
            ),
            GroupedMatches = case maps:get(grouping, Opts, grouped) of
                grouped -> fts2_search_collapse_scored_groups(
                    ScoredMatches, Ranked
                );
                ungrouped -> ScoredMatches
            end,
            {GroupedMatches, CountHint}
    end,
    Count0 = case BaseCountHint of
        ExactCount when is_integer(ExactCount), Deltas =:= [] -> ExactCount;
        _ -> length(Scored0)
    end,
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    PrePage = fts2_search_can_page_before_identity(Opts, Ranked),
    {Scored, PageOffset} = case PrePage of
        true -> fts2_search_prepage_matches(Scored0, Ranked, Offset, Limit);
        false -> {Scored0, Offset}
    end,
    IdentityRequests = [
        case Match of
            #fts2_match{group_id = undefined} -> undefined;
            #fts2_match{group_id = GroupId, chunk_id = ChunkId} ->
                {GroupId, ChunkId}
        end
     || Match <- Scored
    ],
    TieFields = maps:get(rank_tie_fields, Opts, []),
    {Hits, Count} = fts2_search_serve(
        Bookie, Schema, Root, Scored, IdentityRequests, Opts, Ranked, PrePage,
        Count0, PageOffset, Limit, TieFields
    ),
    case maps:get(return_count, Opts, false) of
        true -> {ok, #{hits => Hits, count => Count,
            count_kind => fts2_search_count_kind(Opts)}};
        false -> {ok, Hits}
    end
    end.

fts2_search_count_kind(#{grouping := ungrouped}) -> raw;
fts2_search_count_kind(_Opts) -> grouped.

%% Two-phase served path. Phase one projects only the tie columns from v4
%% identity pages, orders the bounded score/tie window, and returns stable
%% identity coordinates. Public records are materialised by hydrate_page/3.
%%
%% Fresh generations assign group ids in native group-key order. When the
%% caller's tie fields are exactly that group key, the group id is already the
%% required deterministic tie ordinal. Select the final addresses without an
%% identity-page read; phase two still hydrates exactly the returned page.
fts2_search_serve(
    _Bookie, _Schema, Root, Scored, Requests, Opts, true, PrePage,
    Count0, PageOffset, Limit, TieFields
) when Root =/= undefined,
    map_get(page_only, Opts) =:= true,
    map_get(group_order, Root) =:= native,
    map_get(group_tie_fields, Root) =:= TieFields
->
    Decorated = [
        {{-Match#fts2_match.score, GroupId}, Match, Request}
     || {Match, Request = {GroupId, _ChunkId}} <- lists:zip(Scored, Requests)
    ],
    Ordered = lists:sort(
        fun({KeyA, _MatchA, _ReqA}, {KeyB, _MatchB, _ReqB}) ->
            KeyA =< KeyB
        end,
        Decorated
    ),
    Window = lists:sublist(
        fts2_search_drop(PageOffset, Ordered), Limit
    ),
    Hits = [
        %% hydrate_page/3 replaces this private placeholder with the durable
        %% document key before the hit leaves AshLeveled.
        fts2_search_page_entry(Match, Request, <<>>, Opts)
     || {_Key, Match, Request} <- Window
    ],
    Count = case PrePage of true -> Count0; false -> length(Ordered) end,
    {Hits, Count};
fts2_search_serve(
    Bookie, Schema, Root, Scored, Requests, Opts, Ranked, PrePage,
    Count0, PageOffset, Limit, TieFields
) when Root =/= undefined, map_get(page_only, Opts) =:= true ->
    Handles = fts2_search_identity_page_handles(
        Bookie, Schema, Root, Requests
    ),
    Ordinals = maps:map(
        fun(_Page, Handle) ->
            fts2_codec_identity_page_ordinals(Handle, TieFields)
        end,
        Handles
    ),
    case Ranked of
        false ->
            %% The unranked pre-page is already source-key ordered and has
            %% applied offset/limit. Preserve that order while projecting the
            %% stable address and candidate key from the v4 identity page.
            Hits = lists:filtermap(
                fun({Match, Request}) ->
                    case fts2_search_serve_probe(
                        Handles, Ordinals, Root, Request, Match, TieFields
                    ) of
                        undefined -> false;
                        {DocKey, _TieValues} ->
                            {true, fts2_search_page_entry(
                                Match, Request, DocKey, Opts
                            )}
                    end
                end,
                lists:zip(Scored, Requests)
            ),
            {Hits, Count0};
        true ->
            Decorated = lists:filtermap(
                fun({Match, Request}) ->
                    case fts2_search_serve_probe(
                        Handles, Ordinals, Root, Request, Match, TieFields
                    ) of
                        undefined ->
                            false;
                        {DocKey, TieValues} ->
                            {true, {{-Match#fts2_match.score,
                                fts2_search_serve_tie(
                                    TieFields, TieValues, DocKey
                                )}, Match, Request, DocKey}}
                    end
                end,
                lists:zip(Scored, Requests)
            ),
            Ordered = lists:sort(
                fun({KeyA, _MatchA, _ReqA, _DocA},
                    {KeyB, _MatchB, _ReqB, _DocB}) -> KeyA =< KeyB end,
                Decorated
            ),
            Window = lists:sublist(
                fts2_search_drop(PageOffset, Ordered), Limit
            ),
            Hits = [
                fts2_search_page_entry(Match, Request, DocKey, Opts)
             || {_Key, Match, Request, DocKey} <- Window
            ],
            Count = case PrePage of true -> Count0; false -> length(Ordered) end,
            {Hits, Count}
    end;
fts2_search_serve(
    Bookie, Schema, Root, Scored, Requests, Opts, Ranked, PrePage, Count0,
    PageOffset, Limit, TieFields
) ->
    IdentityRows = case Root of
        undefined -> [undefined || _ <- Scored];
        _ -> fts2_search_read_identity_rows(Bookie, Schema, Root, Requests)
    end,
    Hydrated = lists:filtermap(
        fun({Match, IdentityRow}) ->
            fts2_search_hydrate_hit(Match, IdentityRow, Opts)
        end,
        lists:zip(Scored, IdentityRows)
    ),
    Hits0 = [
        Hit
     || Hit <- Hydrated,
        fts2_search_facet_matches(Hit, Schema, maps:get(impact_facet, Opts, nil))
    ],
    Count = case PrePage of
        true -> Count0;
        false -> length(Hits0)
    end,
    fts2_search_check_cancellation(),
    Hits1 = case {PrePage, Ranked, TieFields} of
        {true, false, _} -> Hits0;
        _ -> fts2_search_order_hits(Hits0, Ranked, TieFields)
    end,
    {lists:sublist(fts2_search_drop(PageOffset, Hits1), Limit), Count}.

fts2_search_serve_tie([], _Values, DocKey) ->
    DocKey;
fts2_search_serve_tie(_Fields, Values, DocKey) ->
    {Values, DocKey}.

fts2_search_serve_probe(
    _Handles, _Ordinals, _Root, undefined, Match, Fields
) ->
    case Match#fts2_match.delta_document of
        undefined ->
            undefined;
        Document ->
            Candidate = maps:get(candidate_record, Document),
            {maps:get(doc_key, Document),
                [maps:get(Field, Candidate, nil) || Field <- Fields]}
    end;
fts2_search_serve_probe(
    Handles, PageOrdinals, Root, {GroupId, ChunkId}, _Match, Fields
) ->
    Shift = fts2_search_identity_page_shift(Root),
    case fts2_search_identity_handle(Handles, GroupId, Shift) of
        error ->
            undefined;
        {ok, Handle} ->
            case maps:get(GroupId bsr Shift, PageOrdinals) of
                fallback ->
                    case fts2_codec_identity_page_row(Handle, GroupId, ChunkId) of
                        undefined -> undefined;
                        Row ->
                            Candidate = element(8, Row),
                            {element(5, Row),
                                [maps:get(Field, Candidate, nil) || Field <- Fields]}
                    end;
                FieldOrdinals ->
                    case fts2_codec_identity_page_project(
                        Handle, GroupId, ChunkId, FieldOrdinals
                    ) of
                        undefined -> undefined;
                        {ok, DocKey, Values} -> {DocKey, Values};
                        {fallback, DocKey, Candidate} ->
                            {DocKey,
                                [maps:get(Field, Candidate, nil) || Field <- Fields]}
                    end
            end
    end.

fts2_search_page_entry(Match, undefined, _DocKey, Opts) ->
    case Match#fts2_match.delta_document of
        undefined -> #{};
        _Document ->
            {true, Hit} = fts2_search_hydrate_hit(Match, undefined, Opts),
            Hit
    end;
fts2_search_page_entry(Match, {GroupId, ChunkId}, DocKey, Opts) ->
    Base0 = #{
        key => Match#fts2_match.source_id,
        score => Match#fts2_match.score,
        doc_length => Match#fts2_match.doc_length,
        match_count => case Match#fts2_match.group_match_count of
            undefined -> fts2_search_match_count(Match);
            GroupCount -> GroupCount
        end,
        candidate_key => DocKey,
        group_id => GroupId,
        chunk_id => ChunkId
    },
    Base1 = case maps:get(return_positions, Opts, false) of
        true -> Base0#{positions => fts2_search_window_positions(
            Match#fts2_match.match_positions
        )};
        false -> Base0
    end,
    case maps:get(return_terms, Opts, false) of
        true -> Base1#{matched_terms => Match#fts2_match.terms};
        false -> Base1
    end.

fts2_search_fast_single_term(
    _Bookie, _Schema, _Root, _AST, #{grouping := ungrouped}, _Wanted, _Deltas
) ->
    no;
fts2_search_fast_single_term(
    Bookie,
    Schema,
    Root,
    {phrase, Specs, Columns},
    Opts,
    all,
    []
) ->
    fts2_phrase_fast(
        Bookie, Schema, Root, Specs, Columns, Opts, all
    );
fts2_search_fast_single_term(
    Bookie,
    Schema,
    Root,
    {term, Token, false, Columns},
    Opts,
    all,
    []
) ->
    ColumnIds = fts2_search_selector_ids(Columns, Schema),
    Window = maps:get(offset, Opts, 0) + maps:get(limit, Opts, ?DEFAULT_LIMIT),
    case
        {
            ColumnIds,
            maps:get(rank, Opts, none),
            maps:get(impact_facet, Opts, nil),
            Window =< 256
        }
    of
        {[Column], bm25, nil, true} ->
            Generation = maps:get(generation, Root),
            Bucket = maps:get(index, Schema),
            {Key, _} = fts2_codec_term_key(
                Generation, Column, Token
            ),
            NeedPositions = maps:get(return_positions, Opts, false),
            Requests = [{Key, <<"h">>}],
            case leveled_fts_residency:headonly_many(Bookie, Bucket, Requests) of
                [not_found] ->
                    {ranked_champions, [], 0};
                [{ok, HeaderValue}] ->
                    Header = fts2_codec_decode_header(HeaderValue),
                    Df = maps:get(group_df, Header),
                    Champions = fts2_search_impact_window(
                        maps:get(champions, Header),
                        Window,
                        fts2_search_term_impact(Df, Root)
                    ),
                    PositionMap = case NeedPositions of
                        true -> fts2_codec_header_positions(
                            Header, [element(1, Entry) || Entry <- Champions]
                        );
                        false -> #{}
                    end,
                    {ranked_champions,
                        fts2_search_ranked_champions(
                            Champions, Column, Token, Df, PositionMap, Root
                        ), maps:get(group_df, Header)};
                Bad ->
                    erlang:error({invalid_fts2_term_header, Token, Bad})
            end;
        _ ->
            no
    end;
fts2_search_fast_single_term(
    Bookie,
    Schema,
    Root,
    {term, Prefix, true, Columns},
    Opts,
    all,
    []
) ->
    fts2_search_fast_prefix(Bookie, Schema, Root, Prefix, Columns, Opts);
fts2_search_fast_single_term(
    Bookie,
    Schema,
    Root,
    {near, Items, Distance, Columns},
    Opts,
    all,
    []
) ->
    fts2_search_fast_near(
        Bookie, Schema, Root, Items, Distance, Columns, Opts
    );
fts2_search_fast_single_term(Bookie, Schema, Root, AST, Opts, all, []) ->
    fts2_search_fast_boolean(Bookie, Schema, Root, AST, Opts);
fts2_search_fast_single_term(_Bookie, _Schema, _Root, _AST, _Opts, _Wanted, _Deltas) ->
    no.

fts2_search_fast_boolean(Bookie, Schema, Root, AST, Opts) ->
    Window = maps:get(offset, Opts, 0) + maps:get(limit, Opts, ?DEFAULT_LIMIT),
    case {
        maps:get(return_positions, Opts, false),
        maps:get(rank, Opts, none),
        maps:get(impact_facet, Opts, nil),
        Window =< 256,
        fts2_search_boolean_terms(AST, Schema)
    } of
        {false, bm25, nil, true, {ok, Column, Tokens}} when length(Tokens) > 1 ->
            Generation = maps:get(generation, Root),
            Bucket = maps:get(index, Schema),
            PureOrQuery = fts2_search_boolean_pure_or(AST),
            Keys = [
                begin
                    {Key, _} = fts2_codec_term_key(Generation, Column, Token),
                    {Token, Key}
                end
             || Token <- Tokens
            ],
            HeaderPlaneResults = leveled_fts_residency:headonly_many(
                Bookie,
                Bucket,
                lists:append([
                    [
                        {Key, <<"h">>},
                        {fts2_codec_term_plane_key(Key, boolean), <<"g">>}
                    ]
                 || {_Token, Key} <- Keys
                ])
            ),
            {HeaderResults, BooleanResults} =
                fts2_search_boolean_result_pairs(HeaderPlaneResults, [], []),
            Headers = lists:zipwith(
                fun
                    ({Token, Key}, {ok, Value}) ->
                        Header = case PureOrQuery of
                            true -> fts2_codec_decode_header(Value);
                            false -> fts2_codec_decode_header_metadata(Value)
                        end,
                        {Token, Key, Header};
                    ({Token, Key}, not_found) ->
                        {Token, Key, not_found}
                end,
                Keys,
                HeaderResults
            ),
            ChampionExact = PureOrQuery andalso
                fts2_search_or_champions_exact(Headers),
            case ChampionExact of
                true ->
                    {MatchesByChunk, GroupBitmap} = lists:foldl(
                        fun
                            ({_Token, _Key, not_found}, State) ->
                                State;
                            ({Token, _Key, Header}, {MatchAcc, BitmapAcc}) ->
                                Matches = fts2_search_ranked_champions(
                                    maps:get(champions, Header),
                                    Column,
                                    Token,
                                    maps:get(group_df, Header),
                                    #{},
                                    Root
                                ),
                                {
                                    lists:foldl(
                                        fun fts2_search_merge_champion_match/2,
                                        MatchAcc,
                                        Matches
                                    ),
                                    BitmapAcc bor
                                        fts2_search_header_group_bitmap(Header)
                                }
                        end,
                        {#{}, 0},
                        Headers
                    ),
                    Grouped = fts2_search_collapse_scored_groups(
                        maps:values(MatchesByChunk), true
                    ),
                    {ranked_champions,
                        Grouped,
                        fts2_search_bitmap_count(GroupBitmap)};
                false ->
                    fts2_search_fast_boolean_planes(
                        Bookie, Bucket, Keys, Headers, AST, Column, Root,
                        PureOrQuery, Window, BooleanResults
                    )
            end;
        _ ->
            no
    end.

fts2_search_boolean_result_pairs([], Headers, Planes) ->
    {lists:reverse(Headers), lists:reverse(Planes)};
fts2_search_boolean_result_pairs(
    [Header, Plane | Rest], Headers, Planes
) ->
    fts2_search_boolean_result_pairs(
        Rest, [Header | Headers], [Plane | Planes]
    ).

fts2_search_fast_boolean_planes(
    _Bookie, _Bucket, Keys, Headers, AST, Column, Root, PureOrQuery, Window,
    BooleanResults
) ->
    BooleanByToken = maps:from_list(lists:zip(
        [Token || {Token, _Key} <- Keys], BooleanResults
    )),
    TermData = maps:from_list([
        case Header of
            not_found -> {Token, {0, <<>>, 0}};
            _ ->
                EntryPlane = fts2_search_header_entry_plane(
                    Header, maps:get(Token, BooleanByToken), Token
                ),
                GroupBitmap = case maps:find(group_bitmap, Header) of
                    {ok, StoredBitmap} -> StoredBitmap;
                    error -> fts2_search_plane_chunk_bitmap(EntryPlane, 0)
                end,
                {Token, {
                    maps:get(group_df, Header), EntryPlane, GroupBitmap
                }}
        end
     || {Token, _Key, Header} <- Headers
    ]),
    case PureOrQuery andalso Window =< 20 of
        true ->
            case fts2_search_fast_or_bounded(
                Headers, TermData, Column, Root, Window
            ) of
                no -> fts2_search_fast_boolean_full(
                    AST, TermData, Column, Root, Window
                );
                Result -> Result
            end;
        false ->
            fts2_search_fast_boolean_full(
                AST, TermData, Column, Root, Window
            )
    end.

fts2_search_fast_or_bounded(Headers, TermData, Column, Root, Window) ->
    GroupBitmap = lists:foldl(
        fun
            ({_Token, _Key, not_found}, Bitmap) -> Bitmap;
            ({_Token, _Key, Header}, Bitmap) ->
                Bitmap bor fts2_search_header_group_bitmap(Header)
        end,
        0,
        Headers
    ),
    GroupCount = fts2_search_bitmap_count(GroupBitmap),
    case GroupCount =< 1024 of
        false -> no;
        true -> fts2_search_fast_or_prefixes(
            [Size || Size <- [144, ?HEAD_WINDOW], Size >= Window],
            Headers,
            TermData,
            Column,
            Root,
            Window,
            GroupCount
        )
    end.

fts2_search_fast_or_prefixes(
    [], _Headers, _TermData, _Column, _Root, _Window, _GroupCount
) ->
    no;
fts2_search_fast_or_prefixes(
    [PrefixSize | Rest], Headers, TermData, Column, Root, Window, GroupCount
) ->
    {ChampionGroups, Threshold} = fts2_search_or_prefix_proof(
        Headers, Root, PrefixSize
    ),
    case map_size(ChampionGroups) >= Window of
        false ->
            fts2_search_fast_or_prefixes(
                Rest, Headers, TermData, Column, Root, Window, GroupCount
            );
        true ->
            Candidates = fts2_search_or_candidate_groups(
                TermData, ChampionGroups, Column, Root
            ),
            case length(Candidates) >= Window andalso
                fts2_search_or_compact_score(lists:nth(Window, Candidates)) >
                    Threshold
            of
                true ->
                    Proven = fts2_search_impact_window(
                        Candidates,
                        Window,
                        fun fts2_search_or_compact_score/1
                    ),
                    {ranked_champions,
                        fts2_search_or_candidate_records(Proven, TermData),
                        GroupCount};
                false ->
                    fts2_search_fast_or_prefixes(
                        Rest,
                        Headers,
                        TermData,
                        Column,
                        Root,
                        Window,
                        GroupCount
                    )
            end
    end.

fts2_search_or_prefix_proof(Headers, Root, PrefixSize) ->
    {Entries, Bounds} = lists:unzip([
        fts2_search_or_header_prefix(Token, Header, Root, PrefixSize)
     || {Token, _Key, Header} <- Headers,
        Header =/= not_found
    ]),
    {
        maps:from_keys(
            [element(2, Entry) || Prefix <- Entries, Entry <- Prefix],
            true
        ),
        lists:sum(Bounds)
    }.

fts2_search_or_header_prefix(Token, Header, Root, PrefixSize) ->
    Champions = maps:get(champions, Header, []),
    Prefix0 = lists:sublist(Champions, PrefixSize),
    Prefix = case length(Prefix0) < length(Champions) of
        false -> Prefix0;
        true ->
            Boundary = fts2_search_or_term_factor(lists:last(Prefix0), Root),
            Prefix0 ++ lists:takewhile(
                fun(Entry) ->
                    fts2_search_or_term_factor(Entry, Root) =:= Boundary
                end,
                lists:nthtail(length(Prefix0), Champions)
            )
    end,
    Df = maps:get(group_df, Header),
    Bound = case length(Prefix) < length(Champions) of
        true ->
            Unseen = lists:nth(length(Prefix) + 1, Champions),
            fts2_search_or_term_score(Unseen, Token, Df, Root);
        false when Champions =:= [] ->
            0.0;
        false ->
            case length(Champions) >= Df of
                true -> 0.0;
                false -> fts2_search_or_term_score(
                    lists:last(Champions), Token, Df, Root
                )
            end
    end,
    {Prefix, Bound}.

fts2_search_or_term_factor(Entry, Root) ->
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    fts2_build_bm25_tf(element(5, Entry), element(4, Entry), Avg).

fts2_search_or_term_score(Entry, _Token, Df, Root) ->
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    fts2_search_or_term_factor(Entry, Root) *
        fts2_build_bm25_idf(DocCount, Df).

fts2_search_or_candidate_groups(
    TermData, ChampionGroups, _Column, Root
) ->
    [{_First, {FirstDf, FirstPlane, _FirstBitmap}},
        {_Second, {SecondDf, SecondPlane, _SecondBitmap}}] =
            lists:sort(maps:to_list(TermData)),
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    FirstIdf = fts2_build_bm25_idf(DocCount, FirstDf),
    SecondIdf = fts2_build_bm25_idf(DocCount, SecondDf),
    Groups = fts2_search_or_target_groups(
        lists:sort(maps:keys(ChampionGroups)),
        FirstPlane, FirstIdf, SecondPlane, SecondIdf, Avg, []
    ),
    [
        Entry
     || {_Key, Entry} <- lists:keysort(
            1,
            [
                {{-element(6, Chunk), element(3, Chunk)}, Entry}
             || {Chunk, _GroupTf} = Entry <- Groups
            ]
        )
    ].

fts2_search_or_target_groups(
    [], _FirstPlane, _FirstIdf, _SecondPlane, _SecondIdf, _Avg, Acc
) ->
    lists:reverse(Acc);
fts2_search_or_target_groups(
    [GroupId | Rest], FirstPlane, FirstIdf, SecondPlane, SecondIdf, Avg, Acc
) ->
    FirstAtGroup = fts2_search_plane_seek_group(FirstPlane, GroupId),
    SecondAtGroup = fts2_search_plane_seek_group(SecondPlane, GroupId),
    {NextFirst, NextSecond, Winner, GroupTf} =
        fts2_search_or_target_group(
            FirstAtGroup, FirstIdf, SecondAtGroup, SecondIdf,
            GroupId, Avg, none, 0
        ),
    NextAcc = case Winner of
        none -> Acc;
        _ -> [{Winner, GroupTf} | Acc]
    end,
    fts2_search_or_target_groups(
        Rest, NextFirst, FirstIdf, NextSecond, SecondIdf, Avg, NextAcc
    ).

fts2_search_plane_seek_group(<<>>, _GroupId) ->
    <<>>;
fts2_search_plane_seek_group(
    <<_ChunkId:32/unsigned-big, FoundGroup:32/unsigned-big,
        _SourceId:64/unsigned-big, _Length:32/unsigned-big,
        _Tf:32/unsigned-big, _DenseId:32/unsigned-big, Rest/binary>>,
    GroupId
) when FoundGroup < GroupId ->
    fts2_search_plane_seek_group(Rest, GroupId);
fts2_search_plane_seek_group(Plane, _GroupId) ->
    Plane.

fts2_search_or_target_group(
    FirstPlane, FirstIdf, SecondPlane, SecondIdf,
    GroupId, Avg, Winner, GroupTf
) ->
    FirstIn = fts2_search_plane_group(FirstPlane) =:= GroupId,
    SecondIn = fts2_search_plane_group(SecondPlane) =:= GroupId,
    case {FirstIn, SecondIn} of
        {false, false} ->
            {FirstPlane, SecondPlane, Winner, GroupTf};
        {true, false} ->
            {NextFirst, Candidate} = fts2_search_or_take_plane(
                FirstPlane, 1, FirstIdf, Avg
            ),
            {NextWinner, NextTf} = fts2_search_or_target_add(
                Candidate, Winner, GroupTf
            ),
            fts2_search_or_target_group(
                NextFirst, FirstIdf, SecondPlane, SecondIdf,
                GroupId, Avg, NextWinner, NextTf
            );
        {false, true} ->
            {NextSecond, Candidate} = fts2_search_or_take_plane(
                SecondPlane, 2, SecondIdf, Avg
            ),
            {NextWinner, NextTf} = fts2_search_or_target_add(
                Candidate, Winner, GroupTf
            ),
            fts2_search_or_target_group(
                FirstPlane, FirstIdf, NextSecond, SecondIdf,
                GroupId, Avg, NextWinner, NextTf
            );
        {true, true} ->
            <<FirstChunk:32/unsigned-big, _/binary>> = FirstPlane,
            <<SecondChunk:32/unsigned-big, _/binary>> = SecondPlane,
            case FirstChunk - SecondChunk of
                Difference when Difference < 0 ->
                    {NextFirst, Candidate} = fts2_search_or_take_plane(
                        FirstPlane, 1, FirstIdf, Avg
                    ),
                    {NextWinner, NextTf} = fts2_search_or_target_add(
                        Candidate, Winner, GroupTf
                    ),
                    fts2_search_or_target_group(
                        NextFirst, FirstIdf, SecondPlane, SecondIdf,
                        GroupId, Avg, NextWinner, NextTf
                    );
                Difference when Difference > 0 ->
                    {NextSecond, Candidate} = fts2_search_or_take_plane(
                        SecondPlane, 2, SecondIdf, Avg
                    ),
                    {NextWinner, NextTf} = fts2_search_or_target_add(
                        Candidate, Winner, GroupTf
                    ),
                    fts2_search_or_target_group(
                        FirstPlane, FirstIdf, NextSecond, SecondIdf,
                        GroupId, Avg, NextWinner, NextTf
                    );
                0 ->
                    {NextFirst, NextSecond, Candidate} =
                        fts2_search_or_take_both(
                            FirstPlane, FirstIdf, SecondPlane, SecondIdf, Avg
                        ),
                    {NextWinner, NextTf} = fts2_search_or_target_add(
                        Candidate, Winner, GroupTf
                    ),
                    fts2_search_or_target_group(
                        NextFirst, FirstIdf, NextSecond, SecondIdf,
                        GroupId, Avg, NextWinner, NextTf
                    )
            end
    end.

fts2_search_plane_group(<<_ChunkId:32/unsigned-big,
        GroupId:32/unsigned-big, _/binary>>) ->
    GroupId;
fts2_search_plane_group(<<>>) ->
    none.

fts2_search_or_take_plane(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        Tf:32/unsigned-big, _DenseId:32/unsigned-big, Rest/binary>>,
    Bit, Idf, Avg
) ->
    {Rest, {ChunkId, GroupId, SourceId, Length, Tf,
        fts2_search_bm25(Tf, Idf, Avg, Length), Bit}}.

fts2_search_or_take_both(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        FirstTf:32/unsigned-big, _FirstDense:32/unsigned-big,
        FirstRest/binary>>,
    FirstIdf,
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        SecondTf:32/unsigned-big, _SecondDense:32/unsigned-big,
        SecondRest/binary>>,
    SecondIdf,
    Avg
) ->
    {FirstRest, SecondRest,
        {ChunkId, GroupId, SourceId, Length, FirstTf + SecondTf,
            fts2_search_bm25(FirstTf, FirstIdf, Avg, Length) +
                fts2_search_bm25(SecondTf, SecondIdf, Avg, Length),
            3}}.

fts2_search_or_target_add(Candidate, none, GroupTf) ->
    {Candidate, GroupTf + element(5, Candidate)};
fts2_search_or_target_add(Candidate, Winner, GroupTf) ->
    %% v4 term planes have one document-grain entry per term/group, but
    %% different terms may choose different representative chunks.  The OR
    %% score is the sum of both term contributions, never the best anchor.
    {ChunkId, GroupId, SourceId, Length} = case
        {element(1, Candidate), element(3, Candidate)} <
            {element(1, Winner), element(3, Winner)}
    of
        true -> {
            element(1, Candidate), element(2, Candidate),
            element(3, Candidate), element(4, Candidate)
        };
        false -> {
            element(1, Winner), element(2, Winner),
            element(3, Winner), element(4, Winner)
        }
    end,
    NextWinner = {
        ChunkId, GroupId, SourceId, Length,
        element(5, Winner) + element(5, Candidate),
        element(6, Winner) + element(6, Candidate),
        element(7, Winner) bor element(7, Candidate)
    },
    {NextWinner, GroupTf + element(5, Candidate)}.

fts2_search_or_compact_score({{_ChunkId, _GroupId, _SourceId, _Length,
        _Tf, Score, _Mask}, _GroupTf}) ->
    Score.

fts2_search_or_candidate_records(Entries, TermData) ->
    [{First, _}, {Second, _}] = lists:sort(maps:to_list(TermData)),
    [
        #fts2_match{
            chunk_id = ChunkId,
            group_id = GroupId,
            source_id = SourceId,
            doc_length = Length,
            tf = Tf,
            terms = case Mask of
                1 -> [First];
                2 -> [Second];
                3 -> [First, Second]
            end,
            group_match_count = GroupTf,
            score = Score
        }
     || {{ChunkId, GroupId, SourceId, Length, Tf, Score, Mask}, GroupTf} <-
            Entries
    ].

fts2_search_fast_boolean_full(AST, TermData, Column, Root, Window) ->
    ResultBitmap = fts2_search_boolean_bitmap(AST, TermData),
    ResultBytes = binary:encode_unsigned(ResultBitmap, little),
    PositiveTokens = fts2_search_boolean_positive_tokens(AST),
    case fts2_search_boolean_required_tokens(AST) of
        [] ->
            ResultCount = fts2_search_bitmap_count(ResultBitmap),
            RowsByChunk = lists:foldl(
                fun(Token, Acc) ->
                    {Df, EntryPlane, _Bitmap} = maps:get(Token, TermData),
                    case ResultCount * 16 < Df of
                        true ->
                            fts2_search_boolean_select_sparse(
                                EntryPlane, ResultBytes, Column, Token, Df, Acc
                            );
                        false ->
                            fts2_search_boolean_select_plane(
                                EntryPlane, ResultBytes, Column, Token, Df, Acc
                            )
                    end
                end,
                #{},
                PositiveTokens
            ),
            Rows = maps:values(RowsByChunk),
            Scored = fts2_search_boolean_score_rows(Rows, TermData, Root),
            Grouped = fts2_search_collapse_scored_groups(Scored, true),
            {ranked_champions, Grouped, length(Grouped)};
        RequiredTokens ->
            Driver = fts2_search_boolean_smallest_required(
                RequiredTokens, TermData
            ),
            case length(PositiveTokens) =< 3 of
                true -> fts2_search_boolean_compact_ranked(
                    Driver, PositiveTokens, TermData, ResultBytes, Column,
                    Root, Window
                );
                false -> fts2_search_boolean_driver_ranked(
                    Driver, PositiveTokens, TermData, ResultBytes, Column,
                    Root, Window
                )
            end
    end.

%% Every satisfying assignment of an AND/NOT expression contains its required
%% terms.  OR keeps only terms required by both arms.  A non-empty required set
%% therefore provides a complete driver plane: no result row can exist outside
%% the smallest required plane.
fts2_search_boolean_required_tokens({term, Token, false, _Columns}) ->
    [Token];
fts2_search_boolean_required_tokens({'and', A, B}) ->
    lists:usort(
        fts2_search_boolean_required_tokens(A) ++
            fts2_search_boolean_required_tokens(B)
    );
fts2_search_boolean_required_tokens({'or', A, B}) ->
    ordsets:intersection(
        fts2_search_boolean_required_tokens(A),
        fts2_search_boolean_required_tokens(B)
    );
fts2_search_boolean_required_tokens({'not', A, _B}) ->
    fts2_search_boolean_required_tokens(A).

fts2_search_boolean_smallest_required([First | Rest], TermData) ->
    lists:foldl(
        fun(Token, Best) ->
            {Df, _Plane, _Bitmap} = maps:get(Token, TermData),
            {BestDf, _BestPlane, _BestBitmap} = maps:get(Best, TermData),
            case Df < BestDf of
                true -> Token;
                false -> Best
            end
        end,
        First,
        Rest
    ).

fts2_search_boolean_compact_ranked(
    Driver, PositiveTokens, TermData, Result, Column, Root, Window
) ->
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    Descriptors = [
        begin
            {Df, Plane, _Bitmap} = maps:get(Token, TermData),
            {Token, Df, fts2_build_bm25_idf(DocCount, Df), Plane}
        end
     || Token <- lists:reverse(PositiveTokens)
    ],
    Groups = fts2_search_boolean_compact_groups(
        Driver, Descriptors, Result, Avg
    ),
    Ordered = [
        Entry
     || {_Key, Entry} <- lists:keysort(
            1,
            [
                {{-element(1, Chunk), element(2, Chunk)}, Entry}
             || {Chunk, _GroupTf} = Entry <- Groups
            ]
        )
    ],
    Proven = fts2_search_impact_window(
        Ordered,
        Window,
        fun({Chunk, _GroupTf}) -> element(1, Chunk) end
    ),
    Specs = [{Token, Df} || {Token, Df, _Idf, _Plane} <- Descriptors],
    Matches = [
        begin
            TokenTfs = [
                {Token, Tf, Df}
             || {{Token, Df}, Tf} <- lists:zip(Specs, tuple_to_list(Tfs)),
                Tf > 0
            ],
            #fts2_match{
                chunk_id = ChunkId,
                group_id = GroupId,
                source_id = SourceId,
                doc_length = Length,
                tf = TotalTf,
                term_stats = [
                    {{Column, Token}, Tf, base, Df}
                 || {Token, Tf, Df} <- TokenTfs
                ],
                terms = [Token || {Token, _Tf, _Df} <- TokenTfs],
                group_match_count = GroupTf,
                score = Score
            }
        end
     || {{Score, SourceId, GroupId, ChunkId, Length, TotalTf, Tfs},
            GroupTf} <- Proven
    ],
    {ranked_champions, Matches, length(Groups)}.

fts2_search_boolean_compact_groups(
    Driver,
    [{Token, _Df, Idf, Plane}],
    Result,
    Avg
) when Token =:= Driver ->
    fts2_search_boolean_compact_one(
        Plane, Idf, Result, Avg, none, []
    );
fts2_search_boolean_compact_groups(
    Driver,
    [{First, _FirstDf, FirstIdf, FirstPlane},
        {Second, _SecondDf, SecondIdf, SecondPlane}],
    Result,
    Avg
) ->
    {DriverPlane, OtherPlane, DriverSide} = case Driver of
        First -> {FirstPlane, SecondPlane, first};
        Second -> {SecondPlane, FirstPlane, second}
    end,
    fts2_search_boolean_compact_two(
        DriverPlane, OtherPlane, DriverSide, FirstIdf, SecondIdf,
        Result, Avg, none, []
    );
fts2_search_boolean_compact_groups(
    Driver,
    [{First, _FirstDf, FirstIdf, FirstPlane},
        {Second, _SecondDf, SecondIdf, SecondPlane},
        {Third, _ThirdDf, ThirdIdf, ThirdPlane}],
    Result,
    Avg
) ->
    {DriverPlane, OtherOne, OtherTwo, DriverSide} = case Driver of
        First -> {FirstPlane, SecondPlane, ThirdPlane, first};
        Second -> {SecondPlane, FirstPlane, ThirdPlane, second};
        Third -> {ThirdPlane, FirstPlane, SecondPlane, third}
    end,
    fts2_search_boolean_compact_three(
        DriverPlane, OtherOne, OtherTwo, DriverSide,
        FirstIdf, SecondIdf, ThirdIdf, Result, Avg, none, []
    ).

fts2_search_boolean_compact_one(
    <<>>, _Idf, _Result, _Avg, Current, Acc
) ->
    lists:reverse(fts2_search_boolean_compact_finish(Current, Acc));
fts2_search_boolean_compact_one(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        Tf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    Idf, Result, Avg, Current, Acc
) ->
    {NextCurrent, NextAcc} = case fts2_search_boolean_selected(
        Result, DenseId
    ) of
        false -> {Current, Acc};
        true -> fts2_search_boolean_compact_add(
            {fts2_search_bm25(Tf, Idf, Avg, Length), SourceId, GroupId,
                ChunkId, Length, Tf, {Tf}},
            Current,
            Acc
        )
    end,
    fts2_search_boolean_compact_one(
        Rest, Idf, Result, Avg, NextCurrent, NextAcc
    ).

fts2_search_boolean_compact_two(
    <<>>, _Other, _Side, _FirstIdf, _SecondIdf, _Result, _Avg, Current, Acc
) ->
    lists:reverse(fts2_search_boolean_compact_finish(Current, Acc));
fts2_search_boolean_compact_two(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        DriverTf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    Other, Side, FirstIdf, SecondIdf, Result, Avg, Current, Acc
) ->
    case fts2_search_boolean_selected(Result, DenseId) of
        false ->
            fts2_search_boolean_compact_two(
                Rest, Other, Side, FirstIdf, SecondIdf, Result, Avg,
                Current, Acc
            );
        true ->
            {Found, NextOther} = fts2_search_plane_seek_dense(Other, DenseId),
            OtherTf = case Found of not_found -> 0; {ok, Tf} -> Tf end,
            {FirstTf, SecondTf} = case Side of
                first -> {DriverTf, OtherTf};
                second -> {OtherTf, DriverTf}
            end,
            Score = fts2_search_bm25(FirstTf, FirstIdf, Avg, Length) +
                fts2_search_bm25(SecondTf, SecondIdf, Avg, Length),
            {NextCurrent, NextAcc} = fts2_search_boolean_compact_add(
                {Score, SourceId, GroupId, ChunkId, Length,
                    FirstTf + SecondTf, {FirstTf, SecondTf}},
                Current,
                Acc
            ),
            fts2_search_boolean_compact_two(
                Rest, NextOther, Side, FirstIdf, SecondIdf, Result, Avg,
                NextCurrent, NextAcc
            )
    end.

fts2_search_boolean_compact_three(
    <<>>, _OtherOne, _OtherTwo, _Side,
    _FirstIdf, _SecondIdf, _ThirdIdf, _Result, _Avg, Current, Acc
) ->
    lists:reverse(fts2_search_boolean_compact_finish(Current, Acc));
fts2_search_boolean_compact_three(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        DriverTf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    OtherOne, OtherTwo, Side, FirstIdf, SecondIdf, ThirdIdf,
    Result, Avg, Current, Acc
) ->
    case fts2_search_boolean_selected(Result, DenseId) of
        false ->
            fts2_search_boolean_compact_three(
                Rest, OtherOne, OtherTwo, Side,
                FirstIdf, SecondIdf, ThirdIdf, Result, Avg, Current, Acc
            );
        true ->
            {FoundOne, NextOtherOne} = fts2_search_plane_seek_dense(
                OtherOne, DenseId
            ),
            {FoundTwo, NextOtherTwo} = fts2_search_plane_seek_dense(
                OtherTwo, DenseId
            ),
            TfOne = case FoundOne of not_found -> 0; {ok, TfA} -> TfA end,
            TfTwo = case FoundTwo of not_found -> 0; {ok, TfB} -> TfB end,
            {FirstTf, SecondTf, ThirdTf} = case Side of
                first -> {DriverTf, TfOne, TfTwo};
                second -> {TfOne, DriverTf, TfTwo};
                third -> {TfOne, TfTwo, DriverTf}
            end,
            Score = fts2_search_bm25(FirstTf, FirstIdf, Avg, Length) +
                fts2_search_bm25(SecondTf, SecondIdf, Avg, Length) +
                fts2_search_bm25(ThirdTf, ThirdIdf, Avg, Length),
            {NextCurrent, NextAcc} = fts2_search_boolean_compact_add(
                {Score, SourceId, GroupId, ChunkId, Length,
                    FirstTf + SecondTf + ThirdTf,
                    {FirstTf, SecondTf, ThirdTf}},
                Current,
                Acc
            ),
            fts2_search_boolean_compact_three(
                Rest, NextOtherOne, NextOtherTwo, Side,
                FirstIdf, SecondIdf, ThirdIdf, Result, Avg,
                NextCurrent, NextAcc
            )
    end.

fts2_search_boolean_selected(Result, DenseId) ->
    ByteIndex = DenseId bsr 3,
    ByteIndex < byte_size(Result) andalso
        (binary:at(Result, ByteIndex) band (1 bsl (DenseId band 7))) =/= 0.

fts2_search_boolean_compact_add(
    {_Score, _SourceId, GroupId, _ChunkId, _Length, Tf, _Tfs} = Chunk,
    none,
    Acc
) ->
    {{GroupId, Chunk, Tf}, Acc};
fts2_search_boolean_compact_add(
    {Score, SourceId, GroupId, _ChunkId, _Length, Tf, _Tfs} = Chunk,
    {GroupId, Existing, GroupTf},
    Acc
) ->
    Winner = case {-Score, SourceId} <
        {-element(1, Existing), element(2, Existing)} of
        true -> Chunk;
        false -> Existing
    end,
    {{GroupId, Winner, GroupTf + Tf}, Acc};
fts2_search_boolean_compact_add(Chunk, Current, Acc) ->
    NextAcc = fts2_search_boolean_compact_finish(Current, Acc),
    {{element(3, Chunk), Chunk, element(6, Chunk)}, NextAcc}.

fts2_search_boolean_compact_finish(none, Acc) ->
    Acc;
fts2_search_boolean_compact_finish({_GroupId, Winner, GroupTf}, Acc) ->
    [{Winner, GroupTf} | Acc].

fts2_search_boolean_driver_ranked(
    Driver, PositiveTokens, TermData, Result, Column, Root, Window
) ->
    Chunks = fts2_search_boolean_driver_rows(
        Driver, PositiveTokens, TermData, Result, Column, Root
    ),
    Groups = lists:foldl(
        fun({_Score, SourceId, GroupId, _ChunkId, _Length, TotalTf,
                _Stats} = Chunk, Acc) ->
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => {Chunk, TotalTf}};
                {ok, {Existing, GroupTf}} ->
                    Winner = case {-element(1, Chunk), SourceId} <
                        {-element(1, Existing), element(2, Existing)} of
                        true -> Chunk;
                        false -> Existing
                    end,
                    Acc#{GroupId => {Winner, GroupTf + TotalTf}}
            end
        end,
        #{},
        Chunks
    ),
    Ordered = [
        Entry
     || {_Key, Entry} <- lists:keysort(
            1,
            [
                {{-element(1, Chunk), element(2, Chunk)}, Entry}
             || {Chunk, _GroupTf} = Entry <- maps:values(Groups)
            ]
        )
    ],
    Proven = fts2_search_impact_window(
        Ordered,
        Window,
        fun({Chunk, _GroupTf}) -> element(1, Chunk) end
    ),
    Matches = [
        #fts2_match{
            chunk_id = ChunkId,
            group_id = GroupId,
            source_id = SourceId,
            doc_length = Length,
            tf = TotalTf,
            term_stats = Stats,
            terms = [Token ||
                {{_StatColumn, Token}, _Tf, _Source, _Df} <- Stats],
            group_match_count = GroupTf,
            score = Score
        }
     || {{Score, SourceId, GroupId, ChunkId, Length, TotalTf, Stats},
            GroupTf} <- Proven
    ],
    {ranked_champions, Matches, map_size(Groups)}.

fts2_search_boolean_driver_rows(
    Driver, PositiveTokens, TermData, Result, Column, Root
) ->
    {DriverDf, DriverPlane, _DriverBitmap} = maps:get(Driver, TermData),
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    Idfs = maps:from_list([
        {Token, fts2_build_bm25_idf(DocCount, Df)}
     || {Token, {Df, _Plane, _Bitmap}} <- maps:to_list(TermData)
    ]),
    StatTokens = lists:reverse(PositiveTokens),
    Cursors = [
        {Token, Df, Plane}
     || Token <- StatTokens,
        Token =/= Driver,
        {Df, Plane, _Bitmap} <- [maps:get(Token, TermData)]
    ],
    fts2_search_boolean_driver_plane(
        DriverPlane, Driver, DriverDf, StatTokens, Result, Column, Avg, Idfs,
        Cursors, []
    ).

fts2_search_boolean_driver_plane(
    <<>>, _Driver, _DriverDf, _Tokens, _Result, _Column, _Avg, _Idfs,
    _Cursors, Acc
) ->
    lists:reverse(Acc);
fts2_search_boolean_driver_plane(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        DriverTf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    Driver, DriverDf, Tokens, Result, Column, Avg, Idfs, Cursors, Acc
) ->
    ByteIndex = DenseId bsr 3,
    Selected = ByteIndex < byte_size(Result) andalso
        (binary:at(Result, ByteIndex) band (1 bsl (DenseId band 7))) =/= 0,
    {NextCursors, NextAcc} = case Selected of
        false ->
            {Cursors, Acc};
        true ->
            {Stats, TotalTf, AdvancedCursors} =
                fts2_search_boolean_driver_stats(
                    Tokens, Driver, DriverTf, DriverDf, DenseId, Column,
                    Cursors, [], 0
                ),
            Score = lists:sum([
                fts2_search_bm25(
                    Tf, maps:get(Token, Idfs), Avg, Length
                )
             || {{_StatColumn, Token}, Tf, _Source, _Df} <- Stats
            ]),
            {AdvancedCursors, [{
                Score, SourceId, GroupId, ChunkId, Length, TotalTf, Stats
            } | Acc]}
    end,
    fts2_search_boolean_driver_plane(
        Rest, Driver, DriverDf, Tokens, Result, Column, Avg, Idfs,
        NextCursors, NextAcc
    ).

fts2_search_boolean_driver_stats(
    [], _Driver, _DriverTf, _DriverDf, _DenseId, _Column, Cursors, Acc, TotalTf
) ->
    {lists:reverse(Acc), TotalTf, Cursors};
fts2_search_boolean_driver_stats(
    [Driver | Rest], Driver, DriverTf, DriverDf, DenseId, Column,
    Cursors, Acc, TotalTf
) ->
    fts2_search_boolean_driver_stats(
        Rest, Driver, DriverTf, DriverDf, DenseId, Column, Cursors,
        [{{Column, Driver}, DriverTf, base, DriverDf} | Acc],
        TotalTf + DriverTf
    );
fts2_search_boolean_driver_stats(
    [Token | Rest], Driver, DriverTf, DriverDf, DenseId, Column,
    [{Token, Df, Plane} | Cursors], Acc, TotalTf
) ->
    {Found, NextPlane} = fts2_search_plane_seek_dense(Plane, DenseId),
    {NextAcc, NextTotal} = case Found of
        not_found -> {Acc, TotalTf};
        {ok, Tf} ->
            {[{{Column, Token}, Tf, base, Df} | Acc], TotalTf + Tf}
    end,
    {Stats, FinalTotal, NextCursors} = fts2_search_boolean_driver_stats(
        Rest, Driver, DriverTf, DriverDf, DenseId, Column, Cursors,
        NextAcc, NextTotal
    ),
    {Stats, FinalTotal, [{Token, Df, NextPlane} | NextCursors]}.

%% The tail of a skipped row is passed only to this immediate self-call.  The
%% BEAM keeps the match context instead of allocating one sub-binary per row.
fts2_search_plane_seek_dense(<<>>, _DenseId) ->
    {not_found, <<>>};
fts2_search_plane_seek_dense(
    <<_ChunkId:32/unsigned-big, _GroupId:32/unsigned-big,
        _SourceId:64/unsigned-big, _Length:32/unsigned-big,
        Tf:32/unsigned-big, Found:32/unsigned-big, Rest/binary>> = Plane,
    DenseId
) ->
    case Found - DenseId of
        Difference when Difference < 0 ->
            fts2_search_plane_seek_dense(Rest, DenseId);
        0 ->
            {{ok, Tf}, Plane};
        _ ->
            {not_found, Plane}
    end.

fts2_search_boolean_score_rows(Rows, TermData, Root) ->
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    Idfs = maps:from_list([
        {Token, fts2_build_bm25_idf(DocCount, Df)}
     || {Token, {Df, _EntryPlane, _Bitmap}} <- maps:to_list(TermData)
    ]),
    [
        #fts2_match{
            chunk_id = ChunkId,
            group_id = GroupId,
            source_id = SourceId,
            doc_length = Length,
            tf = TotalTf,
            term_stats = Stats,
            terms = [Token ||
                {{_Column, Token}, _Tf, _Source, _Df} <- Stats],
            score = lists:sum([
                fts2_search_bm25(
                    Tf, maps:get(Token, Idfs), Avg, Length
                )
             || {{_Column, Token}, Tf, _Source, _Df} <- Stats
            ])
        }
     || {ChunkId, GroupId, SourceId, Length, Stats, TotalTf} <- Rows
    ].

fts2_search_or_champions_exact(Headers) ->
    lists:all(
        fun
            ({_Token, _Key, not_found}) ->
                true;
            ({_Token, _Key, Header}) ->
                length(maps:get(champions, Header, [])) >=
                    maps:get(group_df, Header, 0)
        end,
        Headers
    ).

fts2_search_fast_prefix(Bookie, Schema, Root, Prefix, Columns, Opts) ->
    Window = maps:get(offset, Opts, 0) + maps:get(limit, Opts, ?DEFAULT_LIMIT),
    case {
        fts2_search_selector_ids(Columns, Schema),
        maps:get(rank, Opts, none),
        maps:get(impact_facet, Opts, nil),
        Window =< 256
    } of
        {[Column], bm25, nil, true} ->
            Generation = maps:get(generation, Root),
            Bucket = maps:get(index, Schema),
            %% A prefix can make a document competitive by accumulating many
            %% individually non-champion expansions.  Enumerate the complete
            %% group plane for every expansion; champion-only enumeration is
            %% not a sound document-grain proof.
            HeaderRange = fts2_search_prefix_headers(
                Bookie, Bucket, Generation, Column, Prefix,
                fun fts2_codec_decode_header_metadata/1
            ),
            Requests = [
                {fts2_codec_term_plane_key(Key, boolean), <<"g">>}
             || {Key, _Token, _Header} <- HeaderRange
            ],
            Values = case Requests of
                [] -> [];
                _ -> leveled_fts_residency:headonly_many(
                    Bookie, Bucket, Requests
                )
            end,
            TermData = maps:from_list([
                begin
                    Plane = case Value of
                        {ok, EncodedPlane} ->
                            fts2_codec_plane_payload(EncodedPlane);
                        Bad ->
                            erlang:error({invalid_fts2_group_plane, Token, Bad})
                    end,
                    {Token, {
                        maps:get(group_df, Header),
                        Plane,
                        case maps:find(group_bitmap, Header) of
                            {ok, Bitmap} -> Bitmap;
                            error -> fts2_search_plane_chunk_bitmap(Plane, 0)
                        end
                    }}
                end
             || {{_Key, Token, Header}, Value} <-
                    lists:zip(HeaderRange, Values)
            ]),
            case maps:keys(TermData) of
                [] ->
                    {ranked_champions, [], 0};
                [Only] ->
                    fts2_search_fast_boolean_full(
                        {term, Only, false, []},
                        TermData, Column, Root, Window
                    );
                [First | Rest] ->
                    PrefixAST = lists:foldl(
                        fun(Token, AST) ->
                            {'or', AST, {term, Token, false, []}}
                        end,
                        {term, First, false, []},
                        Rest
                    ),
                    fts2_search_fast_boolean_full(
                        PrefixAST, TermData, Column, Root, Window
                    )
            end;
        _ ->
            no
    end.

fts2_search_prefix_group_records(Chunks, Column, Window) ->
    Groups = lists:foldl(
        fun({_ChunkId, GroupId, SourceId, _Length, Tf, Score, _Stats} = Chunk,
                Acc) ->
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => {Chunk, Tf, false}};
                {ok, {Existing, CombinedTf, _Collided}} ->
                    Winner = case {-Score, SourceId} <
                        {-element(6, Existing), element(3, Existing)} of
                        true -> Chunk;
                        false -> Existing
                    end,
                    Acc#{GroupId => {Winner, CombinedTf + Tf, true}}
            end
        end,
        #{},
        Chunks
    ),
    Ordered = [
        Entry
     || {_Key, Entry} <- lists:keysort(
            1,
            [
                {{-element(6, Chunk), element(3, Chunk)}, Entry}
             || {Chunk, _CombinedTf, _Collided} = Entry <- maps:values(Groups)
            ]
        )
    ],
    Proven = fts2_search_impact_window(
        Ordered,
        Window,
        fun({Chunk, _CombinedTf, _Collided}) -> element(6, Chunk) end
    ),
    [
        begin
            OrderedStats = lists:reverse(Stats),
            LastToken = element(1, lists:last(Stats)),
            #fts2_match{
                chunk_id = ChunkId,
                group_id = GroupId,
                source_id = SourceId,
                doc_length = Length,
                tf = Tf,
                term_stats = [
                    {{Column, Token}, TokenTf, base, Df}
                 || {Token, TokenTf, Df} <- OrderedStats
                ],
                terms = [Token || {Token, _TokenTf, _Df} <- OrderedStats],
                columns = [{{Column, LastToken}, []}],
                match_positions = [{LastToken, []}],
                group_match_count = case Collided of
                    true -> CombinedTf;
                    false -> undefined
                end,
                score = Score
            }
        end
     || {{ChunkId, GroupId, SourceId, Length, Tf, Score, Stats}, CombinedTf,
            Collided} <- Proven
    ].

%% Phrase and NEAR verification over the positional planes.
%%
%% The intersection of the two entry planes is driven by chunk ids alone, so a
%% plane row that only one term posts costs one binary step and nothing else.
%% A chunk's position payload is sliced out only after both terms are known to
%% post there, and verification then streams the two delta-encoded payloads
%% through monotonic cursors: no position list is ever materialised, and match
%% starts are collected only when the caller asked for positions.
fts2_search_positional_pair(
    Bookie, Schema, Root, Column, First, Second, Verify, NeedPositions
) ->
    Generation = maps:get(generation, Root),
    Bucket = maps:get(index, Schema),
    {FirstKey, _} = fts2_codec_term_key(Generation, Column, First),
    {SecondKey, _} = fts2_codec_term_key(Generation, Column, Second),
    FirstBooleanKey = fts2_codec_term_plane_key(FirstKey, boolean),
    SecondBooleanKey = fts2_codec_term_plane_key(SecondKey, boolean),
    FirstPositionKey = fts2_codec_term_plane_key(FirstKey, positions),
    SecondPositionKey = fts2_codec_term_plane_key(SecondKey, positions),
    case leveled_fts_residency:headonly_many(
        Bookie,
        Bucket,
        [
            {FirstKey, <<"h">>},
            {SecondKey, <<"h">>},
            {FirstBooleanKey, <<"b">>},
            {SecondBooleanKey, <<"b">>},
            {FirstPositionKey, <<"p">>},
            {SecondPositionKey, <<"p">>}
        ]
    ) of
        [{ok, FirstValue}, {ok, SecondValue}, FirstBoolean, SecondBoolean,
            FirstPosition, SecondPosition] ->
            FirstHeader = fts2_codec_decode_header_metadata(FirstValue),
            SecondHeader = fts2_codec_decode_header_metadata(SecondValue),
            FirstEntries = fts2_search_header_entry_plane(
                FirstHeader, FirstBoolean, First
            ),
            SecondEntries = fts2_search_header_entry_plane(
                SecondHeader, SecondBoolean, Second
            ),
            Rows = case maps:get(position_order, Root, legacy) of
                chunk ->
                    fts2_search_pair_rows(
                        fts2_search_position_payload(
                            fts2_search_header_position_plane(
                                FirstHeader, FirstPosition, First
                            ),
                            First
                        ),
                        fts2_search_position_payload(
                            fts2_search_header_position_plane(
                                SecondHeader, SecondPosition, Second
                            ),
                            Second
                        ),
                        FirstEntries,
                        Verify,
                        NeedPositions
                    );
                legacy ->
                    fts2_search_pair_legacy(
                        FirstEntries, FirstHeader, FirstPosition, First,
                        SecondEntries, SecondHeader, SecondPosition, Second,
                        Verify, NeedPositions
                    )
            end,
            {FirstHeader, SecondHeader, Rows};
        _ ->
            not_found
    end.

fts2_search_position_payload(
    <<?POSITION_VERSION:8, _Count:32/unsigned-big, Payload/binary>>, _Token
) ->
    Payload;
fts2_search_position_payload(Bad, Token) ->
    erlang:error({invalid_fts2_position_row, Token, Bad}).

%% The position rows already carry the chunk id and, as their position count,
%% the term frequency, so the whole intersection runs on the two position
%% planes. Each cursor is advanced by a self-recursive skip run, which the
%% compiler keeps as one match context: a row that only one term posts costs
%% no term at all. The payload is sliced out only for a chunk both terms post,
%% and the doc metadata is read from the first term's entry plane only for a
%% chunk that verifies.
fts2_search_pair_rows(FirstPositions, SecondPositions, Entries, Verify,
        NeedPositions) ->
    fts2_search_pair_seek_second(
        FirstPositions, SecondPositions, Entries, byte_size(Entries) div 28,
        0, Verify, NeedPositions, []
    ).

fts2_search_pair_seek_second(<<>>, _SecondPositions, _Entries, _High, _Low,
        _Verify, _NeedPositions, Acc) ->
    lists:reverse(Acc);
fts2_search_pair_seek_second(
    <<FirstChunk:32/unsigned-big, _/binary>> = FirstPositions,
    SecondPositions, Entries, High, Low, Verify, NeedPositions, Acc
) ->
    case fts2_search_position_advance(SecondPositions, FirstChunk) of
        done ->
            lists:reverse(Acc);
        {FirstChunk, Matched} ->
            fts2_search_pair_match(
                FirstChunk, FirstPositions, Matched, Entries, High, Low,
                Verify, NeedPositions, Acc
            );
        {SecondChunk, Ahead} ->
            fts2_search_pair_seek_first(
                FirstPositions, SecondChunk, Ahead, Entries, High, Low,
                Verify, NeedPositions, Acc
            )
    end.

fts2_search_pair_seek_first(
    FirstPositions, SecondChunk, SecondPositions, Entries, High, Low, Verify,
    NeedPositions, Acc
) ->
    case fts2_search_position_advance(FirstPositions, SecondChunk) of
        done ->
            lists:reverse(Acc);
        {SecondChunk, Matched} ->
            fts2_search_pair_match(
                SecondChunk, Matched, SecondPositions, Entries, High, Low,
                Verify, NeedPositions, Acc
            );
        {_FirstChunk, Ahead} ->
            fts2_search_pair_seek_second(
                Ahead, SecondPositions, Entries, High, Low, Verify,
                NeedPositions, Acc
            )
    end.

%% Advance a cursor to the first row at or after Target. The tail is only ever
%% the argument of the immediate self call, so a skipped row allocates nothing.
fts2_search_position_advance(
    <<ChunkId:32/unsigned-big, _Count:32/unsigned-big, Bytes:32/unsigned-big,
        _:Bytes/binary, Rest/binary>>,
    Target
) when ChunkId < Target ->
    fts2_search_position_advance(Rest, Target);
fts2_search_position_advance(
    <<ChunkId:32/unsigned-big, _/binary>> = Positions, _Target
) ->
    {ChunkId, Positions};
fts2_search_position_advance(<<>>, _Target) ->
    done.

fts2_search_pair_match(
    ChunkId,
    <<_FirstChunk:32/unsigned-big, FirstCount:32/unsigned-big,
        FirstBytes:32/unsigned-big, FirstEncoded:FirstBytes/binary,
        FirstRest/binary>>,
    <<_SecondChunk:32/unsigned-big, SecondCount:32/unsigned-big,
        SecondBytes:32/unsigned-big, SecondEncoded:SecondBytes/binary,
        SecondRest/binary>>,
    Entries, High, Low, Verify, NeedPositions, Acc
) ->
    {MatchCount, Starts} = fts2_search_pair_verify(
        Verify, FirstCount, FirstEncoded, SecondCount, SecondEncoded,
        NeedPositions
    ),
    {NextLow, NextAcc} = case MatchCount of
        0 ->
            {Low, Acc};
        _ ->
            case fts2_search_plane_find_chunk(Entries, ChunkId, Low, High) of
                {Index, GroupId, SourceId, Length} ->
                    {Index + 1,
                        [{ChunkId, GroupId, SourceId, Length, FirstCount,
                            SecondCount, MatchCount, Starts} | Acc]};
                not_found ->
                    erlang:error({invalid_fts2_plane_alignment, ChunkId})
            end
    end,
    fts2_search_pair_seek_second(
        FirstRest, SecondRest, Entries, High, NextLow, Verify, NeedPositions,
        NextAcc
    ).

%% Verified chunks arrive in plane order, so the entry-plane search never
%% rewinds: each lookup starts after the previous verified row.
fts2_search_plane_find_chunk(_Entries, _ChunkId, Low, High) when Low >= High ->
    not_found;
fts2_search_plane_find_chunk(Entries, ChunkId, Low, High) ->
    Mid = (Low + High) bsr 1,
    <<Found:32/unsigned-big, GroupId:32/unsigned-big, SourceId:64/unsigned-big,
        Length:32/unsigned-big>> = binary:part(Entries, Mid * 28, 20),
    case Found of
        ChunkId ->
            {Mid, GroupId, SourceId, Length};
        _ when Found < ChunkId ->
            fts2_search_plane_find_chunk(Entries, ChunkId, Mid + 1, High);
        _ ->
            fts2_search_plane_find_chunk(Entries, ChunkId, Low, Mid)
    end.

fts2_search_pair_verify(
    phrase, FirstCount, FirstEncoded, SecondCount, SecondEncoded,
    NeedPositions
) when FirstCount > 0, SecondCount > 0 ->
    {First, FirstRest} = fts2_codec_decode_varint(FirstEncoded, 0, 0),
    {Second, SecondRest} = fts2_codec_decode_varint(SecondEncoded, 0, 0),
    fts2_search_stream_phrase(
        FirstCount - 1, FirstRest, First, SecondCount - 1, SecondRest, Second,
        NeedPositions, 0, []
    );
fts2_search_pair_verify(
    {near, Distance}, FirstCount, FirstEncoded, SecondCount, SecondEncoded,
    NeedPositions
) when FirstCount > 0, SecondCount > 0 ->
    {First, FirstRest} = fts2_codec_decode_varint(FirstEncoded, 0, 0),
    {Second, SecondRest} = fts2_codec_decode_varint(SecondEncoded, 0, 0),
    fts2_search_stream_near(
        FirstCount - 1, FirstRest, First, SecondCount - 1, SecondRest, Second,
        Distance, NeedPositions, 0, []
    );
fts2_search_pair_verify(_Verify, _FirstCount, _FirstEncoded, _SecondCount,
        _SecondEncoded, _NeedPositions) ->
    {0, []}.

%% A phrase match is a first position immediately before a second position.
%% Both cursors advance in position order; neither payload is decoded past the
%% point where the other is exhausted.
fts2_search_stream_phrase(
    FirstCount, FirstEncoded, First, SecondCount, SecondEncoded, Second,
    NeedPositions, Count, Acc
) ->
    case (First + 1) - Second of
        Difference when Difference < 0, FirstCount > 0 ->
            {Delta, Rest} = fts2_codec_decode_varint(FirstEncoded, 0, 0),
            fts2_search_stream_phrase(
                FirstCount - 1, Rest, First + Delta, SecondCount,
                SecondEncoded, Second, NeedPositions, Count, Acc
            );
        Difference when Difference > 0, SecondCount > 0 ->
            {Delta, Rest} = fts2_codec_decode_varint(SecondEncoded, 0, 0),
            fts2_search_stream_phrase(
                FirstCount, FirstEncoded, First, SecondCount - 1, Rest,
                Second + Delta, NeedPositions, Count, Acc
            );
        0 when FirstCount > 0, SecondCount > 0 ->
            {FirstDelta, FirstRest} = fts2_codec_decode_varint(FirstEncoded, 0, 0),
            {SecondDelta, SecondRest} = fts2_codec_decode_varint(SecondEncoded, 0, 0),
            fts2_search_stream_phrase(
                FirstCount - 1, FirstRest, First + FirstDelta,
                SecondCount - 1, SecondRest, Second + SecondDelta,
                NeedPositions, Count + 1,
                fts2_search_stream_collect(NeedPositions, First, Acc)
            );
        0 ->
            {Count + 1, lists:reverse(
                fts2_search_stream_collect(NeedPositions, First, Acc)
            )};
        _Exhausted ->
            {Count, lists:reverse(Acc)}
    end.

%% A NEAR match is a first position with a second position no more than
%% Distance token gaps away on either side. The second cursor is monotonic:
%% once a second position is behind the window it can never re-enter it.
fts2_search_stream_near(
    FirstCount, FirstEncoded, First, SecondCount, SecondEncoded, Second,
    Distance, NeedPositions, Count, Acc
) ->
    case Second < First - Distance - 1 of
        true when SecondCount > 0 ->
            {Delta, Rest} = fts2_codec_decode_varint(SecondEncoded, 0, 0),
            fts2_search_stream_near(
                FirstCount, FirstEncoded, First, SecondCount - 1, Rest,
                Second + Delta, Distance, NeedPositions, Count, Acc
            );
        true ->
            {Count, lists:reverse(Acc)};
        false ->
            {NextCount, NextAcc} = case Second =< First + Distance + 1 of
                true ->
                    {Count + 1,
                        fts2_search_stream_collect(NeedPositions, First, Acc)};
                false ->
                    {Count, Acc}
            end,
            case FirstCount of
                0 ->
                    {NextCount, lists:reverse(NextAcc)};
                _ ->
                    {Delta, Rest} = fts2_codec_decode_varint(FirstEncoded, 0, 0),
                    fts2_search_stream_near(
                        FirstCount - 1, Rest, First + Delta, SecondCount,
                        SecondEncoded, Second, Distance, NeedPositions,
                        NextCount, NextAcc
                    )
            end
    end.

fts2_search_stream_collect(true, Position, Acc) -> [Position | Acc];
fts2_search_stream_collect(false, _Position, Acc) -> Acc.

%% Generations written before chunk-ordered position rows cannot be merged
%% cursor against cursor, so the candidates come from the entry planes and the
%% wanted payloads are selected in one pass per term. Verification is the same
%% streaming walk: even here no position list is materialised.
fts2_search_pair_legacy(
    FirstEntries, FirstHeader, FirstPosition, First,
    SecondEntries, SecondHeader, SecondPosition, Second,
    Verify, NeedPositions
) ->
    Pairs = fts2_search_pair_legacy_entries(FirstEntries, SecondEntries, []),
    ChunkIds = [element(1, Pair) || Pair <- Pairs],
    FirstPayloads = fts2_search_header_position_payloads(
        FirstHeader, FirstPosition, ChunkIds, First
    ),
    SecondPayloads = fts2_search_header_position_payloads(
        SecondHeader, SecondPosition, ChunkIds, Second
    ),
    [
        {ChunkId, GroupId, SourceId, Length, FirstTf, SecondTf, MatchCount,
            Starts}
     || {ChunkId, GroupId, SourceId, Length, FirstTf, SecondTf} <- Pairs,
        {FirstCount, FirstEncoded} <- [
            maps:get(ChunkId, FirstPayloads, {0, <<>>})
        ],
        {SecondCount, SecondEncoded} <- [
            maps:get(ChunkId, SecondPayloads, {0, <<>>})
        ],
        {MatchCount, Starts} <- [
            fts2_search_pair_verify(
                Verify, FirstCount, FirstEncoded, SecondCount, SecondEncoded,
                NeedPositions
            )
        ],
        MatchCount > 0
    ].

fts2_search_header_position_payloads(Header, _External, ChunkIds, _Token) when
    is_map_key(positions_packed, Header)
->
    fts2_codec_select_position_payloads(
        fts2_codec_decode_header_tail(maps:get(positions_packed, Header)),
        ChunkIds
    );
fts2_search_header_position_payloads(_Header, {ok, Value}, ChunkIds, _Token) ->
    fts2_codec_select_position_payloads(Value, ChunkIds);
fts2_search_header_position_payloads(_Header, Bad, _ChunkIds, Token) ->
    erlang:error({invalid_fts2_position_row, Token, Bad}).

%% Entry planes are chunk ordered in every generation, so the candidate walk
%% uses the same allocation-free skip runs as the chunk-ordered merge.
fts2_search_pair_legacy_entries(<<>>, _SecondEntries, Acc) ->
    lists:reverse(Acc);
fts2_search_pair_legacy_entries(
    <<FirstChunk:32/unsigned-big, _/binary>> = FirstEntries, SecondEntries, Acc
) ->
    case fts2_search_entry_advance(SecondEntries, FirstChunk) of
        done ->
            lists:reverse(Acc);
        {FirstChunk, Matched} ->
            fts2_search_pair_legacy_row(FirstEntries, Matched, Acc);
        {SecondChunk, Ahead} ->
            case fts2_search_entry_advance(FirstEntries, SecondChunk) of
                done ->
                    lists:reverse(Acc);
                {SecondChunk, Caught} ->
                    fts2_search_pair_legacy_row(Caught, Ahead, Acc);
                {_FirstChunk, Advanced} ->
                    fts2_search_pair_legacy_entries(Advanced, Ahead, Acc)
            end
    end.

fts2_search_pair_legacy_row(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        FirstTf:32/unsigned-big, _FirstDense:32/unsigned-big,
        FirstRest/binary>>,
    <<_SecondChunk:32/unsigned-big, _SecondGroup:32/unsigned-big,
        _SecondSource:64/unsigned-big, _SecondLength:32/unsigned-big,
        SecondTf:32/unsigned-big, _SecondDense:32/unsigned-big,
        SecondRest/binary>>,
    Acc
) ->
    fts2_search_pair_legacy_entries(
        FirstRest, SecondRest,
        [{ChunkId, GroupId, SourceId, Length, FirstTf, SecondTf} | Acc]
    ).

fts2_search_entry_advance(
    <<ChunkId:32/unsigned-big, _:24/binary, Rest/binary>>, Target
) when ChunkId < Target ->
    fts2_search_entry_advance(Rest, Target);
fts2_search_entry_advance(
    <<ChunkId:32/unsigned-big, _/binary>> = Entries, _Target
) ->
    {ChunkId, Entries};
fts2_search_entry_advance(<<>>, _Target) ->
    done.

fts2_search_header_position_plane(Header, _External, _Token) when
    is_map_key(positions_packed, Header)
->
    fts2_codec_decode_header_tail(maps:get(positions_packed, Header));
fts2_search_header_position_plane(_Header, {ok, Value}, _Token) ->
    Value;
fts2_search_header_position_plane(_Header, Bad, Token) ->
    erlang:error({invalid_fts2_position_row, Token, Bad}).

fts2_search_header_entry_plane(Header, _External, _Token) when
    is_map_key(entries_packed, Header)
->
    fts2_codec_header_entry_plane(Header);
fts2_search_header_entry_plane(_Header, {ok, Value}, _Token) ->
    fts2_codec_plane_payload(Value);
fts2_search_header_entry_plane(_Header, Bad, Token) ->
    erlang:error({invalid_fts2_boolean_row, Token, Bad}).

fts2_search_header_positions(Header, _External, ChunkIds, _Token) when
    is_map_key(positions_packed, Header)
->
    fts2_codec_header_positions(Header, ChunkIds);
fts2_search_header_positions(_Header, {ok, Value}, ChunkIds, _Token) ->
    fts2_codec_decode_positions(Value, ChunkIds);
fts2_search_header_positions(_Header, Bad, _ChunkIds, Token) ->
    erlang:error({invalid_fts2_position_row, Token, Bad}).

fts2_search_fast_near(
    Bookie,
    Schema,
    Root,
    [{term, First, false, _}, {term, Second, false, _}],
    Distance,
    Columns,
    Opts
) ->
    Window = maps:get(offset, Opts, 0) + maps:get(limit, Opts, ?DEFAULT_LIMIT),
    case {
        fts2_search_selector_ids(Columns, Schema),
        maps:get(rank, Opts, none),
        maps:get(impact_facet, Opts, nil),
        Window =< 256
    } of
        {[Column], bm25, nil, true} ->
            NeedPositions = maps:get(return_positions, Opts, false),
            case fts2_search_positional_pair(
                Bookie, Schema, Root, Column, First, Second,
                {near, Distance}, NeedPositions
            ) of
                {FirstHeader, SecondHeader, Rows} ->
                    fts2_search_rank_positional_groups(
                        Bookie, Schema, Root, Column, First, Second,
                        FirstHeader, SecondHeader, Rows, near
                    );
                not_found ->
                    {ranked_champions, [], 0}
            end;
        _ ->
            no
    end;
fts2_search_fast_near(
    _Bookie, _Schema, _Root, _Items, _Distance, _Columns, _Opts
) ->
    no.

fts2_search_boolean_terms(AST, Schema) ->
    case fts2_search_boolean_terms(AST, Schema, undefined, []) of
        {ok, Column, Tokens} -> {ok, Column, lists:usort(Tokens)};
        no -> no
    end.

fts2_search_boolean_terms({term, Token, false, Columns}, Schema, Column0, Acc) ->
    case fts2_search_selector_ids(Columns, Schema) of
        [Column] when Column0 =:= undefined; Column =:= Column0 ->
            {ok, Column, [Token | Acc]};
        _ -> no
    end;
fts2_search_boolean_terms({Op, A, B}, Schema, Column0, Acc) when
    Op =:= 'and'; Op =:= 'or'; Op =:= 'not'
->
    case fts2_search_boolean_terms(A, Schema, Column0, Acc) of
        {ok, Column, Acc1} ->
            fts2_search_boolean_terms(B, Schema, Column, Acc1);
        no -> no
    end;
fts2_search_boolean_terms(_AST, _Schema, _Column, _Acc) ->
    no.

fts2_search_boolean_pure_or(
    {'or', {term, _First, false, _}, {term, _Second, false, _}}
) ->
    true;
fts2_search_boolean_pure_or(_AST) ->
    false.

fts2_search_plane_chunk_bitmap(<<>>, Bitmap) ->
    Bitmap;
fts2_search_plane_chunk_bitmap(
    <<_ChunkId:32/unsigned-big, _GroupId:32/unsigned-big,
        _SourceId:64/unsigned-big, _Length:32/unsigned-big,
        _Tf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    Bitmap
) ->
    fts2_search_plane_chunk_bitmap(Rest, Bitmap bor (1 bsl DenseId)).

fts2_search_header_group_bitmap(Header) ->
    case maps:find(group_bitmap, Header) of
        {ok, Bitmap} ->
            Bitmap;
        error ->
            lists:foldl(
                fun(Entry, Bitmap) ->
                    Bitmap bor (1 bsl element(2, Entry))
                end,
                0,
                maps:get(champions, Header)
            )
    end.

fts2_search_boolean_bitmap({term, Token, false, _Columns}, TermData) ->
    {_Df, _EntryPlane, Bitmap} = maps:get(Token, TermData),
    Bitmap;
fts2_search_boolean_bitmap({'and', A, B}, TermData) ->
    fts2_search_boolean_bitmap(A, TermData) band
        fts2_search_boolean_bitmap(B, TermData);
fts2_search_boolean_bitmap({'or', A, B}, TermData) ->
    fts2_search_boolean_bitmap(A, TermData) bor
        fts2_search_boolean_bitmap(B, TermData);
fts2_search_boolean_bitmap({'not', A, B}, TermData) ->
    fts2_search_boolean_bitmap(A, TermData) band
        bnot fts2_search_boolean_bitmap(B, TermData).

fts2_search_boolean_positive_tokens(AST) ->
    lists:usort(fts2_search_boolean_positive_tokens(AST, true, [])).

fts2_search_boolean_positive_tokens(
    {term, Token, false, _Columns}, true, Acc
) ->
    [Token | Acc];
fts2_search_boolean_positive_tokens(
    {term, _Token, false, _Columns}, false, Acc
) ->
    Acc;
fts2_search_boolean_positive_tokens({Op, A, B}, Positive, Acc) when
    Op =:= 'and'; Op =:= 'or'
->
    fts2_search_boolean_positive_tokens(
        B,
        Positive,
        fts2_search_boolean_positive_tokens(A, Positive, Acc)
    );
fts2_search_boolean_positive_tokens({'not', A, B}, Positive, Acc) ->
    fts2_search_boolean_positive_tokens(
        B,
        false,
        fts2_search_boolean_positive_tokens(A, Positive, Acc)
    ).

fts2_search_boolean_select_plane(<<>>, _Result, _Column, _Token, _Df, Acc) ->
    Acc;
fts2_search_boolean_select_plane(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        Tf:32/unsigned-big, DenseId:32/unsigned-big, Rest/binary>>,
    Result,
    Column,
    Token,
    Df,
    Acc
) ->
    ByteIndex = DenseId bsr 3,
    Selected = ByteIndex < byte_size(Result) andalso
        (binary:at(Result, ByteIndex) band (1 bsl (DenseId band 7))) =/= 0,
    NextAcc = case Selected of
        false ->
            Acc;
        true ->
            Stat = {{Column, Token}, Tf, base, Df},
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => {
                        ChunkId, GroupId, SourceId, Length, [Stat], Tf
                    }};
                {ok, {StoredChunkId, GroupId, StoredSourceId, StoredLength,
                        Stats, TotalTf}} ->
                    Acc#{GroupId => {
                        StoredChunkId,
                        GroupId,
                        StoredSourceId,
                        StoredLength,
                        [Stat | Stats],
                        TotalTf + Tf
                    }}
            end
    end,
    fts2_search_boolean_select_plane(
        Rest, Result, Column, Token, Df, NextAcc
    ).

fts2_search_boolean_select_sparse(Plane, Result, Column, Token, Df, Acc) ->
    DenseIds = fts2_search_bitmap_dense_ids(Result, 0, []),
    lists:foldl(
        fun(DenseId, RowAcc) ->
            case fts2_search_plane_find_dense(
                Plane, DenseId, 0, byte_size(Plane) div 28
            ) of
                not_found ->
                    RowAcc;
                {ChunkId, GroupId, SourceId, Length, Tf, DenseId} ->
                    Stat = {{Column, Token}, Tf, base, Df},
                    case maps:find(GroupId, RowAcc) of
                        error ->
                            RowAcc#{GroupId => {
                                ChunkId, GroupId, SourceId, Length, [Stat], Tf
                            }};
                        {ok, {StoredChunkId, GroupId, StoredSourceId,
                                StoredLength, Stats, TotalTf}} ->
                            RowAcc#{GroupId => {
                                StoredChunkId, GroupId, StoredSourceId,
                                StoredLength,
                                [Stat | Stats], TotalTf + Tf
                            }}
                    end
            end
        end,
        Acc,
        DenseIds
    ).

fts2_search_bitmap_dense_ids(<<>>, _Base, Acc) ->
    lists:reverse(Acc);
fts2_search_bitmap_dense_ids(<<Byte, Rest/binary>>, Base, Acc) ->
    Next = fts2_search_bitmap_byte_ids(Byte, Base, 0, Acc),
    fts2_search_bitmap_dense_ids(Rest, Base + 8, Next).

fts2_search_bitmap_byte_ids(_Byte, _Base, 8, Acc) ->
    Acc;
fts2_search_bitmap_byte_ids(Byte, Base, Bit, Acc) ->
    Next = case Byte band (1 bsl Bit) of
        0 -> Acc;
        _ -> [Base + Bit | Acc]
    end,
    fts2_search_bitmap_byte_ids(Byte, Base, Bit + 1, Next).

fts2_search_plane_find_dense(_Plane, _DenseId, Low, High) when Low >= High ->
    not_found;
fts2_search_plane_find_dense(Plane, DenseId, Low, High) ->
    Mid = (Low + High) bsr 1,
    Offset = Mid * 28,
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, Length:32/unsigned-big,
        Tf:32/unsigned-big, Found:32/unsigned-big>> =
            binary:part(Plane, Offset, 28),
    case Found of
        DenseId ->
            {ChunkId, GroupId, SourceId, Length, Tf, Found};
        _ when Found < DenseId ->
            fts2_search_plane_find_dense(
                Plane, DenseId, Mid + 1, High
            );
        _ ->
            fts2_search_plane_find_dense(Plane, DenseId, Low, Mid)
    end.

%% Champion lists are stored in descending impact order.  Keep the requested
%% window and its complete boundary tie before any match record is built.
fts2_search_impact_window(Entries, Window, ScoreFun) when Window >= 1 ->
    fts2_search_impact_window(Entries, Window, ScoreFun, 0.0, []);
fts2_search_impact_window(Entries, _Window, _ScoreFun) ->
    Entries.

fts2_search_impact_window([], _Window, _ScoreFun, _Boundary, Acc) ->
    lists:reverse(Acc);
fts2_search_impact_window([Entry | Rest], Window, ScoreFun, Boundary, Acc) ->
    Score = ScoreFun(Entry),
    if
        Window > 0 ->
            fts2_search_impact_window(
                Rest, Window - 1, ScoreFun, Score, [Entry | Acc]
            );
        Score >= Boundary ->
            fts2_search_impact_window(
                Rest, 0, ScoreFun, Boundary, [Entry | Acc]
            );
        true ->
            lists:reverse(Acc)
    end.

fts2_search_term_impact(Df, Root) ->
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    Idf = fts2_build_bm25_idf(DocCount, Df),
    fun({_ChunkId, _GroupId, _SourceId, Length, Tf, _GroupTf}) ->
        fts2_search_bm25(Tf, Idf, Avg, Length)
    end.

fts2_search_ranked_champions(Entries, Column, Token, Df, PositionMap, Root) ->
    DocCount = erlang:max(maps:get(group_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    Idf = fts2_build_bm25_idf(DocCount, Df),
    Term = {Column, Token},
    [
        #fts2_match{
            chunk_id = ChunkId,
            group_id = GroupId,
            source_id = SourceId,
            doc_length = Length,
            tf = Tf,
            term_stats = [{Term, Tf, base, Df}],
            terms = [Token],
            columns = [{Term, Positions}],
            match_positions = [{Token, Positions}],
            group_match_count = GroupTf,
            score = fts2_search_bm25(Tf, Idf, Avg, Length)
        }
     || {ChunkId, GroupId, SourceId, Length, Tf, GroupTf} <- Entries,
        Positions <- [maps:get(ChunkId, PositionMap, [])]
    ].

fts2_search_merge_champion_match(
    #fts2_match{group_id = GroupId} = Match, Acc
) ->
    case maps:find(GroupId, Acc) of
        error ->
            Acc#{GroupId => Match};
        {ok, Existing} ->
            Base = case Match#fts2_match.chunk_id <
                Existing#fts2_match.chunk_id of
                true -> Match;
                false -> Existing
            end,
            Acc#{GroupId => Base#fts2_match{
                tf = Existing#fts2_match.tf + Match#fts2_match.tf,
                term_stats = Existing#fts2_match.term_stats ++
                    Match#fts2_match.term_stats,
                terms = Existing#fts2_match.terms ++ Match#fts2_match.terms,
                group_match_count =
                    fts2_search_group_match_count(Existing) +
                        fts2_search_group_match_count(Match),
                score = Existing#fts2_match.score + Match#fts2_match.score
            }}
    end.

fts2_search_bitmap_count(0) ->
    0;
fts2_search_bitmap_count(Bitmap) ->
    fts2_search_bitmap_count_binary(binary:encode_unsigned(Bitmap), 0).

fts2_search_bitmap_count_binary(<<>>, Count) ->
    Count;
fts2_search_bitmap_count_binary(<<Byte, Rest/binary>>, Count) ->
    Nibbles = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4},
    Bits = element((Byte band 15) + 1, Nibbles) +
        element((Byte bsr 4) + 1, Nibbles),
    fts2_search_bitmap_count_binary(Rest, Count + Bits).

fts2_search_can_page_before_identity(Opts, true) ->
    maps:get(impact_facet, Opts, nil) =:= nil;
fts2_search_can_page_before_identity(Opts, false) ->
    maps:get(impact_facet, Opts, nil) =:= nil andalso
        maps:get(resolve_hits, Opts, true) =:= false.

fts2_search_prepage_matches(Matches, false, Offset, Limit) ->
    {lists:sublist(fts2_search_drop(Offset, fts2_search_order_matches(Matches, false)), Limit), 0};
fts2_search_prepage_matches(Matches, true, Offset, Limit) ->
    Ordered = fts2_search_order_matches(Matches, true),
    case lists:sublist(fts2_search_drop(Offset, Ordered), Limit) of
        [] ->
            {[], 0};
        Page ->
            StartScore = (hd(Page))#fts2_match.score,
            EndScore = (lists:last(Page))#fts2_match.score,
            Higher = length([
                Match
             || Match <- Ordered,
                Match#fts2_match.score > StartScore
            ]),
            TiedWindow = [
                Match
             || Match <- Ordered,
                Match#fts2_match.score =< StartScore,
                Match#fts2_match.score >= EndScore
            ],
            {TiedWindow, Offset - Higher}
    end.

fts2_search_order_matches(Matches, true) ->
    [
        Match
     || {_Key, Match} <- lists:keysort(
            1,
            [
                {{-Match#fts2_match.score, Match#fts2_match.source_id}, Match}
             || Match <- Matches
            ]
        )
    ];
fts2_search_order_matches(Matches, false) ->
    [
        Match
     || {_SourceId, Match} <- lists:keysort(
            1,
            [{Match#fts2_match.source_id, Match} || Match <- Matches]
        )
    ].

fts2_search_enrich_base_groups(_Bookie, _Schema, undefined, Matches) ->
    Matches;
fts2_search_enrich_base_groups(Bookie, Schema, Root, Matches) ->
    GroupIds = lists:usort([
        Meta#fts2_match.group_id
     || Meta <- maps:values(Matches)
    ]),
    Identities = fts2_search_read_identities(Bookie, Schema, Root, GroupIds),
    VersionField = maps:get(candidate_version_field, Schema, undefined),
    maps:map(
        fun(_ChunkId, Meta) ->
            Group = maps:get(Meta#fts2_match.group_id, Identities),
            SourceId = Meta#fts2_match.source_id,
            [Chunk] = [
                C
             || C <- maps:get(chunks, Group),
                maps:get(source_id, C) =:= SourceId
            ],
            Candidate = maps:get(candidate_record, Chunk),
            Version = case VersionField of
                undefined -> 0;
                _ -> maps:get(VersionField, Candidate, 0)
            end,
            Meta#fts2_match{
                logical_group = maps:get(group_key, Group),
                group_version = Version
            }
        end,
        Matches
    ).

fts2_search_score_root(undefined, Deltas, Schema) ->
    Live = fts2_delta_live_documents(Deltas),
    #{
        chunk_count => map_size(Live),
        group_count => map_size(fts2_delta_document_groups(Live, Schema)),
        total_length => lists:sum([
            maps:get(doc_length, Document)
         || Document <- maps:values(Live)
        ])
    };
fts2_search_score_root(Root, [], _Schema) ->
    Root;
fts2_search_score_root(Root, Deltas, _Schema) ->
    Live = fts2_delta_live_documents(Deltas),
    Removed = [
        Delta
     || {_RowId, Delta} <- Deltas,
        maps:get(status, Delta) =:= remove orelse
            (maps:get(status, Delta) =:= live andalso
                maps:get(retired_ids, Delta, []) =:= [] andalso
                is_integer(maps:get(base_length, Delta, none)))
    ],
    Root#{
        chunk_count => erlang:max(
            0,
            maps:get(chunk_count, Root) - length(Removed) + map_size(Live)
        ),
        total_length => erlang:max(
            0,
            maps:get(total_length, Root) -
                lists:sum([maps:get(doc_length, D) || D <- Removed]) +
                lists:sum([
                    maps:get(doc_length, Document)
                 || Document <- maps:values(Live)
                ])
        )
    }.

fts2_search_eval(_Bookie, _Schema, _Root, {empty}, _NeedPositions, _Wanted) ->
    #{};
fts2_search_eval(Bookie, Schema, Root, {all_docs}, _NeedPositions, Wanted) ->
    fts2_search_all_chunks(Bookie, Schema, Root, Wanted);
fts2_search_eval(
    Bookie, Schema, Root, {term, Token, Prefix, Columns}, NeedPositions, Wanted
) ->
    fts2_search_read_term(
        Bookie, Schema, Root, Token, Prefix, Columns, NeedPositions, Wanted
    );
fts2_search_eval(Bookie, Schema, Root, {phrase, Specs, Columns}, _NeedPositions, Wanted) ->
    fts2_search_eval_phrase(Bookie, Schema, Root, Specs, Columns, Wanted);
fts2_search_eval(
    Bookie,
    Schema,
    Root,
    {near, Items, Distance, Columns},
    _NeedPositions,
    Wanted
) ->
    fts2_search_eval_near(Bookie, Schema, Root, Items, Distance, Columns, Wanted);
fts2_search_eval(
    Bookie,
    Schema,
    Root,
    {anchor, {term, Token, false, Columns}},
    _NeedPositions,
    Wanted
) ->
    lists:foldl(
        fun(Column, Acc) ->
            fts2_search_read_anchor(Bookie, Schema, Root, Column, Token, Wanted, Acc)
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    );
fts2_search_eval(Bookie, Schema, Root, {anchor, Child}, NeedPositions, Wanted) ->
    maps:filter(
        fun(_ChunkId, Meta) ->
            lists:member(
                0,
                fts2_search_flatten_positions(
                    Meta#fts2_match.match_positions
                )
            )
        end,
        fts2_search_eval(Bookie, Schema, Root, Child, NeedPositions, Wanted)
    );
fts2_search_eval(Bookie, Schema, Root, {'and', A, B}, NeedPositions, Wanted) ->
    fts2_search_intersect(
        fts2_search_eval(Bookie, Schema, Root, A, NeedPositions, Wanted),
        fts2_search_eval(Bookie, Schema, Root, B, NeedPositions, Wanted)
    );
fts2_search_eval(Bookie, Schema, Root, {'or', A, B}, NeedPositions, Wanted) ->
    fts2_search_union(
        fts2_search_eval(Bookie, Schema, Root, A, NeedPositions, Wanted),
        fts2_search_eval(Bookie, Schema, Root, B, NeedPositions, Wanted)
    );
fts2_search_eval(Bookie, Schema, Root, {'not', A, B}, NeedPositions, Wanted) ->
    Positive = fts2_search_eval(Bookie, Schema, Root, A, NeedPositions, Wanted),
    Negative = fts2_search_eval(Bookie, Schema, Root, B, false, Wanted),
    maps:without(maps:keys(Negative), Positive).

%% Grouped evaluation is deliberately keyed by logical group from its first
%% term read.  Boolean AND/OR/NOT therefore operates at the same grain as a
%% document engine even when different chunks satisfy different clauses.
fts2_search_eval_grouped(
    _Bookie, _Schema, _Root, {empty}, _NeedPositions, _Wanted
) ->
    #{};
fts2_search_eval_grouped(
    Bookie, Schema, Root, {all_docs}, _NeedPositions, Wanted
) ->
    fts2_search_group_chunk_matches(
        Bookie, Schema, Root,
        fts2_search_all_chunks(Bookie, Schema, Root, Wanted)
    );
fts2_search_eval_grouped(
    Bookie, Schema, Root, {term, Token, Prefix, Columns}, NeedPositions, Wanted
) ->
    Groups = fts2_search_read_group_term(
        Bookie, Schema, Root, Token, Prefix, Columns, Wanted
    ),
    case NeedPositions of
        false -> Groups;
        true -> fts2_search_attach_group_term_positions(
            Groups,
            fts2_search_read_term(
                Bookie, Schema, Root, Token, Prefix, Columns, true, Wanted
            )
        )
    end;
fts2_search_eval_grouped(
    Bookie, Schema, Root, {phrase, Specs, Columns}, _NeedPositions, Wanted
) ->
    Groups = fts2_search_group_chunk_matches(
        Bookie, Schema, Root,
        fts2_search_eval_phrase(Bookie, Schema, Root, Specs, Columns, Wanted)
    ),
    fts2_search_enrich_group_score_terms(
        Bookie, Schema, Root,
        [{term, Token, Prefix, Columns}
         || {Token, Prefix, _Offset} <- Specs],
        Wanted, Groups
    );
fts2_search_eval_grouped(
    Bookie, Schema, Root, {near, Items, Distance, Columns}, _NeedPositions, Wanted
) ->
    Groups = fts2_search_group_chunk_matches(
        Bookie, Schema, Root,
        fts2_search_eval_near(
            Bookie, Schema, Root, Items, Distance, Columns, Wanted
        )
    ),
    fts2_search_enrich_group_score_terms(
        Bookie, Schema, Root,
        fts2_search_group_score_specs(Items, Columns),
        Wanted, Groups
    );
fts2_search_eval_grouped(
    Bookie, Schema, Root, {anchor, Child}, NeedPositions, Wanted
) ->
    fts2_search_group_chunk_matches(
        Bookie, Schema, Root,
        fts2_search_eval(
            Bookie, Schema, Root, {anchor, Child}, NeedPositions, Wanted
        )
    );
fts2_search_eval_grouped(
    Bookie, Schema, Root, {'and', A, B}, NeedPositions, Wanted
) ->
    fts2_search_intersect(
        fts2_search_eval_grouped(
            Bookie, Schema, Root, A, NeedPositions, Wanted
        ),
        fts2_search_eval_grouped(
            Bookie, Schema, Root, B, NeedPositions, Wanted
        )
    );
fts2_search_eval_grouped(
    Bookie, Schema, Root, {'or', A, B}, NeedPositions, Wanted
) ->
    fts2_search_union(
        fts2_search_eval_grouped(
            Bookie, Schema, Root, A, NeedPositions, Wanted
        ),
        fts2_search_eval_grouped(
            Bookie, Schema, Root, B, NeedPositions, Wanted
        )
    );
fts2_search_eval_grouped(
    Bookie, Schema, Root, {'not', A, B}, NeedPositions, Wanted
) ->
    Positive = fts2_search_eval_grouped(
        Bookie, Schema, Root, A, NeedPositions, Wanted
    ),
    Negative = fts2_search_eval_grouped(
        Bookie, Schema, Root, B, false, Wanted
    ),
    maps:without(maps:keys(Negative), Positive).

fts2_search_read_group_term(
    Bookie, Schema, Root, Token, false, Columns, Wanted
) ->
    lists:foldl(
        fun(Column, Acc) ->
            fts2_search_merge_group_term_row(
                Bookie, Schema, Root, Column, Token, Wanted, Acc
            )
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    );
fts2_search_read_group_term(
    Bookie, #{index := Bucket} = Schema, Root, Prefix, true, Columns, Wanted
) ->
    Generation = maps:get(generation, Root),
    lists:foldl(
        fun(Column, ColumnAcc) ->
            Headers = fts2_search_prefix_headers(
                Bookie, Bucket, Generation, Column, Prefix,
                fun fts2_codec_decode_header_metadata/1
            ),
            Requests = [
                {fts2_codec_term_plane_key(Key, boolean), <<"g">>}
             || {Key, _Token, _Header} <- Headers
            ],
            Values = case Requests of
                [] -> [];
                _ -> leveled_fts_residency:headonly_many(
                    Bookie, Bucket, Requests
                )
            end,
            lists:foldl(
                fun({{_Key, Token, Header}, Value}, Acc) ->
                    Entries = case Value of
                        {ok, Encoded} -> fts2_codec_decode_plane(Encoded);
                        Bad -> erlang:error({
                            invalid_fts2_group_plane, Token, Bad
                        })
                    end,
                    fts2_search_add_group_entries(
                        Entries, Column, Token,
                        maps:get(group_df, Header), Wanted, Acc
                    )
                end,
                ColumnAcc,
                lists:zip(Headers, Values)
            )
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    ).

fts2_search_merge_group_term_row(
    Bookie, #{index := Bucket}, Root, Column, Token, Wanted, Acc
) ->
    {Key, _} = fts2_codec_term_key(
        maps:get(generation, Root), Column, Token
    ),
    GroupKey = fts2_codec_term_plane_key(Key, boolean),
    case leveled_fts_residency:headonly_many(
        Bookie, Bucket, [{Key, <<"h">>}, {GroupKey, <<"g">>}]
    ) of
        [not_found, not_found] ->
            Acc;
        [{ok, HeaderValue}, {ok, GroupValue}] ->
            Header = fts2_codec_decode_header_metadata(HeaderValue),
            fts2_search_add_group_entries(
                fts2_codec_decode_plane(GroupValue), Column, Token,
                maps:get(group_df, Header), Wanted, Acc
            );
        Bad ->
            erlang:error({invalid_fts2_group_rows, Token, Bad})
    end.

fts2_search_add_group_entries(
    Entries, Column, Token, Df, Wanted, Acc
) ->
    lists:foldl(
        fun({ChunkId, GroupId, SourceId, GroupLength, GroupTf, _DenseId}, Inner) ->
            case fts2_search_wanted(SourceId, Wanted) of
                false ->
                    Inner;
                true ->
                    Term = {Column, Token},
                    Meta = #fts2_match{
                        chunk_id = ChunkId,
                        group_id = GroupId,
                        source_id = SourceId,
                        doc_length = GroupLength,
                        tf = GroupTf,
                        term_stats = [{Term, GroupTf, base, Df}],
                        terms = [Token],
                        columns = [{Term, []}],
                        match_positions = [{Token, []}],
                        group_match_count = GroupTf
                    },
                    case maps:find(GroupId, Inner) of
                        error -> Inner#{GroupId => Meta};
                        {ok, Existing} -> Inner#{
                            GroupId => fts2_search_merge_meta(Existing, Meta)
                        }
                    end
            end
        end,
        Acc,
        Entries
    ).

fts2_search_attach_group_term_positions(Groups, ChunkMatches) ->
    PositionsByGroup = maps:fold(
        fun(_ChunkId, Meta, Acc) ->
            GroupId = Meta#fts2_match.group_id,
            Existing = maps:get(GroupId, Acc, #{}),
            PositionMap = lists:foldl(
                fun({Term, Positions}, Inner) ->
                    Inner#{Term => maps:get(Term, Inner, []) ++ Positions}
                end,
                Existing,
                Meta#fts2_match.columns
            ),
            Acc#{GroupId => PositionMap}
        end,
        #{},
        ChunkMatches
    ),
    maps:map(
        fun(GroupId, Meta) ->
            PositionMap = maps:get(GroupId, PositionsByGroup, #{}),
            Columns = [
                {Term, maps:get(Term, PositionMap, [])}
             || {Term, _Positions} <- Meta#fts2_match.columns
            ],
            MatchPositions = [
                {Token, lists:append([
                    Positions
                 || {{_Column, PositionToken}, Positions} <-
                        maps:to_list(PositionMap),
                    PositionToken =:= Token
                ])}
             || Token <- Meta#fts2_match.terms
            ],
            Meta#fts2_match{
                columns = Columns,
                match_positions = MatchPositions
            }
        end,
        Groups
    ).

fts2_search_group_score_specs(Items, DefaultColumns) ->
    lists:append([
        fts2_search_group_score_spec(Item, DefaultColumns)
     || Item <- Items
    ]).

fts2_search_group_score_spec(
    {term, Token, Prefix, Columns}, _DefaultColumns
) ->
    [{term, Token, Prefix, Columns}];
fts2_search_group_score_spec(
    {phrase, Specs, Columns}, _DefaultColumns
) ->
    [{term, Token, Prefix, Columns}
     || {Token, Prefix, _Offset} <- Specs];
fts2_search_group_score_spec({anchor, Child}, DefaultColumns) ->
    fts2_search_group_score_spec(Child, DefaultColumns);
fts2_search_group_score_spec(
    {Op, A, B}, DefaultColumns
) when Op =:= 'and'; Op =:= 'or'; Op =:= 'not' ->
    fts2_search_group_score_spec(A, DefaultColumns) ++
        fts2_search_group_score_spec(B, DefaultColumns);
fts2_search_group_score_spec(
    {near, Nested, _Distance, Columns}, _DefaultColumns
) ->
    fts2_search_group_score_specs(Nested, Columns);
fts2_search_group_score_spec(_Other, _DefaultColumns) ->
    [].

fts2_search_enrich_group_score_terms(
    _Bookie, _Schema, _Root, [], _Wanted, Groups
) ->
    Groups;
fts2_search_enrich_group_score_terms(
    Bookie, Schema, Root, Specs0, Wanted, Groups
) ->
    Specs = lists:usort(Specs0),
    TermMaps = [
        fts2_search_read_group_term(
            Bookie, Schema, Root, Token, Prefix, Columns, Wanted
        )
     || {term, Token, Prefix, Columns} <- Specs
    ],
    maps:map(
        fun(GroupId, MatchMeta) ->
            Stats = [
                maps:get(GroupId, TermMap)
             || TermMap <- TermMaps,
                maps:is_key(GroupId, TermMap)
            ],
            case Stats of
                [] ->
                    MatchMeta;
                [First | Rest] ->
                    ScoreMeta = lists:foldl(
                        fun(Meta, Acc) ->
                            fts2_search_merge_meta(Acc, Meta)
                        end,
                        First,
                        Rest
                    ),
                    MatchMeta#fts2_match{
                        doc_length = ScoreMeta#fts2_match.doc_length,
                        tf = ScoreMeta#fts2_match.tf,
                        term_stats = ScoreMeta#fts2_match.term_stats,
                        terms = ScoreMeta#fts2_match.terms
                    }
            end
        end,
        Groups
    ).

fts2_search_group_chunk_matches(_Bookie, _Schema, undefined, Matches) ->
    maps:from_list([
        {Meta#fts2_match.source_id, Meta}
     || Meta <- fts2_search_collapse_scored_groups(
            maps:values(Matches), false
        )
    ]);
fts2_search_group_chunk_matches(Bookie, Schema, Root, Matches) ->
    Collapsed = fts2_search_collapse_scored_groups(
        maps:values(Matches), false
    ),
    GroupIds = [Meta#fts2_match.group_id || Meta <- Collapsed],
    Identities = fts2_search_read_identities(
        Bookie, Schema, Root, GroupIds
    ),
    maps:from_list([
        begin
            Group = maps:get(GroupId, Identities),
            GroupLength = lists:sum([
                maps:get(doc_length, Chunk, 0)
             || Chunk <- maps:get(chunks, Group, [])
            ]),
            {GroupId, Meta#fts2_match{doc_length = GroupLength}}
        end
     || Meta = #fts2_match{group_id = GroupId} <- Collapsed
    ]).

fts2_search_read_anchor(
    Bookie, #{index := Bucket}, Root, Column, Token, Wanted, Acc
) ->
    {Key, _} = fts2_codec_term_key(
        maps:get(generation, Root), Column, Token
    ),
    case leveled_fts_residency:headonly_many(
        Bookie, Bucket, [{Key, <<"h">>}, {Key, <<"a">>}]
    ) of
        [{ok, HeaderValue}, {ok, AnchorValue}] ->
            Header = fts2_codec_decode_header_metadata(HeaderValue),
            Entries = fts2_codec_decode_plane(AnchorValue),
            PositionMap = maps:from_list([
                {element(1, Entry), [0]} || Entry <- Entries
            ]),
            fts2_search_add_entries(
                Entries,
                Column,
                Token,
                maps:get(chunk_df, Header),
                PositionMap,
                Wanted,
                Acc
            );
        [{ok, _HeaderValue}, not_found] ->
            Acc;
        [not_found, not_found] ->
            Acc;
        Bad ->
            erlang:error({invalid_fts2_anchor_rows, Token, Bad})
    end.

fts2_search_read_term(Bookie, Schema, Root, Token, false, Columns, NeedPositions, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            fts2_search_merge_term_row(
                Bookie, Schema, Root, Column, Token, NeedPositions, Wanted, Acc
            )
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    );
fts2_search_read_term(Bookie, Schema, Root, Prefix, true, Columns, NeedPositions, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            fts2_search_fold_prefix_rows(
                Bookie, Schema, Root, Column, Prefix, NeedPositions, Wanted, Acc
            )
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    ).

fts2_search_merge_term_row(
    Bookie, #{index := Bucket}, Root, Column, Token, NeedPositions, Wanted, Acc
) ->
    Generation = maps:get(generation, Root),
    {Key, _} = fts2_codec_term_key(Generation, Column, Token),
    BooleanKey = fts2_codec_term_plane_key(Key, boolean),
    case
        leveled_fts_residency:headonly_many(
            Bookie, Bucket, [{Key, <<"h">>}, {BooleanKey, <<"b">>}]
        )
    of
        [not_found, not_found] ->
            Acc;
        [{ok, HeaderValue}, BooleanResult] ->
            Header = fts2_codec_decode_header_metadata(HeaderValue),
            Entries = fts2_search_row_entries(
                Bookie, Bucket, Key, Header, BooleanResult, Token
            ),
            fts2_search_note_term_read(Token, NeedPositions),
            PositionMap =
                case NeedPositions of
                    true -> fts2_search_read_positions(
                        Bookie, Bucket, Key, Header,
                        fts2_search_wanted_chunk_ids(Entries, Wanted)
                    );
                    false -> #{}
                end,
            fts2_search_add_entries(
                Entries,
                Column,
                Token,
                maps:get(chunk_df, Header),
                PositionMap,
                Wanted,
                Acc
            );
        Bad ->
            erlang:error({invalid_fts2_term_rows, Token, Bad})
    end.

fts2_search_fold_prefix_rows(
    Bookie,
    #{index := Bucket},
    Root,
    Column,
    Prefix,
    NeedPositions,
    Wanted,
    Acc0
) ->
    Generation = maps:get(generation, Root),
    Headers = fts2_search_prefix_headers(
        Bookie, Bucket, Generation, Column, Prefix,
        fun fts2_codec_decode_header_metadata/1
    ),
    fts2_search_check_cancellation(),
    Requests = lists:append([
        fts2_search_prefix_requests(Key, Header, NeedPositions)
     || {Key, _Token, Header} <- Headers
    ]),
    Values = case Requests of
        [] -> [];
        _ -> leveled_fts_residency:headonly_many(Bookie, Bucket, Requests)
    end,
    External = maps:from_list(lists:zip(Requests, Values)),
    lists:foldl(
        fun({Key, Token, Header}, Acc) ->
            Entries = case fts2_codec_header_has_entries(Header) of
                true -> fts2_codec_header_entries(Header);
                false -> fts2_search_row_entries(
                    Bookie,
                    Bucket,
                    Key,
                    Header,
                    maps:get({fts2_codec_term_plane_key(Key, boolean), <<"b">>},
                        External),
                    Token
                )
            end,
            PositionMap = case NeedPositions of
                false -> #{};
                true -> fts2_search_read_positions(
                    Bookie, Bucket, Key, Header,
                    fts2_search_wanted_chunk_ids(Entries, Wanted)
                )
            end,
            fts2_search_note_term_read(Token, NeedPositions),
            fts2_search_add_entries(
                Entries,
                Column,
                Token,
                maps:get(chunk_df, Header),
                PositionMap,
                Wanted,
                Acc
            )
        end,
        Acc0,
        Headers
    ).

fts2_search_prefix_headers(
    Bookie, Bucket, Generation, Column, Prefix, Decode
) ->
    case leveled_fts_residency:header_values(
        Bookie, Bucket, Generation, Column, Prefix
    ) of
        {resident, Values} ->
            [{Key, Token, Decode(Value)} || {Key, Token, Value} <- Values];
        fallback ->
            {Start, Finish} = fts2_codec_term_range(
                Generation, Column, Prefix
            ),
            Fold = fun
                (B, {Key, <<"h">>}, Value, Acc) when B =:= Bucket ->
                    case Key of
                        <<"f2:t:", Generation:64/unsigned-big,
                            Column:8, Token/binary>> ->
                            [{Key, Token, Decode(Value)} | Acc];
                        _ -> Acc
                    end;
                (_B, _Key, _Value, Acc) -> Acc
            end,
            {async, Runner} = leveled_bookie:book_headfold(
                Bookie,
                ?HEAD_TAG,
                {range, Bucket, {{Start, <<>>}, {Finish, <<255>>}}},
                {Fold, []},
                false,
                true,
                false
            ),
            Runner()
    end.

fts2_search_prefix_requests(Key, Header, NeedPositions) ->
    Boolean = case fts2_codec_header_has_entries(Header) of
        true -> [];
        false -> [{fts2_codec_term_plane_key(Key, boolean), <<"b">>}]
    end,
    Position = case NeedPositions andalso
        not fts2_codec_header_has_positions(Header) of
        true -> [{fts2_codec_term_plane_key(Key, positions), <<"p">>}];
        false -> []
    end,
    Boolean ++ Position.

fts2_search_row_entries(Header, not_found, Token) ->
    case fts2_codec_header_has_entries(Header) of
        true -> fts2_codec_header_entries(Header);
        false -> erlang:error({invalid_fts2_boolean_row, Token, not_found})
    end;
fts2_search_row_entries(_Header, {ok, Value}, _Token) ->
    fts2_codec_decode_plane(Value);
fts2_search_row_entries(_Header, Bad, Token) ->
    erlang:error({invalid_fts2_boolean_row, Token, Bad}).

%% A multi-key SST lookup can conservatively miss a key while its block-index
%% cache is still cold.  A single-key lookup takes the established lookup path
%% and distinguishes that transient miss from a genuinely incomplete immutable
%% generation.  The fallback is paid only for a batch miss.
fts2_search_row_entries(
    Bookie, Bucket, Key, Header, not_found, Token
) ->
    fts2_search_row_entries(
        Header,
        fts2_search_required_head(
            Bookie, Bucket, fts2_codec_term_plane_key(Key, boolean), <<"b">>
        ),
        Token
    );
fts2_search_row_entries(
    _Bookie, _Bucket, _Key, Header, Result, Token
) ->
    fts2_search_row_entries(Header, Result, Token).

fts2_search_required_head(Bookie, Bucket, Key, SubKey) ->
    case leveled_fts_residency:headonly(Bookie, Bucket, Key, SubKey) of
        not_found ->
            %% The first uncached SST lookup also installs the slot's block
            %% index.  Retry once through that now-authoritative index.
            leveled_fts_residency:headonly(Bookie, Bucket, Key, SubKey);
        Result ->
            Result
    end.

fts2_search_read_positions(Bookie, Bucket, Key, Header) ->
    fts2_search_read_positions(Bookie, Bucket, Key, Header, all).

fts2_search_read_positions(Bookie, Bucket, Key, Header, all) ->
    case fts2_codec_header_has_positions(Header) of
        true -> fts2_codec_header_positions(Header);
        false -> fts2_search_position_result(
            leveled_fts_residency:headonly(
                Bookie, Bucket, fts2_codec_term_plane_key(Key, positions), <<"p">>
            ), Key
        )
    end;
fts2_search_read_positions(Bookie, Bucket, Key, Header, ChunkIds) ->
    case fts2_codec_header_has_positions(Header) of
        true -> fts2_codec_header_positions(Header, ChunkIds);
        false -> fts2_search_selected_position_result(
            leveled_fts_residency:headonly(
                Bookie, Bucket, fts2_codec_term_plane_key(Key, positions), <<"p">>
            ), Key,
            ChunkIds
        )
    end.

fts2_search_selected_position_result({ok, Value}, _Token, ChunkIds) ->
    fts2_codec_decode_positions(Value, ChunkIds);
fts2_search_selected_position_result(Bad, Token, _ChunkIds) ->
    erlang:error({invalid_fts2_position_row, Token, Bad}).

fts2_search_wanted_chunk_ids(_Entries, all) ->
    all;
fts2_search_wanted_chunk_ids(_Entries, {all_except, _Excluded}) ->
    all;
fts2_search_wanted_chunk_ids(Entries, Wanted) ->
    [
        ChunkId
     || {ChunkId, _GroupId, SourceId, _Length, _Tf, _DenseId} <- Entries,
        fts2_search_wanted(SourceId, Wanted)
    ].

fts2_search_position_result({ok, Value}, _Token) ->
    fts2_codec_decode_positions(Value);
fts2_search_position_result(Bad, Token) ->
    erlang:error({invalid_fts2_position_row, Token, Bad}).

fts2_search_add_entries(Entries, Column, Token, Df0, PositionMap, Wanted, Acc) ->
    Selected = [
        Entry
     || {_ChunkId, _GroupId, SourceId, _Length, _Tf, _DenseId} = Entry <- Entries,
        fts2_search_wanted(SourceId, Wanted)
    ],
    Df = case Df0 of
        StoredDf when is_integer(StoredDf) -> StoredDf;
        _ -> length(Selected)
    end,
    fts2_search_note_plane_decode(),
    lists:foldl(
        fun({ChunkId, GroupId, SourceId, Length, Tf, _DenseId}, Inner) ->
            Positions = maps:get(ChunkId, PositionMap, []),
            Term = {Column, Token},
            Meta = #fts2_match{
                chunk_id = ChunkId,
                group_id = GroupId,
                source_id = SourceId,
                doc_length = Length,
                tf = Tf,
                term_stats = [{Term, Tf, base, Df}],
                terms = [Token],
                columns = [{Term, Positions}],
                match_positions = [{Token, Positions}]
            },
            case maps:find(ChunkId, Inner) of
                error ->
                    Inner#{ChunkId => Meta};
                {ok, Existing} ->
                    Inner#{ChunkId => fts2_search_merge_meta(Existing, Meta)}
            end
        end,
        Acc,
        Selected
    ).

fts2_search_eval_phrase(Bookie, Schema, Root, Specs, Columns, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            Strategy = maps:get(
                phrase_strategy,
                Schema,
                maps:get(phrase_strategy, Root, skip)
            ),
            ColumnMatches = case fts2_phrase_evaluate(
                Strategy, Bookie, Schema, Root, Specs, Column, Wanted
            ) of
                {matches, Matches} -> Matches;
                {positions, PositionWanted} ->
                    fts2_search_eval_phrase_positions(
                        Bookie,
                        Schema,
                        Root,
                        Specs,
                        Column,
                        PositionWanted
                    )
            end,
            fts2_search_union(Acc, ColumnMatches)
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    ).

fts2_search_eval_phrase_positions(Bookie, Schema, Root, Specs, Column, Wanted) ->
    TermMaps = [
        fts2_search_read_term(
            Bookie,
            Schema,
            Root,
            Token,
            Prefix,
            [fts2_search_column_name(Column, Schema)],
            true,
            Wanted
        )
     || {Token, Prefix, _Offset} <- Specs
    ],
    Candidates = fts2_search_intersect_many(TermMaps),
    maps:fold(
        fun(ChunkId, Meta, Inner) ->
            Starts = fts2_search_phrase_starts(Meta, Column, Specs),
            case Starts of
                [] ->
                    Inner;
                _ ->
                    Inner#{ChunkId => Meta#fts2_match{
                        match_positions = [{phrase, Starts}],
                        match_count = length(Starts)
                    }}
            end
        end,
        #{},
        Candidates
    ).

fts2_search_phrase_starts(_Meta, _Column, []) ->
    [];
fts2_search_phrase_starts(Meta, Column, [{First, Prefix, FirstOffset} | Rest]) ->
    Positions = fts2_search_column_token_positions(Meta, Column, First, Prefix),
    [
        Position - FirstOffset
     || Position <- Positions,
        fts2_search_phrase_rest(Meta, Column, Rest, Position - FirstOffset)
    ].

fts2_search_phrase_rest(_Meta, _Column, [], _Start) ->
    true;
fts2_search_phrase_rest(Meta, Column, [{Token, Prefix, Offset} | Rest], Start) ->
    lists:member(
        Start + Offset, fts2_search_column_token_positions(Meta, Column, Token, Prefix)
    ) andalso
        fts2_search_phrase_rest(Meta, Column, Rest, Start).

fts2_search_eval_near(Bookie, Schema, Root, Items, Distance, Columns, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            ColumnName = fts2_search_column_name(Column, Schema),
            ItemMaps = [
                fts2_search_eval(
                    Bookie,
                    Schema,
                    Root,
                    fts2_search_restrict_columns(Item, [ColumnName]),
                    true,
                    Wanted
                )
             || Item <- Items
            ],
            Candidates = fts2_search_intersect_many(ItemMaps),
            maps:fold(
                fun(ChunkId, Meta, Inner) ->
                    SpanLists = [
                        fts2_search_item_spans(Meta, Column, Item)
                     || Item <- Items
                    ],
                    Starts = fts2_search_near_positions(SpanLists, Distance),
                    case Starts of
                        [] ->
                            Inner;
                        _ ->
                            Match = Meta#fts2_match{
                                match_positions = [{near, Starts}],
                                match_count = length(Starts)
                            },
                            case maps:find(ChunkId, Inner) of
                                error ->
                                    Inner#{ChunkId => Match};
                                {ok, Existing} ->
                                    Inner#{
                                        ChunkId => fts2_search_merge_meta(Existing, Match)
                                    }
                            end
                    end
                end,
                Acc,
                Candidates
            )
        end,
        #{},
        fts2_search_selector_ids(Columns, Schema)
    ).

fts2_search_item_spans(Meta, Column, {term, Token, Prefix, _Columns}) ->
    [{P, P} || P <- fts2_search_column_token_positions(Meta, Column, Token, Prefix)];
fts2_search_item_spans(Meta, Column, {phrase, Specs, _Columns}) ->
    Starts = fts2_search_phrase_starts(Meta, Column, Specs),
    LastOffset = lists:max([Offset || {_Token, _Prefix, Offset} <- Specs]),
    [{Start, Start + LastOffset} || Start <- Starts];
fts2_search_item_spans(Meta, Column, {anchor, Item}) ->
    [
        {Start, End}
     || {Start, End} <- fts2_search_item_spans(Meta, Column, Item), Start =:= 0
    ];
fts2_search_item_spans(Meta, _Column, _Other) ->
    [
        {P, P}
     || P <- fts2_search_flatten_positions(
            Meta#fts2_match.match_positions
        )
    ].

fts2_search_near_positions(SpanLists, _Distance) when SpanLists =:= [] -> [];
fts2_search_near_positions(SpanLists, _Distance) when
    length(SpanLists) > 0,
    hd(SpanLists) =:= []
->
    [];
fts2_search_near_positions([First, Second], Distance) ->
    fts2_search_near_pair_positions(First, Second, Distance, []);
fts2_search_near_positions([First | Rest], Distance) ->
    [
        Start
     || {Start, _End} = Span <- First,
        fts2_search_near_position_matches([Span], Rest, Distance)
    ].

fts2_search_near_term_positions([], _Second, _Distance, Acc) ->
    lists:reverse(Acc);
fts2_search_near_term_positions(
    [First | Rest], Second0, Distance, Acc
) ->
    Second = fts2_search_near_term_drop_before(
        Second0, First - Distance - 1
    ),
    NextAcc = case Second of
        [SecondPosition | _] when SecondPosition =< First + Distance + 1 ->
            [First | Acc];
        _ ->
            Acc
    end,
    fts2_search_near_term_positions(Rest, Second, Distance, NextAcc).

fts2_search_near_term_drop_before(
    [Position | Rest], Minimum
) when Position < Minimum ->
    fts2_search_near_term_drop_before(Rest, Minimum);
fts2_search_near_term_drop_before(Positions, _Minimum) ->
    Positions.

%% Both position planes are ordered. For the common two-item NEAR grammar,
%% advance the second cursor monotonically instead of probing every pair of
%% spans. A second span matches when its interval is no more than Distance
%% token gaps before or after the first interval.
fts2_search_near_pair_positions([], _Second, _Distance, Acc) ->
    lists:reverse(Acc);
fts2_search_near_pair_positions(
    [{Start, End} | Rest], Second0, Distance, Acc
) ->
    Second = fts2_search_near_drop_before(
        Second0, Start - Distance - 1
    ),
    NextAcc = case Second of
        [{SecondStart, _SecondEnd} | _] when
            SecondStart =< End + Distance + 1
        ->
            [Start | Acc];
        _ ->
            Acc
    end,
    fts2_search_near_pair_positions(Rest, Second, Distance, NextAcc).

fts2_search_near_drop_before(
    [{_Start, End} | Rest], MinimumEnd
) when End < MinimumEnd ->
    fts2_search_near_drop_before(Rest, MinimumEnd);
fts2_search_near_drop_before(Spans, _MinimumEnd) ->
    Spans.

fts2_search_near_position_matches(_Chosen, [], _Distance) ->
    true;
fts2_search_near_position_matches(Chosen, [Spans | Rest], Distance) ->
    lists:any(
        fun(Span) ->
            lists:all(
                fun(Other) -> fts2_search_span_distance(Span, Other) =< Distance end,
                Chosen
            ) andalso fts2_search_near_position_matches([Span | Chosen], Rest, Distance)
        end,
        Spans
    ).

fts2_search_span_distance({_SA, EA}, {SB, _EB}) when EA < SB -> SB - EA - 1;
fts2_search_span_distance({SA, _EA}, {_SB, EB}) when EB < SA -> SA - EB - 1;
fts2_search_span_distance(_A, _B) -> 0.

fts2_search_intersect_many([]) -> #{};
fts2_search_intersect_many([First | Rest]) -> lists:foldl(fun fts2_search_intersect/2, First, Rest).

fts2_search_intersect(A, B) ->
    maps:fold(
        fun(ChunkId, MetaA, Acc) ->
            case maps:find(ChunkId, B) of
                {ok, MetaB} -> Acc#{ChunkId => fts2_search_merge_meta(MetaA, MetaB)};
                error -> Acc
            end
        end,
        #{},
        A
    ).

fts2_search_union(A, B) ->
    maps:fold(
        fun(ChunkId, Meta, Acc) ->
            case maps:find(ChunkId, Acc) of
                error -> Acc#{ChunkId => Meta};
                {ok, Existing} -> Acc#{ChunkId => fts2_search_merge_meta(Existing, Meta)}
            end
        end,
        A,
        B
    ).

fts2_search_merge_meta(A, B) ->
    A#fts2_match{
        columns = lists:ukeysort(
            1, B#fts2_match.columns ++ A#fts2_match.columns
        ),
        terms = lists:usort(B#fts2_match.terms ++ A#fts2_match.terms),
        term_stats = lists:ukeysort(
            1, B#fts2_match.term_stats ++ A#fts2_match.term_stats
        ),
        tf = A#fts2_match.tf + B#fts2_match.tf,
        group_match_count = case {
            A#fts2_match.group_match_count,
            B#fts2_match.group_match_count
        } of
            {undefined, undefined} -> undefined;
            _ -> fts2_search_group_match_count(A) +
                fts2_search_group_match_count(B)
        end,
        match_positions = lists:ukeysort(
            1,
            B#fts2_match.match_positions ++ A#fts2_match.match_positions
        )
    }.

fts2_search_collapse_scored_groups(Chunks, Ranked) ->
    Groups = lists:foldl(
        fun(Meta, Acc) ->
            GroupKey = case Meta#fts2_match.logical_group of
                undefined -> {generation, Meta#fts2_match.group_id};
                LogicalGroup -> LogicalGroup
            end,
            Version = Meta#fts2_match.group_version,
            case maps:find(GroupKey, Acc) of
                error ->
                    Acc#{GroupKey => Meta};
                {ok, Existing} ->
                    ExistingVersion = Existing#fts2_match.group_version,
                    if
                        Version > ExistingVersion ->
                            Acc#{GroupKey => Meta};
                        Version < ExistingVersion ->
                            Acc;
                        true ->
                            CombinedCount =
                                fts2_search_group_match_count(Existing) +
                                    fts2_search_group_match_count(Meta),
                            Winner = case fts2_search_better_chunk(Meta, Existing, Ranked) of
                                true -> Meta;
                                false -> Existing
                            end,
                            Acc#{GroupKey => Winner#fts2_match{
                                group_match_count = CombinedCount
                            }}
                    end
            end
        end,
        #{},
        Chunks
    ),
    maps:values(Groups).

fts2_search_better_chunk(A, B, true) ->
    {-A#fts2_match.score, A#fts2_match.source_id} <
        {-B#fts2_match.score, B#fts2_match.source_id};
fts2_search_better_chunk(A, B, false) ->
    A#fts2_match.source_id < B#fts2_match.source_id.

fts2_search_match_count(#fts2_match{match_count = undefined, tf = Tf}) -> Tf;
fts2_search_match_count(#fts2_match{match_count = Count}) -> Count.

fts2_search_group_match_count(
    #fts2_match{group_match_count = undefined} = Match
) ->
    fts2_search_match_count(Match);
fts2_search_group_match_count(#fts2_match{group_match_count = Count}) ->
    Count.

fts2_search_score_matches(Matches, _Root, false) ->
    [Meta#fts2_match{score = 0.0} || Meta <- maps:values(Matches)];
fts2_search_score_matches(Matches, Root, true) ->
    CountKey = case maps:get(scoring_grouping, Root, grouped) of
        grouped -> group_count;
        ungrouped -> chunk_count
    end,
    DocCount = erlang:max(maps:get(CountKey, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    MatchValues = maps:values(Matches),
    Terms = lists:usort(
        [Term || Meta <- MatchValues,
            {Term, _Tf, _Source, _Df} <- Meta#fts2_match.term_stats]
    ),
    Parts = lists:ukeysort(
        1,
        [
            {{Source, Term}, Df}
         || Meta <- MatchValues,
            {Term, _Tf, Source, Df} <- Meta#fts2_match.term_stats
        ]
    ),
    Idfs = maps:from_list([
        begin
            NHit0 = lists:sum([
                Df
             || {{_Source, PartTerm}, Df} <- Parts,
                PartTerm =:= Term
            ]),
            NHit = erlang:min(DocCount, erlang:max(1, NHit0)),
            Idf0 = math:log((DocCount - NHit + 0.5) / (NHit + 0.5)),
            {Term, erlang:max(Idf0, 1.0e-6)}
        end
     || Term <- Terms
    ]),
    [
        Meta#fts2_match{
            score = lists:sum([
                fts2_search_bm25(
                    Tf,
                    maps:get(Term, Idfs),
                    Avg,
                    Meta#fts2_match.doc_length
                )
             || {Term, Tf, _Source, _Df} <- Meta#fts2_match.term_stats,
                Tf > 0
            ])
        }
     || Meta <- MatchValues
    ].

fts2_search_bm25(Tf, Idf, Avg, Length) ->
    Ratio =
        case Avg > 0.0 of
            true -> Length / Avg;
            false -> 1.0
        end,
    Idf * (Tf * 2.2) / (Tf + 1.2 * (0.25 + 0.75 * Ratio)).

fts2_search_read_identities(_Bookie, _Schema, _Root, []) ->
    #{};
fts2_search_read_identities(Bookie, #{index := Bucket} = Schema, Root, GroupIds) ->
    Generation = maps:get(generation, Root),
    Shift = fts2_search_identity_page_shift(Root, Schema),
    IdentityBookie = maps:get(identity_bookie, Schema, Bookie),
    Key = fts2_codec_identity_key(Generation),
    Pages = lists:usort([
        GroupId bsr Shift
     || GroupId <- GroupIds
    ]),
    Values = leveled_fts_residency:headonly_many(
        IdentityBookie,
        Bucket,
        [
            {Key, fts2_codec_identity_subkey(Page)}
         || Page <- Pages
        ]
    ),
    lists:foldl(
        fun
            ({Page, {ok, Value}}, Acc) ->
                Wanted = [
                    GroupId
                 || GroupId <- GroupIds,
                    GroupId bsr Shift =:= Page
                ],
                maps:merge(
                    Acc,
                    fts2_search_groups_to_map(
                        fts2_codec_decode_identity_page(Value, Wanted)
                    )
                );
            ({_Page, not_found}, Acc) ->
                Acc
        end,
        #{},
        lists:zip(Pages, Values)
    ).

fts2_search_identity_page_handles(_Bookie, _Schema, _Root, []) ->
    #{};
fts2_search_identity_page_handles(
    Bookie, #{index := Bucket} = Schema, Root, Requests
) ->
    Generation = maps:get(generation, Root),
    Shift = fts2_search_identity_page_shift(Root, Schema),
    IdentityBookie = maps:get(identity_bookie, Schema, Bookie),
    Key = fts2_codec_identity_key(Generation),
    Pages = lists:usort([
        GroupId bsr Shift
     || {GroupId, _ChunkId} <- Requests
    ]),
    Values = leveled_fts_residency:headonly_many(
        IdentityBookie,
        Bucket,
        [
            {Key, fts2_codec_identity_subkey(Page)}
         || Page <- Pages
        ]
    ),
    maps:from_list([
        {Page, fts2_codec_identity_page_handle(Value)}
     || {Page, {ok, Value}} <- lists:zip(Pages, Values)
    ]).

fts2_search_identity_handle(Handles, GroupId, Shift) ->
    maps:find(GroupId bsr Shift, Handles).

fts2_search_read_identity_rows(_Bookie, _Schema, _Root, []) ->
    [];
fts2_search_read_identity_rows(Bookie, Schema, Root, Requests) ->
    Shift = fts2_search_identity_page_shift(Root, Schema),
    Handles = fts2_search_identity_page_handles(
        Bookie, Schema, Root, Requests
    ),
    [
        case Request of
            undefined ->
                undefined;
            {GroupId, ChunkId} ->
                case fts2_search_identity_handle(Handles, GroupId, Shift) of
                    {ok, Handle} ->
                        fts2_codec_identity_page_row(Handle, GroupId, ChunkId);
                    error ->
                        undefined
                end
        end
     || Request <- Requests
    ].

fts2_search_identity_page_shift(Root) ->
    maps:get(
        identity_page_shift, Root, ?LEGACY_IDENTITY_PAGE_SHIFT
    ).

fts2_search_identity_page_shift(Root, Schema) ->
    maps:get(
        identity_page_shift,
        Schema,
        fts2_search_identity_page_shift(Root)
    ).

fts2_search_groups_to_map(Rows) ->
    lists:foldl(
        fun(
            {GroupId, GroupKey, ChunkId, SourceId, DocKey, DocVersion,
                DocLength, Candidate, Hit},
            Acc
        ) ->
            Chunk = #{
                chunk_id => ChunkId,
                group_id => GroupId,
                source_id => SourceId,
                doc_key => DocKey,
                doc_version => DocVersion,
                doc_length => DocLength,
                candidate_record => Candidate,
                hit_record => Hit
            },
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => #{
                        group_id => GroupId,
                        group_key => GroupKey,
                        chunks => [Chunk]
                    }};
                {ok, Group} ->
                    Acc#{GroupId => Group#{
                        chunks => maps:get(chunks, Group) ++ [Chunk]
                    }}
            end
        end,
        #{},
        Rows
    ).

fts2_search_hydrate_hit(
    #fts2_match{delta_document = Document} = Match, undefined, Opts
) when Document =/= undefined ->
    fts2_search_hydrate_hit(
        Match,
        {undefined, undefined, Match#fts2_match.chunk_id,
            maps:get(source_id, Document), maps:get(doc_key, Document),
            maps:get(doc_version, Document), maps:get(doc_length, Document),
            maps:get(candidate_record, Document),
            maps:get(hit_record, Document)},
        Opts
    );
fts2_search_hydrate_hit(_Match, undefined, _Opts) ->
    false;
fts2_search_hydrate_hit(
    Match,
    {_GroupId, _GroupKey, _ChunkId, SourceId, DocKey, DocVersion, _StoredLength,
        Candidate, HitRecord},
    Opts
) ->
            Base0 = #{
                key => SourceId,
                score => Match#fts2_match.score,
                doc_length => Match#fts2_match.doc_length,
                match_count => case Match#fts2_match.group_match_count of
                    undefined -> fts2_search_match_count(Match);
                    GroupCount -> GroupCount
                end,
                candidate_key => DocKey,
                candidate_version => DocVersion,
                candidate_record => Candidate
            },
            Base1 =
                case maps:get(return_positions, Opts, false) of
                    true ->
                        Base0#{
                            positions => fts2_search_window_positions(
                                Match#fts2_match.match_positions
                            )
                        };
                    false ->
                        Base0
                end,
            Base2 =
                case maps:get(return_terms, Opts, false) of
                    true -> Base1#{matched_terms => Match#fts2_match.terms};
                    false -> Base1
                end,
            Base3 =
                case
                    {
                        maps:find('$fts_text_blocks', Candidate),
                        maps:find('$fts_text_bytes', Candidate)
                    }
                of
                    {{ok, Blocks}, {ok, Bytes}} ->
                        Base2#{
                            text_blocks => Blocks,
                            text_bytes => Bytes,
                            index_resident_complete => true
                        };
                    _ ->
                        Base2
                end,
            case maps:get(resolve_hits, Opts, true) of
                false ->
                    {true, Base3};
                true ->
                    Resolved0 = Base3#{
                        key => DocKey, doc_id => SourceId
                    },
                    Resolved =
                        case HitRecord of
                            #{
                                record := Record,
                                text_blocks := HitBlocks,
                                text_bytes := HitBytes
                            } ->
                                WithText = Resolved0#{
                                    text_blocks => HitBlocks,
                                    text_bytes => HitBytes
                                },
                                case map_size(Record) of
                                    0 -> WithText;
                                    _ -> WithText#{record => Record}
                                end;
                            _ ->
                                Resolved0
                    end,
                    {true, Resolved}
            end.

fts2_search_order_hits(Hits, false, _TieFields) ->
    lists:sort(fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end, Hits);
fts2_search_order_hits(Hits, true, TieFields) ->
    lists:sort(
        fun(A, B) ->
            {-maps:get(score, A), fts2_search_tie_key(A, TieFields)} =<
                {-maps:get(score, B), fts2_search_tie_key(B, TieFields)}
        end,
        Hits
    ).

fts2_search_tie_key(Hit, []) ->
    maps:get(candidate_key, Hit, maps:get(key, Hit));
fts2_search_tie_key(Hit, Fields) ->
    Candidate = maps:get(candidate_record, Hit, #{}),
    {
        [maps:get(Field, Candidate, nil) || Field <- Fields],
        maps:get(candidate_key, Hit)
    }.

fts2_search_facet_matches(_Hit, _Schema, nil) ->
    true;
fts2_search_facet_matches(Hit, Schema, Facet) when is_list(Facet) ->
    Candidate = maps:get(candidate_record, Hit, #{}),
    Fields = maps:get(candidate_filter_fields, Schema, []),
    Facet =:=
        [maps:get(Field, Candidate, undefined) || {_Column, Field} <- Fields];
fts2_search_facet_matches(_Hit, _Schema, _Facet) ->
    false.

fts2_search_all_chunks(Bookie, Schema, Root, Wanted) ->
    GroupCount = maps:get(group_count, Root),
    Identity = fts2_search_read_identities(
        Bookie, Schema, Root, lists:seq(0, GroupCount - 1)
    ),
    maps:fold(
        fun(_GroupId, Group, Acc) ->
            lists:foldl(
                fun(Chunk, Inner) ->
                    SourceId = maps:get(source_id, Chunk),
                    case fts2_search_wanted(SourceId, Wanted) of
                        false ->
                            Inner;
                        true ->
                            ChunkId = maps:get(chunk_id, Chunk),
                            Inner#{ChunkId => #fts2_match{
                                chunk_id = ChunkId,
                                group_id = maps:get(group_id, Chunk),
                                source_id = SourceId,
                                doc_length = maps:get(doc_length, Chunk)
                            }}
                    end
                end,
                Acc,
                maps:get(chunks, Group)
            )
        end,
        #{},
        Identity
    ).

fts2_search_column_token_positions(Meta, Column, Token) ->
    proplists:get_value(
        {Column, Token}, Meta#fts2_match.columns, []
    ).

fts2_search_column_token_positions(Meta, Column, Token, false) ->
    fts2_search_column_token_positions(Meta, Column, Token);
fts2_search_column_token_positions(Meta, Column, Prefix, true) ->
    lists:usort(lists:append([
        Positions
     || {{EntryColumn, Token}, Positions} <- Meta#fts2_match.columns,
        EntryColumn =:= Column,
        binary:match(Token, Prefix) =:= {0, byte_size(Prefix)}
    ])).

fts2_search_selector_ids(all, Schema) ->
    lists:seq(0, length(maps:get(columns, Schema)) - 1);
fts2_search_selector_ids({not_columns, Excluded}, Schema) ->
    fts2_search_selector_ids(
        [C || C <- maps:get(columns, Schema), not lists:member(C, Excluded)],
        Schema
    );
fts2_search_selector_ids(Columns, Schema) ->
    Names = maps:get(columns, Schema),
    [
        Index
     || {Name, Index} <- lists:zip(Names, lists:seq(0, length(Names) - 1)),
        lists:member(Name, Columns)
    ].

fts2_search_column_name(Column, Schema) -> lists:nth(Column + 1, maps:get(columns, Schema)).

fts2_search_restrict_columns({term, T, P, _}, Columns) ->
    {term, T, P, Columns};
fts2_search_restrict_columns({phrase, Specs, _}, Columns) ->
    {phrase, Specs, Columns};
fts2_search_restrict_columns({near, Items, D, _}, Columns) ->
    {near, [fts2_search_restrict_columns(I, Columns) || I <- Items], D, Columns};
fts2_search_restrict_columns({anchor, A}, Columns) ->
    {anchor, fts2_search_restrict_columns(A, Columns)};
fts2_search_restrict_columns({'and', A, B}, Columns) ->
    {'and', fts2_search_restrict_columns(A, Columns), fts2_search_restrict_columns(B, Columns)};
fts2_search_restrict_columns({'or', A, B}, Columns) ->
    {'or', fts2_search_restrict_columns(A, Columns), fts2_search_restrict_columns(B, Columns)};
fts2_search_restrict_columns({'not', A, B}, Columns) ->
    {'not', fts2_search_restrict_columns(A, Columns), fts2_search_restrict_columns(B, Columns)};
fts2_search_restrict_columns(Other, _Columns) ->
    Other.

fts2_search_wanted(_SourceId, all) -> true;
fts2_search_wanted(SourceId, {all_except, Excluded}) -> not maps:is_key(SourceId, Excluded);
fts2_search_wanted(SourceId, Wanted) -> maps:is_key(SourceId, Wanted).

fts2_search_wanted_delta(_SourceId, all) -> true;
fts2_search_wanted_delta(_SourceId, {all_except, _Excluded}) -> true;
fts2_search_wanted_delta(SourceId, Wanted) -> maps:is_key(SourceId, Wanted).

fts2_search_window_positions(Value) ->
    {Windowed, _Remaining} = lists:foldl(
        fun({Key, Nested}, {Acc, Left}) ->
            {Kept, Next} = fts2_search_window_positions(Nested, Left),
            {Acc#{Key => Kept}, Next}
        end,
        {#{}, ?MAX_RETURN_POSITIONS},
        lists:sort(Value)
    ),
    Windowed.

fts2_search_window_positions(Value, Remaining) when is_map(Value) ->
    lists:foldl(
        fun({Key, Nested}, {Acc, Left}) ->
            {Windowed, Next} = fts2_search_window_positions(Nested, Left),
            {Acc#{Key => Windowed}, Next}
        end,
        {#{}, Remaining},
        lists:sort(maps:to_list(Value))
    );
fts2_search_window_positions(Value, Remaining) when is_list(Value) ->
    Kept = lists:sublist(lists:sort(Value), Remaining),
    {Kept, Remaining - length(Kept)};
fts2_search_window_positions(Value, Remaining) ->
    {Value, Remaining}.

fts2_search_flatten_positions(Value) when is_map(Value) ->
    lists:append([fts2_search_flatten_positions(V) || V <- maps:values(Value)]);
fts2_search_flatten_positions([{_Key, _Nested} | _] = Value) ->
    lists:append([
        fts2_search_flatten_positions(Nested)
     || {_, Nested} <- Value
    ]);
fts2_search_flatten_positions(Value) when is_list(Value) -> Value;
fts2_search_flatten_positions(_Value) ->
    [].

fts2_search_drop(0, Values) -> Values;
fts2_search_drop(_Count, []) -> [];
fts2_search_drop(Count, [_ | Rest]) -> fts2_search_drop(Count - 1, Rest).

fts2_search_note_plane_decode() ->
    case erlang:get({leveled_fts, direct_page_decodes}) of
        Count when is_integer(Count) ->
            erlang:put({leveled_fts, direct_page_decodes}, Count + 1);
        _ ->
            ok
    end.

fts2_search_note_term_read(Token, true) ->
    case erlang:get({leveled_fts, term_run_folds}) of
        Counts when is_map(Counts) ->
            erlang:put(
                {leveled_fts, term_run_folds},
                Counts#{Token => maps:get(Token, Counts, 0) + 1}
            );
        _ ->
            ok
    end;
fts2_search_note_term_read(_Token, false) ->
    ok.

fts2_search_check_cancellation() ->
    case erlang:get(ash_leveled_search_cancellation) of
        #{owner := Owner, deadline_at := DeadlineAt, cancel_token := Token} ->
            receive
                {ash_leveled_cancel, Token, Reason} ->
                    throw({fts_error, {search_cancelled, Reason}})
            after 0 ->
                case erlang:is_process_alive(Owner) of
                    false ->
                        throw({fts_error, {search_cancelled, caller_down}});
                    true ->
                        case
                            DeadlineAt =/= infinity andalso
                                erlang:monotonic_time(millisecond) >= DeadlineAt
                        of
                            true ->
                                throw(
                                    {fts_error, {search_cancelled, deadline}}
                                );
                            false ->
                                ok
                        end
                end
            end;
        _ ->
            ok
    end.
