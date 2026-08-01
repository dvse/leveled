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
    record_fetch/4,
    candidate_fetch/4,
    consolidate/3
]).

-define(DEFAULT_LIMIT, 10000).
-define(MAX_LIMIT, 20000).
-define(MAX_WINDOW, 20000).
-define(MAX_QUERY_BYTES, 4096).
-define(MAX_QUERY_TOKENS, 128).
-define(MAX_AST_DEPTH, 32).
-define(MAX_NEAR_DISTANCE, 64).
-define(MAX_PREFIX_BYTES, 64).
-define(DEFAULT_NEAR, 10).

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
%% the exhaustive document-major delta set. The library owns no ETS or other
%% serving cache; leveled's ledger/page caches are the sole source of warmth.
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
    TextTokens = tokenize_with_offsets(Text, maps:get(options, Schema)),
    Blocks = client_text_blocks(Text),
    BlockDirectory = client_text_block_directory(Text, TextTokens),
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
    BlockSpecs = client_text_block_specs(Bucket, DocId, Blocks, TextTokens),
    WriteShards = client_write_shards(Touched),
    {ok,
        BlockSpecs ++
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
            client_record_tail_dirty_spec(Bucket),
            client_stats_dirty_spec(Bucket)
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
        leveled_fts2_codec:encode(delta, Delta)}.

client_record_tail_dirty_spec(Bucket) ->
    {add, Bucket, <<"record-tail">>, <<"dirty">>, <<1>>}.

client_text_block_key(DocId) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    <<"b:", DocId:64/unsigned-big>>.

client_text_block_specs(Bucket, DocId, Blocks, Tokens) ->
    Key = client_text_block_key(DocId),
    case Blocks of
        [] ->
            [];
        _ ->
            [
                {add, Bucket, Key, <<0:32/unsigned-big>>,
                    client_encode_text_pack(Blocks, Tokens)}
            ]
    end.

client_text_block_token_offsets(BlockNo, Tokens) ->
    Start = BlockNo * ?TEXT_BLOCK_BYTES,
    Finish = Start + ?TEXT_BLOCK_BYTES,
    [
        {Ordinal, Offset - Start, Length}
     || {_Token, Ordinal, Offset, Length} <- Tokens,
        Offset >= Start,
        Offset < Finish
    ].

client_remove_text_block_specs(Bucket, DocId, BlockCount) ->
    Key = client_text_block_key(DocId),
    [
        {remove, Bucket, Key, <<BlockNo:32/unsigned-big>>, <<>>}
     || BlockNo <- lists:seq(0, erlang:max(BlockCount - 1, 0))
    ].

client_encode_text_pack(Blocks, Tokens) ->
    {Directory0, Payload0, _Offset} = lists:foldl(
        fun({BlockNo, Block}, {Directory, Payload, Offset}) ->
            {OffsetDirectory, EncodedOffsets} =
                client_encode_seekable_text_block_offsets(
                    client_text_block_token_offsets(BlockNo, Tokens)
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
    case text_blocks_with_offsets_batch(Bookie, Schema, Requests) of
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

-spec record_fetch(pid(), map(), [non_neg_integer()], fold | points) ->
    {ok, map()} | {error, term()}.
record_fetch(Bookie, #{index := _} = Schema, DocIds0, Strategy) when
    is_pid(Bookie),
    is_list(DocIds0),
    (Strategy =:= fold orelse Strategy =:= points)
->
    try
        DocIds = lists:usort(DocIds0),
        Documents = leveled_fts2:lookup_documents(Bookie, Schema, DocIds),
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
        Documents = leveled_fts2:lookup_documents(Bookie, Schema, DocIds),
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

client_posting_read(_Bookie, _Schema, _AST, [], _Opts) ->
    {ok, []};
client_posting_read(Bookie, Schema, AST, DocKeys, Opts) ->
    CandidateIds = maps:keys(
        client_posting_candidates(Bookie, Schema, DocKeys)
    ),
    case client_fts2_state(Bookie, Schema) of
        {clean, Root} when is_map(Root) ->
            leveled_fts2:posting_read(
                Bookie, Schema, Root, AST, CandidateIds, Opts
            );
        {_State, Root} ->
            leveled_fts2:posting_read_dirty(
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
client_fts2_state(Bookie, #{index := Bucket} = Schema) ->
    Root = case leveled_fts2:available(Bookie, Schema) of
        {ok, ExistingRoot} -> ExistingRoot;
        not_found -> undefined
    end,
    State = case
        leveled_bookie:book_headonly_many(
            Bookie,
            Bucket,
            [
                {<<"stats">>, <<"dirty">>},
                {<<"record-tail">>, <<"dirty">>}
            ]
        )
    of
        [not_found, not_found] -> clean;
        _DirtyOrUnavailable ->
            dirty
    end,
    {State, Root}.

client_search(Bookie, Schema, AST, Opts, Hook) ->
    case client_fts2_state(Bookie, Schema) of
        {clean, Root} when is_map(Root) ->
            leveled_fts2:search(Bookie, Schema, Root, AST, Opts);
        {_State, Root} ->
            leveled_fts2:search_dirty(Bookie, Schema, Root, AST, Opts, Hook)
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
        case client_option(shards, Opts, all) of
            Shards when is_list(Shards) ->
                {ok, #{consolidated => Shards, skipped => []}};
            all ->
                case leveled_fts2:consolidate(Bookie, Schema, Hook) of
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

client_text_block_directory(<<>>, _Opts) ->
    [];
client_text_block_directory(Text, Tokens) ->
    BlockCount =
        (byte_size(Text) + ?TEXT_BLOCK_BYTES - 1) div ?TEXT_BLOCK_BYTES,
    client_text_block_directory(0, BlockCount, Tokens, []).

client_text_block_directory(BlockNo, BlockCount, _Tokens, Acc) when
    BlockNo >= BlockCount
->
    lists:reverse(Acc);
client_text_block_directory(BlockNo, BlockCount, Tokens, Acc) ->
    Start = BlockNo * ?TEXT_BLOCK_BYTES,
    FirstOrdinal = client_first_block_ordinal(Tokens, Start),
    client_text_block_directory(
        BlockNo + 1,
        BlockCount,
        Tokens,
        [{BlockNo, FirstOrdinal, Start} | Acc]
    ).

client_first_block_ordinal(Tokens, Start) ->
    client_first_block_ordinal(Tokens, Start, length(Tokens)).

client_first_block_ordinal([], _Start, EndOrdinal) ->
    EndOrdinal;
client_first_block_ordinal(
    [{_Token, Ordinal, Offset, Length} | Rest], Start, EndOrdinal
) ->
    case Offset + Length > Start of
        true -> Ordinal;
        false -> client_first_block_ordinal(Rest, Start, EndOrdinal)
    end.

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
    group_positions([
        TP
     || {Token, _Pos} = TP <- tokenize(Text, Opts),
        byte_size(Token) =< 65535
    ]).

group_positions(Tokens) ->
    group_positions(Tokens, #{}).

group_positions([], Acc) ->
    %% Positions are appended in increasing order during tokenisation and
    %% prepended here, so each list is descending; a single reverse yields the
    %% ascending order encode_positions/1 expects (no per-token sort needed).
    [
        {Token, lists:reverse(Positions)}
     || {Token, Positions} <- maps:to_list(Acc)
    ];
group_positions([{Token, Pos} | Rest], Acc) ->
    group_positions(
        Rest, maps:update_with(Token, fun(Ps) -> [Pos | Ps] end, [Pos], Acc)
    ).

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
    Stopwords = maps:get(stopwords, Opts, []),
    Tokens =
        case
            maps:get(tokenchars, Opts, []) =:= [] andalso
                maps:get(separators, Opts, []) =:= []
        of
            true ->
                fast_tokens_with_offsets(
                    Text, 0, Opts, Stopwords, <<>>, false, undefined, 0, []
                );
            false ->
                unicode_tokens_with_offsets(
                    Text, 0, Opts, Stopwords, <<>>, undefined, 0, []
                )
        end,
    [compat_source_range(Text, Token) || Token <- Tokens].

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

%% Offset-aware common path.  Token bytes follow sqlite_utf8_compat/1 while
%% Start/Offset remain coordinates in the original input binary.
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C >= $a, C =< $z ->
    fast_tokens_with_offsets(
        Rest,
        Offset + 1,
        Opts,
        SW,
        <<Tok/binary, C>>,
        NA,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C >= $0, C =< $9 ->
    fast_tokens_with_offsets(
        Rest,
        Offset + 1,
        Opts,
        SW,
        <<Tok/binary, C>>,
        NA,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C >= $A, C =< $Z ->
    fast_tokens_with_offsets(
        Rest,
        Offset + 1,
        Opts,
        SW,
        <<Tok/binary, (C bor 16#20)>>,
        NA,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C < 128 ->
    {Pos1, Acc1} = fast_offset_flush(
        Tok, NA, Start, Offset, Opts, SW, Pos, Acc
    ),
    fast_tokens_with_offsets(
        Rest, Offset + 1, Opts, SW, <<>>, false, undefined, Pos1, Acc1
    );
fast_tokens_with_offsets(
    <<16#F0, 16#9F, 16#92, Next, Rest/binary>>,
    Offset,
    Opts,
    SW,
    Tok,
    _NA,
    Start,
    Pos,
    Acc
) when Next band 16#C0 =/= 16#80 ->
    fast_tokens_with_offsets(
        <<Next, Rest/binary>>,
        Offset + 3,
        Opts,
        SW,
        <<Tok/binary, 16#DF, 16#92>>,
        true,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<CP/utf8, Rest/binary>> = Bin,
    Offset,
    Opts,
    SW,
    Tok,
    NA,
    Start,
    Pos,
    Acc
) ->
    CharLen = byte_size(Bin) - byte_size(Rest),
    case unicode_token_char(CP, Opts) of
        true ->
            <<Char:CharLen/binary, _/binary>> = Bin,
            fast_tokens_with_offsets(
                Rest,
                Offset + CharLen,
                Opts,
                SW,
                <<Tok/binary, Char/binary>>,
                true,
                token_start(Start, Offset),
                Pos,
                Acc
            );
        false ->
            {Pos1, Acc1} = fast_offset_flush(
                Tok, NA, Start, Offset, Opts, SW, Pos, Acc
            ),
            fast_tokens_with_offsets(
                Rest,
                Offset + CharLen,
                Opts,
                SW,
                <<>>,
                false,
                undefined,
                Pos1,
                Acc1
            )
    end;
fast_tokens_with_offsets(
    <<_Bad, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) ->
    {Pos1, Acc1} = fast_offset_flush(
        Tok, NA, Start, Offset, Opts, SW, Pos, Acc
    ),
    fast_tokens_with_offsets(
        Rest, Offset + 1, Opts, SW, <<>>, false, undefined, Pos1, Acc1
    );
fast_tokens_with_offsets(
    <<>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) ->
    {_Pos1, Acc1} = fast_offset_flush(
        Tok, NA, Start, Offset, Opts, SW, Pos, Acc
    ),
    lists:reverse(Acc1).

fast_offset_flush(<<>>, _NA, _Start, _End, _Opts, _SW, Pos, Acc) ->
    {Pos, Acc};
fast_offset_flush(Tok, false, Start, End, _Opts, SW, Pos, Acc) ->
    case lists:member(Tok, SW) of
        true -> {Pos + 1, Acc};
        false -> {Pos + 1, [{Tok, Pos, Start, End - Start} | Acc]}
    end;
fast_offset_flush(Tok, true, Start, End, Opts, SW, Pos, Acc) ->
    Norm = normalise_token(Tok, Opts),
    case {Norm =:= <<>>, lists:member(Norm, SW)} of
        {true, _} -> {Pos, Acc};
        {false, true} -> {Pos + 1, Acc};
        {false, false} -> {Pos + 1, [{Norm, Pos, Start, End - Start} | Acc]}
    end.

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
