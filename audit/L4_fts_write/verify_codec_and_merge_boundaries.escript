#!/usr/bin/env escript
%%! -pa /Users/dvse/projects/agents/leveled/_build/default/lib/leveled/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/lz4/ebin /Users/dvse/projects/agents/leveled/_build/default/lib/zstd/ebin

-mode(compile).

-define(FTS_SOURCE, "/Users/dvse/projects/agents/leveled/src/leveled_fts.erl").
-define(INCLUDE_DIR, "/Users/dvse/projects/agents/leveled/include").

main(_) ->
    ok = load_export_all(leveled_fts, ?FTS_SOURCE),
    position_boundaries(),
    frame_and_token_boundaries(),
    verbatim_boundaries(),
    delta_carrier_tag_discipline(),
    merge_shapes(),
    tokenizer_path_parity(),
    io:format("PASS codec, frame, verbatim, merge, and tokenizer-path boundaries~n", []).

position_boundaries() ->
    ExactPositions = lists:seq(0, 65524),
    ExactBin = leveled_fts:encode_positions(ExactPositions),
    65525 = byte_size(ExactBin),
    {ok, ExactPositions} = leveled_fts:decode_positions(ExactBin, 0, []),

    %% The next dense position is dropped once the threshold is reached.
    ExactBin = leveled_fts:encode_positions(ExactPositions ++ [65525]),

    %% A final ten-byte varint may cross the threshold, but remains below the
    %% 16-bit frame maximum and must decode without disturbing the next frame.
    Prefix = lists:seq(0, 65523),
    HugePosition = (1 bsl 63) + 65523,
    OvershootPositions = Prefix ++ [HugePosition],
    OvershootBin = leveled_fts:encode_positions(OvershootPositions),
    65534 = byte_size(OvershootBin),
    {ok, OvershootPositions} = leveled_fts:decode_positions(OvershootBin, 0, []),

    Frame1 = leveled_fts:encode_frame(9, <<"a">>, OvershootBin),
    Frame2 = leveled_fts:encode_frame(10, <<"b">>, <<>>),
    [
        {<<"a">>, 9, OvershootPositions},
        {<<"b">>, 10, []}
    ] = leveled_fts:extract_frames(<<Frame1/binary, Frame2/binary>>, all, true).

frame_and_token_boundaries() ->
    MaxKey = binary:copy(<<"k">>, 65535),
    MaxKeyFrame = leveled_fts:encode_frame(1, MaxKey, <<>>),
    [{MaxKey, 1, present}] = leveled_fts:extract_frames(MaxKeyFrame, all, false),

    TooLongKey = binary:copy(<<"k">>, 65536),
    Overflow =
        try leveled_fts:encode_frame(1, TooLongKey, <<>>) of
            _UnexpectedFrame -> no_overflow
        catch
            throw:Reason -> Reason
        end,
    {fts_error, {frame_field_overflow, 65536, 0}} = Overflow,

    MaxToken = binary:copy(<<"t">>, 65535),
    Entry = leveled_fts:encode_entry(MaxToken, 0, <<>>),
    [{MaxToken, 0, <<>>}] = leveled_fts:fold_entries(
        Entry,
        fun(Token, NDocs, Frames, Acc) -> [{Token, NDocs, Frames} | Acc] end,
        []
    ).

verbatim_boundaries() ->
    {ok, [Schema]} = leveled_fts:normalise_indexes([
        #{
            bucket => <<"docs">>,
            index => <<"verbatim">>,
            columns => [#{name => raw, path => [raw], mode => verbatim}]
        }
    ]),
    MaxToken = binary:copy(<<"v">>, 65535),
    {_Ref, _Marker, [{0, MaxToken, PositionBin}]} =
        leveled_fts:derive_doc(Schema, #{raw => MaxToken}, 1),
    {ok, [0]} = leveled_fts:decode_positions(PositionBin, 0, []),
    {_Ref2, _Marker2, []} = leveled_fts:derive_doc(Schema, #{raw => <<>>}, 2),
    TooLong = binary:copy(<<"v">>, 65536),
    {_Ref3, _Marker3, []} = leveled_fts:derive_doc(Schema, #{raw => TooLong}, 3).

delta_carrier_tag_discipline() ->
    {ok, [Schema]} = leveled_fts:normalise_indexes([
        #{
            bucket => <<"docs">>,
            tag => custom_object_tag,
            index => <<"custom">>,
            columns => [body]
        }
    ]),
    LK = leveled_codec:to_objectkey(
        <<"docs">>, <<"application-key">>, custom_object_tag
    ),
    {ok, Augmented, [_Touched]} = leveled_fts:augment_object_changes(
        [{LK, #{body => <<"term">>}, {[], infinity}}], [Schema], 1
    ),
    CarrierKeys = [
        Key
     || {{o, <<"docs">>, <<"$fts_d$", _/binary>> = Key, null}, <<0>>, _} <- Augmented
    ],
    1 = length(CarrierKeys).

merge_shapes() ->
    Token = <<"same">>,
    Runs = [
        leveled_fts:encode_entry(
            Token,
            1,
            leveled_fts:encode_frame(I, <<"k", (integer_to_binary(I))/binary>>, <<>>)
        )
     || I <- lists:seq(1, 32)
    ],
    Merged = leveled_fts:merge_streams_to_stream([<<>> | Runs]),
    [{Token, 32, MergedFrames}] = leveled_fts:fold_entries(
        Merged,
        fun(T, N, F, Acc) -> [{T, N, F} | Acc] end,
        []
    ),
    32 = leveled_fts:count_frames(MergedFrames),

    EmptyFramesEntry = leveled_fts:encode_entry(<<"empty">>, 0, <<>>),
    EmptyFramesEntry = leveled_fts:merge_streams_to_stream([
        <<>>, EmptyFramesEntry, <<>>
    ]),

    Single = leveled_fts:encode_entry(
        <<"single">>, 1, leveled_fts:encode_frame(1, <<"one">>, <<0>>)
    ),
    Single = leveled_fts:merge_streams_to_stream([Single]),

    Delta = leveled_fts:encode_delta([{0, Merged}, {1, Single}]),
    [{0, Merged}, {1, Single}] = leveled_fts:decode_delta(Delta).

tokenizer_path_parity() ->
    Corpus = [
        <<"ASCII Mixed 123">>,
        <<"caf", 16#E9/utf8, " e", 16#0301/utf8, "clair">>,
        <<16#4E2D/utf8, 16#570B/utf8, " mixed">>,
        <<16#AD11/utf8, 16#C6B4/utf8, " hangul">>
    ],
    lists:foreach(
        fun(Mode) ->
            Opts = #{
                tokenchars => [],
                separators => [],
                stopwords => [],
                remove_diacritics => Mode
            },
            lists:foreach(
                fun(Text) ->
                    Expected = leveled_fts:tokenize_unicode(Text, Opts),
                    Expected = leveled_fts:tokenize(Text, Opts)
                end,
                Corpus
            )
        end,
        [0, 1, 2]
    ).

load_export_all(Module, Source) ->
    Result = compile:file(Source, [binary, export_all, {i, ?INCLUDE_DIR}]),
    Binary =
        case Result of
            {ok, Module, Bin} -> Bin;
            {ok, Module, Bin, _Warnings} -> Bin
        end,
    _ = code:purge(Module),
    _ = code:delete(Module),
    {module, Module} = code:load_binary(Module, Source, Binary),
    ok.
