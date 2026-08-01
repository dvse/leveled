%% Immutable FTS2 row formats and key construction.
%%
%% Every generation-qualified row is write-once.  The only mutable row is the
%% root manifest; readers fetch it once and thereafter address one immutable
%% generation.  Values use the upstream zstd module directly.

-module(leveled_fts2_codec).

-export([
    root_key/0,
    term_key/3,
    term_range/3,
    bigram_key/4,
    identity_key/1,
    identity_subkey/1,
    encode/2,
    decode/2,
    encode_root/1,
    decode_root/1,
    encode_plane/1,
    decode_plane/1,
    encode_positions/1,
    decode_positions/1
]).

-define(ROOT_VERSION, 1).
-define(ROW_VERSION, 1).
-define(PLANE_VERSION, 1).
-define(POSITION_VERSION, 1).

root_key() ->
    {<<"f2:root">>, <<"manifest">>}.

term_key(Generation, Column, Token) ->
    {<<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>>, <<>>}.

term_range(Generation, Column, Prefix) ->
    Start = <<"f2:t:", Generation:64/unsigned-big, Column:8, Prefix/binary>>,
    {Start, <<Start/binary, 255>>}.

bigram_key(Generation, Column, First, Second) ->
    {
        <<"f2:g:", Generation:64/unsigned-big, Column:8,
            (byte_size(First)):16/unsigned-big, First/binary, Second/binary>>,
        <<>>
    }.

identity_key(Generation) ->
    <<"f2:i:", Generation:64/unsigned-big>>.

identity_subkey(PageNo) ->
    <<PageNo:32/unsigned-big>>.

encode(Type, Term) when is_atom(Type) ->
    Raw = term_to_binary(Term, [deterministic]),
    Compressed = iolist_to_binary(zstd:compress(Raw)),
    <<?ROW_VERSION:8, (type_id(Type)):8, (byte_size(Raw)):32/unsigned-big,
        Compressed/binary>>.

decode(
    Type,
    <<?ROW_VERSION:8, TypeId:8, RawBytes:32/unsigned-big, Compressed/binary>>
) ->
    case TypeId =:= type_id(Type) of
        true ->
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
decode(Type, _Bad) ->
    erlang:error({invalid_fts2_row, Type}).

encode_root(Root) ->
    Payload = term_to_binary(Root, [deterministic]),
    <<?ROOT_VERSION:8, (byte_size(Payload)):32/unsigned-big, Payload/binary>>.

decode_root(<<?ROOT_VERSION:8, Bytes:32/unsigned-big, Payload:Bytes/binary>>) ->
    binary_to_term(Payload, [safe]);
decode_root(Bad) ->
    erlang:error({invalid_fts2_root, Bad}).

%% Boolean/anchor planes are fixed-width and deliberately uncompressed.  They
%% are already dense integer streams and avoiding decompression keeps the full
%% result/hash path predictable.  Header/champion and identity rows use
%% encode/2 because those payloads contain arbitrary projected Ash values.
encode_plane(Entries) ->
    Payload = iolist_to_binary([
        <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
            SourceId:64/unsigned-big, DocLength:32/unsigned-big,
            Tf:32/unsigned-big>>
     || {ChunkId, GroupId, SourceId, DocLength, Tf} <- Entries
    ]),
    <<?PLANE_VERSION:8, (length(Entries)):32/unsigned-big, Payload/binary>>.

decode_plane(<<?PLANE_VERSION:8, Count:32/unsigned-big, Payload/binary>>) when
    byte_size(Payload) =:= Count * 24
->
    decode_plane_entries(Payload, []);
decode_plane(Bad) ->
    erlang:error({invalid_fts2_plane, Bad}).

decode_plane_entries(<<>>, Acc) ->
    lists:reverse(Acc);
decode_plane_entries(
    <<ChunkId:32/unsigned-big, GroupId:32/unsigned-big,
        SourceId:64/unsigned-big, DocLength:32/unsigned-big, Tf:32/unsigned-big,
        Rest/binary>>,
    Acc
) ->
    decode_plane_entries(
        Rest, [{ChunkId, GroupId, SourceId, DocLength, Tf} | Acc]
    ).

encode_positions(Entries) ->
    Payload = iolist_to_binary([
        begin
            Encoded = encode_position_list(Positions),
            <<ChunkId:32/unsigned-big, (length(Positions)):32/unsigned-big,
                (byte_size(Encoded)):32/unsigned-big, Encoded/binary>>
        end
     || {ChunkId, Positions} <- Entries
    ]),
    <<?POSITION_VERSION:8, (length(Entries)):32/unsigned-big, Payload/binary>>.

decode_positions(
    <<?POSITION_VERSION:8, Count:32/unsigned-big, Payload/binary>>
) ->
    decode_position_entries(Count, Payload, #{});
decode_positions(Bad) ->
    erlang:error({invalid_fts2_positions, Bad}).

decode_position_entries(0, <<>>, Acc) ->
    Acc;
decode_position_entries(
    Count,
    <<ChunkId:32/unsigned-big, PositionCount:32/unsigned-big,
        Bytes:32/unsigned-big, Encoded:Bytes/binary, Rest/binary>>,
    Acc
) when Count > 0 ->
    Positions = decode_position_list(PositionCount, Encoded, 0, []),
    decode_position_entries(Count - 1, Rest, Acc#{ChunkId => Positions});
decode_position_entries(_Count, Bad, _Acc) ->
    erlang:error({invalid_fts2_positions, Bad}).

encode_position_list(Positions) ->
    {_, Encoded} = lists:foldl(
        fun(Position, {Previous, Acc}) when
            is_integer(Position), Position >= Previous, Position =< 16#FFFFFFFF
        ->
            {Position, [encode_varint(Position - Previous) | Acc]}
        end,
        {0, []},
        Positions
    ),
    iolist_to_binary(lists:reverse(Encoded)).

decode_position_list(0, <<>>, _Previous, Acc) ->
    lists:reverse(Acc);
decode_position_list(Count, Encoded, Previous, Acc) when Count > 0 ->
    {Delta, Rest} = decode_varint(Encoded, 0, 0),
    Position = Previous + Delta,
    decode_position_list(Count - 1, Rest, Position, [Position | Acc]);
decode_position_list(_Count, Bad, _Previous, _Acc) ->
    erlang:error({invalid_fts2_position_list, Bad}).

encode_varint(Value) when Value < 128 ->
    <<Value>>;
encode_varint(Value) ->
    <<((Value band 127) bor 128), (encode_varint(Value bsr 7))/binary>>.

decode_varint(<<Byte, Rest/binary>>, Shift, Acc) when Shift =< 63 ->
    Value = Acc bor ((Byte band 127) bsl Shift),
    case Byte band 128 of
        0 -> {Value, Rest};
        _ -> decode_varint(Rest, Shift + 7, Value)
    end;
decode_varint(Bad, _Shift, _Acc) ->
    erlang:error({invalid_fts2_varint, Bad}).

type_id(header) -> 1;
type_id(identity) -> 2;
type_id(bigram_header) -> 3;
type_id(bigram_plane) -> 4;
type_id(delta) -> 5.
