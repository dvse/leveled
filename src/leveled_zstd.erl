-module(leveled_zstd).

-export([compress/1, decompress/1, decompress_many/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% OTP 28 added stdlib:zstd.  The legacy zstd-erlang dependency uses the same
%% module name, which can collide with OTP's built-in NIF registration.  Prefer
%% the stdlib module when it exists, and fall back to the dependency on older OTP.

-spec compress(binary()) -> binary().
compress(Binary) when is_binary(Binary) ->
    ensure_stdlib_zstd_loaded(),
    iolist_to_binary(zstd:compress(Binary)).

-spec decompress(binary()) -> binary() | error.
decompress(Binary) when is_binary(Binary) ->
    ensure_stdlib_zstd_loaded(),
    try zstd:decompress(Binary) of
        error ->
            error;
        Output ->
            iolist_to_binary(Output)
    catch
        error:{zstd_error, _Reason} ->
            error
    end.

-spec decompress_many([{binary(), non_neg_integer()}]) -> [binary()] | error.
decompress_many([]) ->
    [];
decompress_many(Frames) when is_list(Frames) ->
    ensure_stdlib_zstd_loaded(),
    Compressed = iolist_to_binary([Frame || {Frame, _Bytes} <- Frames]),
    try zstd:decompress(Compressed) of
        error ->
            error;
        Output ->
            split_frames(
                iolist_to_binary(Output),
                [Bytes || {_Frame, Bytes} <- Frames],
                []
            )
    catch
        error:{zstd_error, _Reason} ->
            error
    end.

split_frames(<<>>, [], Acc) ->
    lists:reverse(Acc);
split_frames(Binary, [Bytes | Rest], Acc) when byte_size(Binary) >= Bytes ->
    <<Frame:Bytes/binary, Tail/binary>> = Binary,
    split_frames(Tail, Rest, [Frame | Acc]);
split_frames(_Binary, _Sizes, _Acc) ->
    error.

-spec ensure_stdlib_zstd_loaded() -> ok.
ensure_stdlib_zstd_loaded() ->
    case stdlib_zstd_beam() of
        {ok, StdlibZstd} ->
            ensure_loaded_from_stdlib(StdlibZstd);
        error ->
            ok
    end.

-spec stdlib_zstd_beam() -> {ok, string()} | error.
stdlib_zstd_beam() ->
    StdlibEbin = filename:join(code:lib_dir(stdlib), "ebin"),
    ZstdBeam = filename:join(StdlibEbin, "zstd.beam"),
    case filelib:is_regular(ZstdBeam) of
        true ->
            {ok, filename:rootname(ZstdBeam, ".beam")};
        false ->
            error
    end.

-spec ensure_loaded_from_stdlib(string()) -> ok.
ensure_loaded_from_stdlib(StdlibZstd) ->
    StdlibBeam = StdlibZstd ++ ".beam",
    case code:is_loaded(zstd) of
        {file, LoadedBeam} when LoadedBeam == StdlibBeam ->
            ok;
        {file, _OtherBeam} ->
            ok;
        false ->
            case code:load_abs(StdlibZstd) of
                {module, zstd} ->
                    ok;
                {error, sticky_directory} ->
                    ok;
                {error, _Reason} ->
                    ok
            end
    end.

-ifdef(TEST).
roundtrip_test() ->
    Payload = crypto:strong_rand_bytes(4096),
    ?assertEqual(Payload, decompress(compress(Payload))).

invalid_payload_test() ->
    ?assertEqual(error, decompress(<<"not a zstd frame">>)).
-endif.
