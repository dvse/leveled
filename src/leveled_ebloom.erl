%% -------- TinyBloom ---------
%%
%% A 1-byte per key bloom filter with a 5% fpr.  Pre-prepared segment hashes
%% (a leveled codec type) are, used for building and checking - the filter
%% splits a single hash into a 1 byte slot identifier, and 2 x 12 bit hashes
%% (so k=2, although only a single hash is used).
%%
%% The filter is designed to support a maximum of 64K keys, larger numbers of
%% keys will see higher fprs - with a 40% fpr at 250K keys.
%%
%% The filter uses the second "Extra Hash" part of the segment-hash to ensure
%% no overlap of fpr with the leveled_sst find_pos function.
%%
%% The completed bloom is a binary - to minimise the cost of copying between
%% processes and holding in memory.

-module(leveled_ebloom).

-export([
    create_bloom/1,
    create_bloom/2,
    check_hash/2
]).

-ifdef(TEST).
-export([set_test_observer/1]).
-endif.

-define(BLOOM_SLOTSIZE_BYTES, 512).
-define(INTEGER_SLICE_SIZE, 64).
-define(INTEGER_SLICES, 64).
% i.e. ?INTEGER_SLICES * ?INTEGER_SLICE_SIZE = ?BLOOM_SLOTSIZE_BYTES div 8
-define(MASK_BSR, 6).
% i.e. 2 ^ (12 - 6) = ?INTEGER_SLICES
-define(MASK_BAND, 63).
% i.e. integer slize size - 1
-define(SPLIT_BAND, 4095).
% i.e. (?BLOOM_SLOTSIZE_BYTES * 8) - 1

-type bloom() :: binary().

-export_type([bloom/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec create_bloom(list(leveled_codec:segment_hash())) -> bloom().
%% @doc
%% Create a binary bloom filter from a list of hashes.  In the leveled
%% implementation the hashes are leveled_codec:segment_hash/0 type, but only
%% a single 32-bit hash (the second element of the tuple is actually used in
%% the building of the bloom filter
create_bloom(HashList) ->
    create_bloom([HashList], length(HashList)).

-spec create_bloom(
    [list(leveled_codec:segment_hash())], non_neg_integer()
) -> bloom().
%% @doc
%% Create a bloom from bounded batches of hashes. The caller supplies the
%% total count because the bloom's fixed slot width depends on it. Bloom bits
%% are accumulated directly into a fixed atomics array capped at 128 * 64
%% words; no generation-sized flat hash list or per-slot hash lists are
%% constructed.
create_bloom(HashLists, HashCount) ->
    observe_build(HashCount),
    SlotCount =
        case HashCount of
            0 ->
                0;
            L ->
                min(128, max(2, (L - 1) div 512))
        end,
    BloomWords = lists:foldl(
        fun(HashList, Acc) -> map_hashes(HashList, Acc, SlotCount) end,
        new_bloom_words(SlotCount),
        HashLists
    ),
    build_bloom(BloomWords, SlotCount).

-spec check_hash(leveled_codec:segment_hash(), bloom()) -> boolean().
%% @doc
%% Check for the presence of a given hash within a bloom. Only the second
%% element of the leveled_codec:segment_hash/0 type is used - a 32-bit hash.
check_hash(_Hash, <<>>) ->
    false;
check_hash({_SegHash, Hash}, BloomBin) when is_binary(BloomBin) ->
    SlotSplit = byte_size(BloomBin) div ?BLOOM_SLOTSIZE_BYTES,
    {Slot, [H0, H1]} = split_hash(Hash, SlotSplit),
    Pos = ((Slot + 1) * ?BLOOM_SLOTSIZE_BYTES) - 1,
    case match_hash(BloomBin, Pos - (H0 div 8), H0 rem 8) of
        true ->
            match_hash(BloomBin, Pos - (H1 div 8), H1 rem 8);
        _ ->
            false
    end.

%%%============================================================================
%%% Internal Functions
%%%============================================================================

-type slot_count() :: 0 | 2..128.
-type bloom_hash() :: 0..16#FFF.
-type external_hash() :: 0..16#FFFFFFFF.

-spec map_hashes(
    list(leveled_codec:segment_hash()), reference() | undefined, slot_count()
) -> reference() | undefined.
map_hashes([], BloomWords, _SlotCount) ->
    BloomWords;
map_hashes([{_SH, EH} | Rest], BloomWords, SlotCount) ->
    {Slot, [H0, H1]} = split_hash(EH, SlotCount),
    WithH0 = add_hash_word(Slot, H0, BloomWords),
    map_hashes(
        Rest,
        add_hash_word(Slot, H1, WithH0),
        SlotCount
    ).

-spec add_hash_word(
    non_neg_integer(), bloom_hash(), reference()
) -> reference().
add_hash_word(Slot, Hash, BloomWords) ->
    Word = Slot * ?INTEGER_SLICES + (Hash bsr ?MASK_BSR) + 1,
    Mask = 1 bsl (Hash band ?MASK_BAND),
    atomics:put(BloomWords, Word, atomics:get(BloomWords, Word) bor Mask),
    BloomWords.

-spec new_bloom_words(slot_count()) -> reference() | undefined.
new_bloom_words(0) ->
    undefined;
new_bloom_words(SlotCount) ->
    atomics:new(SlotCount * ?INTEGER_SLICES, [{signed, false}]).

-spec split_hash(external_hash(), slot_count()) ->
    {non_neg_integer(), [bloom_hash()]}.
split_hash(Hash, SlotSplit) ->
    Slot = (Hash band 255) rem SlotSplit,
    H0 = (Hash bsr 8) band ?SPLIT_BAND,
    H1 = (Hash bsr 20) band ?SPLIT_BAND,
    {Slot, [H0, H1]}.

-spec match_hash(bloom(), non_neg_integer(), 0..16#FF) -> boolean().
match_hash(BloomBin, Pos, Hash) ->
    <<_Pre:Pos/binary, CheckInt:8/integer, _Rest/binary>> = BloomBin,
    (CheckInt bsr Hash) band 1 == 1.

-spec build_bloom(reference() | undefined, slot_count()) -> bloom().
build_bloom(_BloomWords, 0) ->
    <<>>;
build_bloom(BloomWords, SlotCount) when SlotCount > 0 ->
    iolist_to_binary([
        <<(atomics:get(
            BloomWords,
            Slot * ?INTEGER_SLICES + Slice + 1
        )):?INTEGER_SLICE_SIZE/unsigned-big>>
     || Slot <- lists:seq(0, SlotCount - 1),
        Slice <- lists:seq(?INTEGER_SLICES - 1, 0, -1)
    ]).

-ifdef(TEST).
observe_build(HashCount) ->
    case persistent_term:get({?MODULE, test_observer}, undefined) of
        Pid when is_pid(Pid) ->
            Pid ! {fts2_build_worker, {ebloom_worker, HashCount}, self()},
            ok;
        undefined ->
            ok
    end.
-else.
observe_build(_HashCount) ->
    ok.
-endif.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

set_test_observer(undefined) ->
    persistent_term:erase({?MODULE, test_observer}),
    ok;
set_test_observer(Pid) when is_pid(Pid) ->
    persistent_term:put({?MODULE, test_observer}, Pid),
    ok.

generate_orderedkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    generate_orderedkeys(Seqn, Count, [], BucketRangeLow, BucketRangeHigh).

generate_orderedkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_orderedkeys(Seqn, Count, Acc, BucketLow, BucketHigh) ->
    BNumber = Seqn div (BucketHigh - BucketLow),
    BucketExt =
        io_lib:format("K~4..0B", [BucketLow + BNumber]),
    KeyExt =
        io_lib:format("K~8..0B", [Seqn * 100 + rand:uniform(100)]),
    LK =
        leveled_codec:to_objectkey(
            list_to_binary("Bucket" ++ BucketExt),
            list_to_binary("Key" ++ KeyExt),
            o
        ),
    Chunk = crypto:strong_rand_bytes(16),
    MV = leveled_codec:convert_to_ledgerv(LK, Seqn, Chunk, 64, infinity),
    generate_orderedkeys(
        Seqn + 1, Count - 1, [{LK, MV} | Acc], BucketLow, BucketHigh
    ).

get_hashlist(N) ->
    KVL = generate_orderedkeys(1, N, 1, 20),
    HashFun =
        fun({K, _V}) ->
            leveled_codec:segment_hash(K)
        end,
    lists:map(HashFun, KVL).

check_all_hashes(BloomBin, HashList) ->
    CheckFun =
        fun(Hash) ->
            ?assertMatch(true, check_hash(Hash, BloomBin))
        end,
    lists:foreach(CheckFun, HashList).

check_neg_hashes(BloomBin, HashList, Counters) ->
    CheckFun =
        fun(Hash, {AccT, AccF}) ->
            case check_hash(Hash, BloomBin) of
                true ->
                    {AccT + 1, AccF};
                false ->
                    {AccT, AccF + 1}
            end
        end,
    lists:foldl(CheckFun, Counters, HashList).

empty_bloom_test() ->
    BloomBin0 = create_bloom([]),
    ?assertMatch(
        {0, 4}, check_neg_hashes(BloomBin0, [0, 10, 100, 100000], {0, 0})
    ).

chunked_bloom_equivalence_test() ->
    HashList = get_hashlist(20000),
    {First, Rest} = lists:split(7311, HashList),
    {Second, Third} = lists:split(4227, Rest),
    Flat = create_bloom(HashList),
    Flat = create_bloom([First, Second, Third], length(HashList)),
    check_all_hashes(Flat, HashList).

bloom_test_() ->
    {timeout, 120, fun bloom_test_ranges/0}.

bloom_test_ranges() ->
    test_bloom(250000, 2),
    test_bloom(80000, 4),
    test_bloom(60000, 4),
    test_bloom(40000, 4),
    test_bloom(128 * 256, 4),
    test_bloom(20000, 4),
    test_bloom(10000, 4),
    test_bloom(5000, 4),
    test_bloom(2000, 4),
    test_bloom(1000, 4).

test_bloom(N, Runs) ->
    ListOfHashLists =
        lists:map(fun(_X) -> get_hashlist(N * 2) end, lists:seq(1, Runs)),
    SpliListFun =
        fun(HashList) ->
            HitOrMissFun =
                fun(Entry, {HitL, MissL}) ->
                    case rand:uniform() < 0.5 of
                        true ->
                            {[Entry | HitL], MissL};
                        false ->
                            {HitL, [Entry | MissL]}
                    end
                end,
            lists:foldl(HitOrMissFun, {[], []}, HashList)
        end,
    SplitListOfHashLists = lists:map(SpliListFun, ListOfHashLists),

    SWa = os:timestamp(),
    ListOfBlooms =
        lists:map(
            fun({HL, _ML}) -> create_bloom(HL) end, SplitListOfHashLists
        ),
    TSa = timer:now_diff(os:timestamp(), SWa) / Runs,

    SWb = os:timestamp(),
    PosChecks =
        lists:foldl(
            fun(Nth, ChecksMade) ->
                {HL, _ML} = lists:nth(Nth, SplitListOfHashLists),
                BB = lists:nth(Nth, ListOfBlooms),
                check_all_hashes(BB, HL),
                ChecksMade + length(HL)
            end,
            0,
            lists:seq(1, Runs)
        ),
    TSb = timer:now_diff(os:timestamp(), SWb),

    SWc = os:timestamp(),
    {Pos, Neg} =
        lists:foldl(
            fun(Nth, Acc) ->
                {_HL, ML} = lists:nth(Nth, SplitListOfHashLists),
                BB = lists:nth(Nth, ListOfBlooms),
                check_neg_hashes(BB, ML, Acc)
            end,
            {0, 0},
            lists:seq(1, Runs)
        ),
    FPR = Pos / (Pos + Neg),
    TSc = timer:now_diff(os:timestamp(), SWc),

    BytesPerKey =
        (lists:sum(lists:map(fun byte_size/1, ListOfBlooms)) div 4) / N,

    io:format(
        user,
        "Test with size ~w has microsecond timings: - "
        "build in ~w then ~.3f per pos-check, ~.3f per neg-check, "
        "fpr ~.3f with bytes-per-key ~.3f~n",
        [N, round(TSa), TSb / PosChecks, TSc / (Pos + Neg), FPR, BytesPerKey]
    ).

split_builder_speed_test_() ->
    {timeout, 60, fun split_builder_speed_tester/0}.

split_builder_speed_tester() ->
    N = 40000,
    Runs = 50,
    ListOfHashLists =
        lists:map(fun(_X) -> get_hashlist(N * 2) end, lists:seq(1, Runs)),

    Timings =
        lists:map(
            fun(HashList) ->
                SlotCount = min(128, max(2, (length(HashList) - 1) div 512)),
                {MTC, BloomWords} =
                    timer:tc(
                        fun map_hashes/3,
                        [HashList, new_bloom_words(SlotCount), SlotCount]
                    ),
                {BTC, _Bloom} =
                    timer:tc(
                        fun build_bloom/2, [BloomWords, SlotCount]
                    ),
                {MTC, BTC}
            end,
            ListOfHashLists
        ),
    {MTs, BTs} = lists:unzip(Timings),
    io:format(
        user,
        "Total time in microseconds for map_hashlist ~w build_bloom ~w~n",
        [lists:sum(MTs), lists:sum(BTs)]
    ).

-endif.
