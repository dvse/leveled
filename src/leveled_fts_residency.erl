%% Immutable FTS generation residency.
%%
%% This is deliberately generation-scoped index state, not a serving cache:
%% rows are loaded as one bounded immutable generation, serving never adds or
%% replaces entries, and the published pointer is removed atomically when the
%% FTS state row changes.  Any family that does not fit the configured budget
%% remains on the ordinary SST path.

-module(leveled_fts_residency).

-include("leveled.hrl").

-export([
    prepare/2,
    invalidate_specs/2,
    headonly/4,
    headonly_many/3,
    header_values/5,
    status/2
]).

-define(ROW_OVERHEAD_BYTES, 64).

prepare(Bookie, #{index := Bucket, fingerprint := Fingerprint} = Schema) ->
    case current_descriptor(Bookie, Bucket) of
        #{fingerprint := Fingerprint} ->
            ok;
        _ ->
            case leveled_bookie:book_fts_residency_budget(Bookie) of
                Budget when is_integer(Budget), Budget > 0 ->
                    Lock = {{?MODULE, Bookie, Bucket}, self()},
                    _ = global:trans(
                        Lock,
                        fun() -> prepare_locked(Bookie, Schema, Budget) end,
                        [node()]
                    ),
                    ok;
                _ ->
                    ok
            end
    end.

prepare_locked(Bookie, #{index := Bucket, fingerprint := Fingerprint} = Schema,
        Budget) ->
    case current_descriptor(Bookie, Bucket) of
        #{fingerprint := Fingerprint} ->
            ok;
        _ ->
            case leveled_bookie:book_headonly(
                Bookie, Bucket, <<"f2:state">>, <<"current">>
            ) of
                {ok, <<1, RootValue/binary>>} ->
                    case decode_root(RootValue) of
                        #{fingerprint := Fingerprint} = Root ->
                            start_owner(Bookie, Schema, Root, RootValue, Budget);
                        _OtherSchema ->
                            ok
                    end;
                _DirtyOrAbsent ->
                    ok
            end
    end.

start_owner(Bookie, Schema, Root, RootValue, Budget) ->
    Caller = self(),
    Ref = make_ref(),
    _Pid = spawn(fun() ->
        owner_load(Caller, Ref, Bookie, Schema, Root, RootValue, Budget)
    end),
    receive
        {Ref, ready, _Descriptor} -> ok;
        {Ref, error, _Reason} -> ok
    end.

owner_load(Caller, Ref, Bookie, #{index := Bucket} = Schema, Root, RootValue,
        Budget) ->
    IdentityBookie = maps:get(identity_bookie, Schema, Bookie),
    MainMonitor = erlang:monitor(process, Bookie),
    IdentityMonitor = case IdentityBookie =:= Bookie of
        true -> MainMonitor;
        false -> erlang:monitor(process, IdentityBookie)
    end,
    Table = ets:new(?MODULE, [ordered_set, public, {read_concurrency, true}]),
    try
        Generation = maps:get(generation, Root),
        Initial = #{
            table => Table,
            budget => Budget,
            charged_bytes => 0,
            payload_bytes => 0,
            row_count => 0,
            complete => #{},
            family_fits => true
        },
        S0 = maps:remove(
            family_fits,
            load_control(Bookie, Bucket, Generation, RootValue, Initial)
        ),
        S1 = load_family(
            Bookie, Bucket, main, term_header, Generation, S0
        ),
        S2 = load_family(
            Bookie, Bucket, main, boolean_plane, Generation, S1
        ),
        S3 = load_family(Bookie, Bucket, main, anchor_plane, Generation, S2),
        S4 = load_family(
            IdentityBookie, Bucket, identity, identity_page, Generation, S3
        ),
        S5 = load_family(
            Bookie, Bucket, main, position_plane, Generation, S4
        ),
        S6 = load_family(Bookie, Bucket, main, bigram, Generation, S5),
        Descriptor = #{
            owner => self(),
            table => Table,
            bookie => Bookie,
            identity_bookie => IdentityBookie,
            bucket => Bucket,
            generation => Generation,
            fingerprint => maps:get(fingerprint, Root),
            root => Root,
            budget_bytes => Budget,
            resident_bytes => maps:get(charged_bytes, S6),
            payload_bytes => maps:get(payload_bytes, S6),
            row_count => maps:get(row_count, S6),
            ets_table_bytes => ets:info(Table, memory) * erlang:system_info(wordsize),
            complete => maps:get(complete, S6)
        },
        persistent_term:put(generation_key(Generation), Descriptor),
        persistent_term:put(current_key(Bookie, Bucket), Generation),
        persistent_term:put(current_key(IdentityBookie, Bucket), Generation),
        persistent_term:put(status_key(Bookie), status_map(Descriptor)),
        Caller ! {Ref, ready, Descriptor},
        owner_loop(Descriptor, MainMonitor, IdentityMonitor)
    catch
        Class:Reason:Stacktrace ->
            true = ets:delete(Table),
            Caller ! {Ref, error, {Class, Reason, Stacktrace}}
    end.

owner_loop(Descriptor, MainMonitor, IdentityMonitor) ->
    receive
        stop ->
            cleanup(Descriptor);
        {'DOWN', MainMonitor, process, _Pid, _Reason} ->
            cleanup(Descriptor);
        {'DOWN', IdentityMonitor, process, _Pid, _Reason} ->
            cleanup(Descriptor)
    end.

cleanup(#{generation := Generation, bookie := Bookie,
        identity_bookie := IdentityBookie, bucket := Bucket, table := Table}) ->
    erase_if(current_key(Bookie, Bucket), Generation),
    erase_if(current_key(IdentityBookie, Bucket), Generation),
    erase_if(status_key(Bookie), Generation, generation),
    persistent_term:erase(generation_key(Generation)),
    try ets:delete(Table) catch error:badarg -> true end,
    ok.

invalidate_specs(Bookie, Specs) when is_list(Specs) ->
    Buckets = lists:usort([
        Bucket
     || {_Op, Bucket, <<"f2:state">>, <<"current">>, _Value} <- Specs
    ]),
    lists:foreach(fun(Bucket) -> invalidate(Bookie, Bucket) end, Buckets),
    ok.

invalidate(Bookie, Bucket) ->
    case persistent_term:get(current_key(Bookie, Bucket), undefined) of
        Generation when is_integer(Generation) ->
            case persistent_term:get(generation_key(Generation), undefined) of
                #{owner := Owner, identity_bookie := IdentityBookie} ->
                    persistent_term:erase(current_key(Bookie, Bucket)),
                    persistent_term:erase(current_key(IdentityBookie, Bucket)),
                    persistent_term:erase(generation_key(Generation)),
                    erase_if(status_key(Bookie), Generation, generation),
                    Owner ! stop;
                _ ->
                    persistent_term:erase(current_key(Bookie, Bucket))
            end;
        _ ->
            ok
    end.

headonly(Bookie, Bucket, Key, SubKey) ->
    case resident_result(Bookie, Bucket, Key, SubKey) of
        {resident, Result} -> Result;
        fallback -> leveled_bookie:book_headonly(Bookie, Bucket, Key, SubKey)
    end.

headonly_many(Bookie, Bucket, Keys) ->
    Probed = [resident_result(Bookie, Bucket, Key, SubKey) ||
        {Key, SubKey} <- Keys],
    Missing = [Key || {Key, fallback} <- lists:zip(Keys, Probed)],
    Fallback = case Missing of
        [] -> [];
        _ -> leveled_bookie:book_headonly_many(Bookie, Bucket, Missing)
    end,
    merge_results(Probed, Fallback, []).

merge_results([], [], Acc) ->
    lists:reverse(Acc);
merge_results([{resident, Result} | Rest], Fallback, Acc) ->
    merge_results(Rest, Fallback, [Result | Acc]);
merge_results([fallback | Rest], [Result | Fallback], Acc) ->
    merge_results(Rest, Fallback, [Result | Acc]).

%% Returns raw encoded header values for a complete resident header family.
%% A partial family falls back as a unit so prefix semantics cannot be split
%% across memory and SST traversal.
header_values(Bookie, Bucket, Generation, Column, Prefix) ->
    case current_descriptor(Bookie, Bucket) of
        #{generation := Generation, table := Table,
            complete := #{term_header := true}} ->
            Start = <<"f2:t:", Generation:64/unsigned-big, Column:8,
                Prefix/binary>>,
            Finish = <<Start/binary, 255>>,
            try
                {resident, header_values_next(
                    Table, ets:next(Table, {main, Start, <<>>}),
                    Generation, Column, Finish, []
                )}
            catch
                error:badarg -> fallback
            end;
        _ ->
            fallback
    end.

header_values_next(_Table, '$end_of_table', _Generation, _Column, _Finish,
        Acc) ->
    lists:reverse(Acc);
header_values_next(Table, {main, Key, <<"h">>} = TableKey, Generation,
        Column, Finish, Acc) when Key =< Finish ->
    NextAcc = case Key of
        <<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
            [{Key, Token, ets:lookup_element(Table, TableKey, 2)} | Acc];
        _ -> Acc
    end,
    header_values_next(
        Table, ets:next(Table, TableKey), Generation, Column, Finish, NextAcc
    );
header_values_next(Table, {main, Key, _SubKey} = TableKey, Generation,
        Column, Finish, Acc) when Key =< Finish ->
    %% Anchor/control rows can sort before the header row for the same term.
    %% They belong to another resident family and must not terminate the
    %% ordered header scan.
    header_values_next(
        Table, ets:next(Table, TableKey), Generation, Column, Finish, Acc
    );
header_values_next(_Table, _TableKey, _Generation, _Column, _Finish, Acc) ->
    lists:reverse(Acc).

status(Bookie, Budget) ->
    case persistent_term:get(status_key(Bookie), undefined) of
        undefined -> #{
            fts_residency_budget_bytes => Budget,
            fts_resident_generation => undefined,
            fts_resident_bytes => 0,
            fts_resident_payload_bytes => 0,
            fts_resident_ets_table_bytes => 0,
            fts_resident_rows => 0,
            fts_resident_complete => #{}
        };
        Resident -> Resident#{fts_residency_budget_bytes => Budget}
    end.

status_map(Descriptor) ->
    #{
        generation => maps:get(generation, Descriptor),
        fts_residency_budget_bytes => maps:get(budget_bytes, Descriptor),
        fts_resident_generation => maps:get(generation, Descriptor),
        fts_resident_bytes => maps:get(resident_bytes, Descriptor),
        fts_resident_payload_bytes => maps:get(payload_bytes, Descriptor),
        fts_resident_ets_table_bytes => maps:get(ets_table_bytes, Descriptor),
        fts_resident_rows => maps:get(row_count, Descriptor),
        fts_resident_complete => maps:get(complete, Descriptor)
    }.

resident_result(Bookie, Bucket, Key, SubKey) ->
    case current_descriptor(Bookie, Bucket) of
        #{table := Table} = Descriptor ->
            Scope = scope(Bookie, Descriptor),
            case Scope of
                none -> fallback;
                _ ->
                    try ets:lookup(Table, {Scope, Key, SubKey}) of
                        [{_TableKey, Value}] -> {resident, {ok, Value}};
                        [] -> case complete_for(Key, SubKey, Scope, Descriptor) of
                            true -> {resident, not_found};
                            false -> fallback
                        end
                    catch
                        error:badarg -> fallback
                    end
            end;
        _ ->
            fallback
    end.

scope(Bookie, #{bookie := Bookie}) -> main;
scope(Bookie, #{identity_bookie := Bookie}) -> identity;
scope(_Bookie, _Descriptor) -> none.

complete_for(<<"f2:state">>, <<"current">>, main, Descriptor) ->
    family_complete(control, Descriptor);
complete_for(<<"f2:root">>, <<"manifest">>, main, Descriptor) ->
    family_complete(control, Descriptor);
complete_for(<<"f2:bloom">>, _SubKey, main, Descriptor) ->
    maps:get(control, maps:get(complete, Descriptor), false);
complete_for(<<"f2:t:", Generation:64/unsigned-big, _/binary>>, <<"h">>,
        main, #{generation := Generation} = Descriptor) ->
    family_complete(term_header, Descriptor);
complete_for(<<"f2:t:", Generation:64/unsigned-big, _/binary>>, <<"a">>,
        main, #{generation := Generation} = Descriptor) ->
    family_complete(anchor_plane, Descriptor);
complete_for(<<"f2:b:", Generation:64/unsigned-big, _/binary>>, <<"b">>,
        main, #{generation := Generation} = Descriptor) ->
    family_complete(boolean_plane, Descriptor);
complete_for(<<"f2:p:", Generation:64/unsigned-big, _/binary>>, <<"p">>,
        main, #{generation := Generation} = Descriptor) ->
    family_complete(position_plane, Descriptor);
complete_for(<<"f2:g:", Generation:64/unsigned-big, _/binary>>, <<"x">>,
        main, #{generation := Generation} = Descriptor) ->
    family_complete(bigram, Descriptor);
complete_for(<<"f2:i:", Generation:64/unsigned-big>>, _Page, identity,
        #{generation := Generation} = Descriptor) ->
    family_complete(identity_page, Descriptor);
complete_for(_Key, _SubKey, _Scope, _Descriptor) -> false.

family_complete(Family, Descriptor) ->
    maps:get(Family, maps:get(complete, Descriptor), false).

current_descriptor(Bookie, Bucket) ->
    case persistent_term:get(current_key(Bookie, Bucket), undefined) of
        Generation when is_integer(Generation) ->
            persistent_term:get(generation_key(Generation), undefined);
        _ ->
            undefined
    end.

load_control(Bookie, Bucket, Generation, RootValue, State0) ->
    State1 = insert_value(
        main, <<"f2:root">>, <<"manifest">>, RootValue, State0
    ),
    State2 = insert_value(
        main, <<"f2:state">>, <<"current">>, <<1, RootValue/binary>>, State1
    ),
    Keys = [{<<"f2:bloom">>, <<"current">>}] ++
        [{<<"f2:bloom">>, <<"s", Shard:8>>} || Shard <- lists:seq(0, 255)] ++
        [{<<"f2:bloom">>, <<"g", Shard:8>>} || Shard <- lists:seq(0, 255)],
    Results = leveled_bookie:book_headonly_many(Bookie, Bucket, Keys),
    State3 = lists:foldl(
        fun
            ({{Key, SubKey}, {ok, <<1, StoredGeneration:64/unsigned-big,
                    _/binary>> = Value}}, Acc) when
                    StoredGeneration =:= Generation ->
                insert_value(main, Key, SubKey, Value, Acc);
            ({{_Key, _SubKey}, not_found}, Acc) ->
                Acc
        end,
        State2,
        lists:zip(Keys, Results)
    ),
    mark_complete(
        control,
        State3,
        maps:get(family_fits, State3, true)
    ).

load_family(Bookie, Bucket, Scope, Family, Generation, State0) ->
    {Start, Finish} = family_range(Family, Generation),
    Table = maps:get(table, State0),
    Fold = fun
        (B, {Key, SubKey}, Value, Acc) when B =:= Bucket ->
            case family_row(Family, Generation, Key, SubKey) of
                true -> insert_value(Table, Scope, Key, SubKey, Value, Acc);
                false -> Acc
            end;
        (_B, _Key, _Value, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Start, <<>>}, {Finish, <<255, 255, 255, 255>>}}},
        {Fold, State0#{family_fits => true}},
        false,
        true,
        false
    ),
    State1 = Runner(),
    Fits = maps:get(family_fits, State1),
    mark_complete(Family, maps:remove(family_fits, State1), Fits).

insert_value(Scope, Key, SubKey, Value, State) ->
    insert_value(maps:get(table, State), Scope, Key, SubKey, Value, State).

insert_value(_Table, _Scope, _Key, _SubKey, _Value,
        #{family_fits := false} = State) ->
    State;
insert_value(Table, Scope, Key, SubKey, Value, State) ->
    Charge = byte_size(Key) + byte_size(SubKey) + byte_size(Value) +
        ?ROW_OVERHEAD_BYTES,
    NextBytes = maps:get(charged_bytes, State) + Charge,
    case NextBytes =< maps:get(budget, State) of
        true ->
            true = ets:insert(Table, {{Scope, Key, SubKey}, Value}),
            State#{
                charged_bytes => NextBytes,
                payload_bytes => maps:get(payload_bytes, State) +
                    byte_size(Value),
                row_count => maps:get(row_count, State) + 1
            };
        false ->
            State#{family_fits => false}
    end.

mark_complete(Family, State, Fits) ->
    Complete = maps:get(complete, State),
    State#{complete => Complete#{Family => Fits}}.

family_range(term_header, Generation) -> generation_range("f2:t:", Generation);
family_range(boolean_plane, Generation) -> generation_range("f2:b:", Generation);
family_range(position_plane, Generation) -> generation_range("f2:p:", Generation);
family_range(bigram, Generation) -> generation_range("f2:g:", Generation);
family_range(anchor_plane, Generation) -> generation_range("f2:t:", Generation);
family_range(identity_page, Generation) ->
    Key = <<"f2:i:", Generation:64/unsigned-big>>,
    {Key, Key}.

generation_range(Prefix0, Generation) ->
    Prefix = list_to_binary(Prefix0),
    Start = <<Prefix/binary, Generation:64/unsigned-big>>,
    {Start, <<Start/binary, 255>>}.

family_row(term_header, Generation,
        <<"f2:t:", Generation:64/unsigned-big, _/binary>>, <<"h">>) -> true;
family_row(anchor_plane, Generation,
        <<"f2:t:", Generation:64/unsigned-big, _/binary>>, <<"a">>) -> true;
family_row(boolean_plane, Generation,
        <<"f2:b:", Generation:64/unsigned-big, _/binary>>, <<"b">>) -> true;
family_row(position_plane, Generation,
        <<"f2:p:", Generation:64/unsigned-big, _/binary>>, <<"p">>) -> true;
family_row(bigram, Generation,
        <<"f2:g:", Generation:64/unsigned-big, _/binary>>, <<"x">>) -> true;
family_row(identity_page, Generation,
        <<"f2:i:", Generation:64/unsigned-big>>, _Page) -> true;
family_row(_Family, _Generation, _Key, _SubKey) -> false.

decode_root(<<1, Bytes:32/unsigned-big, Payload:Bytes/binary>>) ->
    binary_to_term(Payload, [safe]).

current_key(Bookie, Bucket) -> {?MODULE, current, Bookie, Bucket}.
generation_key(Generation) -> {?MODULE, Generation}.
status_key(Bookie) -> {?MODULE, status, Bookie}.

erase_if(Key, Expected) ->
    case persistent_term:get(Key, undefined) of
        Expected -> persistent_term:erase(Key);
        _ -> ok
    end.

erase_if(Key, Expected, Field) ->
    case persistent_term:get(Key, undefined) of
        #{Field := Expected} -> persistent_term:erase(Key);
        _ -> ok
    end.
