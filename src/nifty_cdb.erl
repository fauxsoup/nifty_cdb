-module(nifty_cdb).
-behaviour(gen_server).
-export([
        new/0,
        new/1,
        start_link/0,
        start_link/1,
        open_file/2,
        fetch/2,
        walk/2,
        get_hash/1
    ]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(nifty_cdb, {
        file_handle         = undefined,
        current_pos         = 0,
        table_offsets       = gb_trees:empty()
    }).

new() ->
    supervisor:start_child(nifty_cdb_sup, []).

new(File) ->
    supervisor:start_child(nifty_cdb_sup, [File]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

start_link(File) ->
    gen_server:start_link(?MODULE, [File], []).

open_file(Pid, File) ->
    gen_server:cast(Pid, {open_file, File}).

walk(Pid, Fun) when is_function(Fun, 2) ->
    gen_server:cast(Pid, {walk, Fun}).

fetch(Pid, Key) ->
    gen_server:call(Pid, {fetch, get_hash(Key)}).

get_hash(Key) when is_list(Key) ->
    get_hash(iolist_to_binary(Key), <<5381:32/integer>>);
get_hash(Key) when is_binary(Key) ->
    get_hash(Key, <<5381:32/integer>>).

get_hash(<<>>, <<H:32/integer>>) ->
    H;
get_hash(<<C:8/integer, Rest/binary>>, <<H:32/integer>>) ->
    <<H2:32/integer>>   = <<(H bsl 5):32/integer>>,
    <<H3:32/integer>>   = <<(H2 + H):32/integer>>,
    Hash                = <<(H3 bxor C):32/integer>>,
    get_hash(Rest, Hash).

init([]) ->
    {ok, #nifty_cdb{}};
init([File]) ->
    open_file(self(), File),
    {ok, #nifty_cdb{}}.

handle_call({fetch, Hash}, _From, State = #nifty_cdb{table_offsets = Offsets}) ->
    Bucket = Hash rem 256,
    {Pointer, Length} = gb_trees:get(Bucket, Offsets),
    Slot = (Hash div 256) rem Length,
    SlotOffset = Pointer + (Slot * 8),
    io:format("Hash: ~p; Bucket: ~p; Bucket Size: ~p; Slot: ~p; Offset: ~p~n", [Hash, Bucket, Length, Slot, SlotOffset]),
    seek_record(Hash, State, SlotOffset);
handle_call(_Call, _From, State) ->
    {reply, undefined, State}.

handle_cast({open_file, File}, State) ->
    {ok, Handle} = file:open(File, [read,raw,binary]),
    {ok, NewState} = scan_file(State#nifty_cdb{file_handle = Handle}),
    {noreply, NewState};
handle_cast({walk, Fun}, State = #nifty_cdb{file_handle = Handle, table_offsets = Offsets}) ->
    {Pointer, _Length} = gb_trees:get(0, Offsets),
    Bytes = Pointer - 2048,         % 2048 is the size of the table pointers table
    Start = 2048,
    do_walk(Handle, Start, Bytes, Fun),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ================================================================
%% Internal Functions
%% ================================================================

scan_file(State = #nifty_cdb{file_handle = Handle}) ->
    case file:read(Handle, 2048) of
        {ok, Pointers} ->
            parse_pointers(Pointers, State);
        _ ->
            throw(bad_cdb)
    end.

parse_pointers(Data, State) ->
    parse_pointers(Data, State, 0).

parse_pointers(<<>>, State, _Index) ->
    {ok, State};
parse_pointers(<<Offset:32/unsigned-little-integer, Length:32/unsigned-little-integer, Rest/binary>>, State = #nifty_cdb{table_offsets = Offsets}, Index) ->
    parse_pointers(Rest, State#nifty_cdb{table_offsets = gb_trees:insert(Index, {Offset, Length}, Offsets)}, Index + 1).

fetch_record(RecordOffset, Handle) ->
    case file:pread(Handle, RecordOffset, 8) of
        {ok, <<KeyLen:32/unsigned-little-integer, DataLen:32/unsigned-little-integer>>} ->
            case file:pread(Handle, RecordOffset + 8, KeyLen + DataLen) of
                {ok, <<Key:KeyLen/binary, Data:DataLen/binary>>} ->
                    {Key, Data};
                _ ->
                    throw(bad_cdb)
            end;
        _ ->
            throw(bad_cdb)
    end.

seek_record(Hash, State = #nifty_cdb{file_handle = Handle}, SlotOffset) ->
    case file:pread(Handle, SlotOffset, 8) of
        {ok, <<Hash:32/unsigned-little-integer, RecordOffset:32/unsigned-little-integer>>} ->
            io:format("Record Offset: ~p~n", [RecordOffset]),
            {NewOffset, Record} = fetch_record(RecordOffset, Handle),
            {reply, {ok, Record}, State#nifty_cdb{current_pos = NewOffset}};
        {ok, <<Other:32/unsigned-little-integer, Offset:32/unsigned-little-integer>>} ->
            io:format("Hash: ~p~n; Other: ~p; Offset: ~p~n", [Hash, Other, Offset]),
            seek_record(Hash, State, SlotOffset - 8);
        eof ->
            {reply, {ow, it_hurts}, State}
    end.

do_walk(Handle, Start, Bytes, Fun) ->
    do_walk(Handle, Start, Bytes, Fun, <<>>).

do_walk(_Handle, _Start, 0, _Fun, <<>>) ->
    ok;
do_walk(Handle, Start, Bytes, Fun, <<KeyLen:32/unsigned-little-integer, DataLen:32/unsigned-little-integer, Key:KeyLen/binary, Data:DataLen/binary, Rest/binary>>) ->
    Fun(Key, Data),
    do_walk(Handle, Start, Bytes, Fun, Rest);
do_walk(Handle, Start, Bytes, Fun, Stub) when Bytes > 1024 ->
    case file:pread(Handle, Start, 1024) of
        {ok, Data} ->
            do_walk(Handle, Start + 1024, Bytes - 1024, Fun, <<Stub/binary, Data/binary>>);
        _ ->
            throw(bad_cdb)
    end;
do_walk(Handle, Start, Bytes, Fun, Stub) ->
    case file:pread(Handle, Start, Bytes) of
        {ok, Data} ->
            do_walk(Handle, Start + Bytes, 0, Fun, <<Stub/binary, Data/binary>>);
        _ ->
            throw(bad_cdb)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

basic_test() ->
    {ok, Ref} = new(),
    ?assertEqual({ok, '_'}, open_file(Ref,"./tmp.cdb")).

-endif.
