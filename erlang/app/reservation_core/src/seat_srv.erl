%% erlang/apps/reservation_core/src/seat_srv.erl
-module(seat_srv).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

init(State) ->
    _ = application:start(mnesia),
    io:format("seat_srv started on ~p~n", [node()]),
    {ok, State}.

%% (Optional) keep calls for later
handle_call(_Req, _From, State) ->
    {reply, {error, unsupported_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% =========================================================
%% Message-passing API (used by JInterface and your Erlang test)
%%
%% Expected message:
%%   {write_seat, FromPid, <<"E1">>, <<"A1">>}
%%
%% Reply:
%%   {write_seat_reply, {EventId, SeatId}, ok | {error, Reason}}
%% =========================================================
handle_info({write_seat, FromPid, EventId, SeatId}, State) ->
    io:format("seat_srv got write_seat ~p ~p from ~p~n", [EventId, SeatId, FromPid]),
    Res = hold_tx(EventId, SeatId),
    FromPid ! {write_seat_reply, {EventId, SeatId}, Res},
    {noreply, State};

handle_info(Other, State) ->
    io:format("seat_srv unknown msg: ~p~n", [Other]),
    {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.

%% free -> held (create if missing)
hold_tx(EventId, SeatId) ->
    Key = {EventId, SeatId},
    Fun = fun() ->
        case mnesia:read(seat, Key) of
            [] ->
                %% create new seat and mark held
                mnesia:write({seat, Key, held, <<"U">>, <<"H">>, 0, undefined}),
                ok;
            [{seat, Key, free, U, H, E, O}] ->
                mnesia:write({seat, Key, held, U, H, E, O}),
                ok;
            [{seat, Key, held, _, _, _, _}] ->
                {error, already_held};
            [{seat, Key, sold, _, _, _, _}] ->
                {error, already_sold}
        end
    end,
    case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        {aborted, Reason} -> {error, {tx_aborted, Reason}}
    end.
