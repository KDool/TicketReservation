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
%% Expected message (gateway):
%%   {hold_seat, FromPid, EventId, SeatId, UserId, HoldId, ExpiresAtMs}
%%
%% Reply:
%%   {hold_seat_reply, {EventId, SeatId},
%%     {ok, HoldId, ExpiresAtMs} | {error, Reason}}
%%
%% Legacy support (tests / old clients):
%%   {write_seat, FromPid, EventId, SeatId}
%%   -> {write_seat_reply, {EventId, SeatId}, ok | {error, Reason}}
%%   (mapped internally to a short-lived hold)
handle_info({write_seat, FromPid, EventId, SeatId}, State) ->
    io:format("seat_srv got legacy write_seat ~p ~p from ~p~n", [EventId, SeatId, FromPid]),
    Now = erlang:system_time(millisecond),
    Res = hold_tx(EventId, SeatId, <<"legacy_user">>, <<"legacy_hold">>, Now + 5000),
    LegacyReply = case Res of
        {ok, _, _} -> ok;
        {error, Reason} -> {error, Reason};
        {error, Reason, _, _} -> {error, Reason}
    end,
    FromPid ! {write_seat_reply, {EventId, SeatId}, LegacyReply},
    {noreply, State};

handle_info({hold_seat, FromPid, EventId, SeatId, UserId, HoldId, ExpiresAtMs}, State) ->
    io:format("seat_srv got hold_seat ~p ~p for user ~p exp=~p from ~p~n",
              [EventId, SeatId, UserId, ExpiresAtMs, FromPid]),
    Res = hold_tx(EventId, SeatId, UserId, HoldId, ExpiresAtMs),
    FromPid ! {hold_seat_reply, {EventId, SeatId}, Res},
    {noreply, State};

handle_info(Other, State) ->
    io:format("seat_srv unknown msg: ~p~n", [Other]),
    {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.

%% free/expired -> held (create if missing)
%% Also enforces: user can hold only one seat at a time
hold_tx(EventId, SeatId, UserId, HoldId, ExpiresAtMs) ->
    Key = {EventId, SeatId},
    Now = erlang:system_time(millisecond),
    Fun = fun() ->
        %% Step 1: Check if user already has an active hold
        UserHoldsKey = {user_holds, UserId},
        UserHoldsList = case mnesia:read(user_holds, UserHoldsKey) of
            [] -> [];
            [{user_holds, _, Holds}] -> Holds
        end,
        
        %% Filter out expired holds and check for active hold
        ActiveHolds = lists:filter(fun({_, _, Exp}) -> Exp > Now end, UserHoldsList),
        
        case ActiveHolds of
            [] ->
                %% No active hold for this user, proceed with seat hold
                case mnesia:read(seat, Key) of
                    [] ->
                        %% New seat
                        mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, undefined}),
                        mnesia:write({user_holds, UserHoldsKey, [{EventId, SeatId, ExpiresAtMs} | UserHoldsList]}),
                        {ok, HoldId, ExpiresAtMs};
                    [{seat, Key, free, _, _, _, OrderId}] ->
                        mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, OrderId}),
                        mnesia:write({user_holds, UserHoldsKey, [{EventId, SeatId, ExpiresAtMs} | UserHoldsList]}),
                        {ok, HoldId, ExpiresAtMs};
                    [{seat, Key, held, _, _, CurExp, OrderId}] when CurExp =< Now ->
                        %% Seat hold expired, reclaim it
                        mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, OrderId}),
                        mnesia:write({user_holds, UserHoldsKey, [{EventId, SeatId, ExpiresAtMs} | UserHoldsList]}),
                        {ok, HoldId, ExpiresAtMs};
                    [{seat, Key, held, _, CurHoldId, CurExp, _}] ->
                        {error, {seat_already_held, CurHoldId, CurExp}};
                    [{seat, Key, sold, _, _, _, _}] ->
                        {error, seat_already_sold}
                end;
            [ActiveHold | _] ->
                %% User already has an active hold
                {ExpEventId, ExpSeatId, ExpExp} = ActiveHold,
                {error, {user_already_holding_seat, ExpEventId, ExpSeatId, ExpExp}}
        end
    end,
    case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        {aborted, Reason} -> {error, {tx_aborted, Reason}}
    end.
