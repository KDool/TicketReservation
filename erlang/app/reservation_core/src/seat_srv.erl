%% erlang/apps/reservation_core/src/seat_srv.erl
-module(seat_srv).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{gateway_pid => undefined}, []).

init(State) ->
    _ = application:start(mnesia),
    io:format("seat_srv started on ~p~n", [node()]),
    %% Start periodic cleanup of expired holds (every 5 seconds)
    erlang:send_after(5000, self(), cleanup_expired_holds),
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

handle_info({register_gateway, GatewayPid}, State) ->
    io:format("seat_srv: gateway registered ~p on node ~p~n", [GatewayPid, node()]),
    {noreply, State#{gateway_pid => GatewayPid}};

handle_info(cleanup_expired_holds, State) ->
    %% Periodic cleanup: transition expired holds back to free
    GatewayPid = maps:get(gateway_pid, State, undefined),
    cleanup_expired_holds_tx(GatewayPid),
    %% Reschedule for 5 seconds later
    erlang:send_after(5000, self(), cleanup_expired_holds),
    {noreply, State};

handle_info({check_seat, FromPid, EventId, SeatId}, State) ->
    io:format("seat_srv got check_seat ~p ~p from ~p~n", [EventId, SeatId, FromPid]),
    Res = check_seat_tx(EventId, SeatId),
    FromPid ! {check_seat_reply, {EventId, SeatId}, Res},
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

%% ========================================================
%% Periodic cleanup: expire held seats if their time has passed
cleanup_expired_holds_tx(GatewayPid) ->
    Now = erlang:system_time(millisecond),
    Fun = fun() ->
        %% Scan all seats and find expired holds
        AllSeats = mnesia:match_object(seat, {seat, {'_', '_'}, held, '_', '_', '_', '_'}, read),
        ExpiredSeats = lists:filter(fun({seat, _, held, _, _, ExpTime, _}) -> ExpTime =< Now end, AllSeats),
        
        %% Collect expired hold info and clean up
        lists:foldl(fun({seat, Key, held, UserId, HoldId, ExpTime, OrderId}, AccHolds) ->
            {EventId, SeatId} = Key,
            
            mnesia:write({seat, Key, free, <<"">>, <<"">>, 0, OrderId}),
            
            %% Remove from user_holds
            UserHoldsKey = {user_holds, UserId},
            case mnesia:read(user_holds, UserHoldsKey) of
                [] -> ok;
                [{user_holds, _, Holds}] ->
                    NewHolds = lists:filter(fun({E, S, _}) -> {E, S} =/= Key end, Holds),
                    case NewHolds of
                        [] -> mnesia:delete(user_holds, UserHoldsKey, write);
                        _ -> mnesia:write({user_holds, UserHoldsKey, NewHolds})
                    end
            end,
            
            io:format("cleanup: expired hold released ~p~n", [Key]),
            [{EventId, SeatId, UserId, HoldId, ExpTime} | AccHolds]
        end, [], ExpiredSeats)
    end,
    case mnesia:transaction(Fun) of
        {atomic, ExpiredHolds} ->
            io:format("cleanup: found ~p expired holds, gateway_pid=~p~n", [length(ExpiredHolds), GatewayPid]),
            %% Notify gateway about each expired hold
            lists:foreach(fun({EventId, SeatId, UserId, HoldId, ExpTime}) ->
                case GatewayPid of
                    undefined -> 
                        io:format("cleanup: skipping notify - no gateway registered~n", []);
                    _ ->
                        io:format("cleanup: sending hold_expired to gateway ~p~n", [GatewayPid]),
                        GatewayPid ! {hold_expired, EventId, SeatId, UserId, HoldId, ExpTime}
                end
            end, ExpiredHolds),
            ok;
        {aborted, Reason} ->
            io:format("cleanup_expired_holds_tx failed: ~p~n", [Reason]),
            ok
    end.

%% ========================================================
%% Query seat state
check_seat_tx(EventId, SeatId) ->
    Key = {EventId, SeatId},
    Fun = fun() ->
        case mnesia:read(seat, Key) of
            [] ->
                {ok, free, <<"">>, <<"">>, 0};
            [{seat, _, State, UserId, HoldId, ExpTime, _}] ->
                {ok, State, UserId, HoldId, ExpTime}
        end
    end,
    case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        {aborted, Reason} -> {error, {tx_aborted, Reason}}
    end.
