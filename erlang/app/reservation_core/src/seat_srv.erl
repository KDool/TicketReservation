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
    %% Start periodic cleanup of expired holds
    erlang:send_after(5000, self(), cleanup_expired_holds),
    {ok, State#{gateway_pids => []}}.

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
%%   {confirm_hold, FromPid, UserId, HoldId}
%%   {list_event_seats, FromPid, EventId}
%%
%% Reply:
%%   {hold_seat_reply, {EventId, SeatId},
%%     {ok, HoldId, ExpiresAtMs} | {error, Reason}}
%%   {confirm_hold_reply, {EventId, SeatId},
%%     {ok, OrderId} | {error, Reason}}
%%   {list_event_seats_reply, EventId,
%%     {ok, [Map]} | {error, Reason}}
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
    Now = erlang:system_time(millisecond),
    io:format("[CONCURRENCY] seat_srv got hold_seat ~p ~p for user ~p (holdId=~p) at ~p from ~p~n",
              [EventId, SeatId, UserId, HoldId, Now, FromPid]),
    Res = hold_tx(EventId, SeatId, UserId, HoldId, ExpiresAtMs),
    io:format("[CONCURRENCY] seat_srv hold_seat result for ~p ~p user=~p: ~p~n",
              [EventId, SeatId, UserId, Res]),
    FromPid ! {hold_seat_reply, {EventId, SeatId}, Res},
    {noreply, State};

handle_info({confirm_hold, FromPid, UserId, HoldId}, State) ->
    io:format("seat_srv got confirm_hold holdId=~p user=~p from ~p~n",
              [HoldId, UserId, FromPid]),
    {ReplyKey, Res} = confirm_tx(UserId, HoldId),
    FromPid ! {confirm_hold_reply, ReplyKey, Res},
    {noreply, State};

handle_info({check_seat, FromPid, EventId, SeatId}, State) ->
    io:format("seat_srv got check_seat ~p ~p from ~p~n", [EventId, SeatId, FromPid]),
    Res = check_seat_tx(EventId, SeatId),
    FromPid ! {check_seat_reply, {EventId, SeatId}, Res},
    {noreply, State};

handle_info({list_event_seats, FromPid, EventId}, State) ->
    io:format("seat_srv got list_event_seats ~p from ~p~n", [EventId, FromPid]),
    Res = list_event_seats_tx(EventId),
    FromPid ! {list_event_seats_reply, EventId, Res},
    {noreply, State};

handle_info({register_gateway, GatewayPid}, State) ->
    io:format("seat_srv registering gateway pid ~p~n", [GatewayPid]),
    Pids = maps:get(gateway_pids, State, []),
    {noreply, State#{gateway_pids => [GatewayPid | Pids]}};

handle_info(cleanup_expired_holds, State) ->
    GatewayPids = maps:get(gateway_pids, State, []),
    cleanup_expired_holds_tx(GatewayPids),
    %% Schedule next cleanup in 5 seconds
    erlang:send_after(5000, self(), cleanup_expired_holds),
    {noreply, State};

handle_info(Other, State) ->
    io:format("seat_srv unknown msg: ~p~n", [Other]),
    {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.

%% free/expired -> held (create if missing)
%% Also enforces: user can hold only one seat at a time
%% Transaction ensures serialization for concurrent requests
hold_tx(EventId, SeatId, UserId, HoldId, ExpiresAtMs) ->
    Key = {EventId, SeatId},
    Now = erlang:system_time(millisecond),
    io:format("[CONCURRENCY] Starting hold_tx for ~p ~p user=~p at ~p~n", 
              [EventId, SeatId, UserId, Now]),
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
                SeatRecord = mnesia:read(seat, Key),
                io:format("[CONCURRENCY] Read seat ~p state: ~p~n", [Key, SeatRecord]),
                case SeatRecord of
                    [] ->
                        %% New seat
                        io:format("[CONCURRENCY] Creating new seat ~p for user ~p~n", [Key, UserId]),
                        mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, undefined}),
                        mnesia:write({user_holds, UserHoldsKey, [{EventId, SeatId, ExpiresAtMs} | UserHoldsList]}),
                        {ok, HoldId, ExpiresAtMs};
                    [{seat, Key, free, _, _, _, OrderId}] ->
                        io:format("[CONCURRENCY] Claiming free seat ~p for user ~p~n", [Key, UserId]),
                        mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, OrderId}),
                        mnesia:write({user_holds, UserHoldsKey, [{EventId, SeatId, ExpiresAtMs} | UserHoldsList]}),
                        {ok, HoldId, ExpiresAtMs};
                    [{seat, Key, held, HeldUserId, _, CurExp, OrderId}] when CurExp =< Now ->
                        %% Seat hold expired, reclaim it
                        io:format("[CONCURRENCY] Reclaiming expired seat ~p (was held by ~p) for user ~p~n", 
                                  [Key, HeldUserId, UserId]),
                        mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, OrderId}),
                        mnesia:write({user_holds, UserHoldsKey, [{EventId, SeatId, ExpiresAtMs} | UserHoldsList]}),
                        {ok, HoldId, ExpiresAtMs};
                    [{seat, Key, held, HeldUserId, CurHoldId, CurExp, _}] ->
                        io:format("[CONCURRENCY] CONFLICT: Seat ~p already held by ~p (holdId=~p, exp=~p), rejecting user ~p~n",
                                  [Key, HeldUserId, CurHoldId, CurExp, UserId]),
                        {error, {seat_already_held, CurHoldId, CurExp}};
                    [{seat, Key, sold, SoldUserId, _, _, _}] ->
                        io:format("[CONCURRENCY] CONFLICT: Seat ~p already sold to ~p, rejecting user ~p~n",
                                  [Key, SoldUserId, UserId]),
                        {error, seat_already_sold}
                end;
            [ActiveHold | _] ->
                %% User already has an active hold
                {ExpEventId, ExpSeatId, ExpExp} = ActiveHold,
                io:format("[CONCURRENCY] User ~p already holding seat ~p:~p (exp=~p), rejecting new hold ~p:~p~n",
                          [UserId, ExpEventId, ExpSeatId, ExpExp, EventId, SeatId]),
                {error, {user_already_holding_seat, ExpEventId, ExpSeatId, ExpExp}}
        end
    end,
    Result = case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        {aborted, Reason} -> {error, {tx_aborted, Reason}}
    end,
    io:format("[CONCURRENCY] Completed hold_tx for ~p ~p user=~p result=~p~n",
              [EventId, SeatId, UserId, Result]),
    Result.

confirm_tx(UserId, HoldId) ->
    Now = erlang:system_time(millisecond),
    Fun = fun() ->
        case find_seat_by_hold_id(HoldId) of
            not_found ->
                {{undefined, undefined}, {error, hold_not_found}};
            {seat, Key = {EventId, SeatId}, State, SeatUserId, HoldId, ExpiresAt, _OrderId} ->
                case State of
                    sold ->
                        {{EventId, SeatId}, {error, seat_already_sold}};
                    held ->
                        case SeatUserId =:= UserId of
                            false ->
                                {{EventId, SeatId}, {error, user_mismatch}};
                            true when ExpiresAt =< Now ->
                                {{EventId, SeatId}, {error, hold_expired}};
                            true ->
                                OrderId = make_order_id(),
                                mnesia:write({seat, Key, sold, UserId, HoldId, ExpiresAt, OrderId}),
                                remove_user_hold(UserId, EventId, SeatId),
                                {{EventId, SeatId}, {ok, OrderId}}
                        end;
                    free ->
                        {{EventId, SeatId}, {error, hold_expired}}
                end
        end
    end,
    case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        {aborted, Reason} -> {{undefined, undefined}, {error, {tx_aborted, Reason}}}
    end.

find_seat_by_hold_id(HoldId) ->
    Match = [{{seat, '$1', '$2', '$3', HoldId, '$5', '$6'}, [], ['$_']}],
    case mnesia:select(seat, Match) of
        [] -> not_found;
        [Seat | _] -> Seat
    end.

remove_user_hold(UserId, EventId, SeatId) ->
    UserKey = {user_holds, UserId},
    case mnesia:read(user_holds, UserKey) of
        [] -> ok;
        [{user_holds, UserKey, Holds}] ->
            Filtered = [H || H = {E, S, _Exp} <- Holds, not (E =:= EventId andalso S =:= SeatId)],
            mnesia:write({user_holds, UserKey, Filtered}),
            ok
    end.

make_order_id() ->
    OrderNum = erlang:unique_integer([monotonic, positive]),
    lists:concat(["order_", integer_to_list(OrderNum)]).

cleanup_expired_holds_tx(GatewayPids) ->
    Now = erlang:system_time(millisecond),
    Fun = fun() ->
        %% Find all held seats that have expired
        AllSeats = mnesia:match_object({seat, '_', held, '_', '_', '_', '_'}),
        ExpiredSeats = [S || {seat, _, held, _, _, ExpiresAt, _} = S <- AllSeats, ExpiresAt =< Now],
        
        %% Free each expired seat and notify gateways
        lists:foreach(fun({seat, Key = {EventId, SeatId}, held, UserId, HoldId, ExpiresAt, OrderId}) ->
            io:format("seat_srv freeing expired hold: ~p ~p holdId=~p~n", [EventId, SeatId, HoldId]),
            mnesia:write({seat, Key, free, UserId, HoldId, ExpiresAt, OrderId}),
            remove_user_hold(UserId, EventId, SeatId),
            
            %% Notify all registered gateways
            Metadata = #{
                event_id => EventId,
                seat_id => SeatId,
                user_id => UserId,
                hold_id => HoldId,
                expired_at => ExpiresAt
            },
            lists:foreach(fun(GwPid) ->
                GwPid ! {hold_expired, Metadata}
            end, GatewayPids)
        end, ExpiredSeats),
        
        length(ExpiredSeats)
    end,
    case mnesia:transaction(Fun) of
        {atomic, Count} when Count > 0 ->
            io:format("seat_srv cleaned up ~p expired holds~n", [Count]),
            ok;
        {atomic, 0} ->
            ok;
        {aborted, Reason} ->
            io:format("seat_srv cleanup failed: ~p~n", [Reason]),
            error
    end.

check_seat_tx(EventId, SeatId) ->
    Key = {EventId, SeatId},
    Now = erlang:system_time(millisecond),
    Fun = fun() ->
        case mnesia:read(seat, Key) of
            [] ->
                {ok, #{status => free, event_id => EventId, seat_id => SeatId}};
            [{seat, Key, Status, UserId, HoldId, ExpiresAt, OrderId}] ->
                State = case Status of
                    free -> free;
                    held when ExpiresAt =< Now -> expired;
                    held -> held;
                    sold -> sold
                end,
                {ok, #{
                    status => State,
                    event_id => EventId,
                    seat_id => SeatId,
                    user_id => UserId,
                    hold_id => HoldId,
                    expires_at => ExpiresAt,
                    order_id => OrderId
                }}
        end
    end,
    case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        {aborted, Reason} -> {error, {tx_aborted, Reason}}
    end.

list_event_seats_tx(EventId) ->
    Now = erlang:system_time(millisecond),
    Fun = fun() ->
        Seats = mnesia:match_object({seat, {EventId, '_'}, '_', '_', '_', '_', '_'}),
        lists:map(fun({seat, {EvId, SeatId}, Status, UserId, HoldId, ExpiresAt, OrderId}) ->
            State = case Status of
                free -> free;
                held when ExpiresAt =< Now -> expired;
                held -> held;
                sold -> sold
            end,
            #{
                status => State,
                event_id => EvId,
                seat_id => SeatId,
                user_id => UserId,
                hold_id => HoldId,
                expires_at => ExpiresAt,
                order_id => OrderId
            }
        end, Seats)
    end,
    case mnesia:transaction(Fun) of
        {atomic, R} -> {ok, R};
        {aborted, Reason} -> {error, {tx_aborted, Reason}}
    end.
