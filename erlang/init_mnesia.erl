%% erlang/init_mnesia.erl (correct: create_schema ONCE for all nodes)
-module(init_mnesia).
-export([bootstrap/3]).

bootstrap(Nodes, MarkerPath, _SeatTable) ->
    io:format("Bootstrap nodes: ~p~n", [Nodes]),
    ok = wait_pong_all(Nodes, 60),

    %% 1) Stop mnesia everywhere (ok if not started)
    lists:foreach(fun(N) ->
        R = rpc:call(N, application, stop, [mnesia]),
        io:format("stop mnesia on ~p => ~p~n", [N, R]),
        ok
    end, Nodes),
    timer:sleep(500),

    %% 2) Create distributed schema ONCE (from this node)
    CS = mnesia:create_schema(Nodes),
    io:format("create_schema(~p) => ~p~n", [Nodes, CS]),
    case CS of
        ok -> ok;
        {error, {_, {already_exists, _}}} -> ok;
        {error, Reason} -> throw({schema_create_failed, Reason});
        Other -> throw({schema_create_failed, Other})
    end,

    %% 3) Start mnesia on coordinator (this node = res1@res1)
    R1 = application:start(mnesia),
    io:format("start mnesia local => ~p~n", [R1]),
    case R1 of
        ok -> ok;
        {error, {already_started, mnesia}} -> ok;
        Other1 -> throw({mnesia_start_failed, node(), Other1})
    end,

    %% 4) Tell other nodes to join via extra_db_nodes=[res1@res1], then start mnesia
    [Seed | _] = Nodes,
    lists:foreach(fun(N) ->
        case N =:= Seed of
            true -> ok;
            false ->
                _ = rpc:call(N, application, set_env, [mnesia, extra_db_nodes, [Seed]]),
                R = rpc:call(N, application, start, [mnesia]),
                io:format("start mnesia on ~p => ~p~n", [N, R]),
                case R of
                    ok -> ok;
                    {error, {already_started, mnesia}} -> ok;
                    Other2 -> throw({mnesia_start_failed, N, Other2})
                end
        end
    end, Nodes),

    %% 5) Wait cluster formed
    ok = wait_running_db_nodes(Nodes, 60),

    %% 6) Create replicated tables
    Attrs = [key, state, user_id, hold_id, expires_at, order_id],
    Def = [{attributes, Attrs}, {type, set}, {disc_copies, Nodes}],
    CT = mnesia:create_table(seat, Def),
    io:format("create_table(seat) => ~p~n", [CT]),
    case CT of
        {atomic, ok} -> ok;
        {aborted, {already_exists, seat}} -> ok;
        Other3 -> throw({create_table_failed, Other3})
    end,

    %% Create user_holds index table: {user_holds, {user_holds, UserId}, [{EventId, SeatId, ExpiresAt}, ...]}
    UserAttrs = [key, holds],
    UserDef = [{attributes, UserAttrs}, {type, set}, {disc_copies, Nodes}],
    CTU = mnesia:create_table(user_holds, UserDef),
    io:format("create_table(user_holds) => ~p~n", [CTU]),
    case CTU of
        {atomic, ok} -> ok;
        {aborted, {already_exists, user_holds}} -> ok;
        Other4 -> throw({create_table_failed, Other4})
    end,

    %% 7) Wait table everywhere
    lists:foreach(fun(N) ->
        R = rpc:call(N, mnesia, wait_for_tables, [[seat, user_holds], 30000]),
        io:format("wait_for_tables(~p) on ~p => ~p~n", [[seat, user_holds], N, R]),
        case R of
            ok -> ok;
            Other5 -> throw({table_not_ready, N, Other5})
        end
    end, Nodes),

    ok = file:write_file(MarkerPath, <<"ok">>),
    io:format("Bootstrap done. Marker written: ~s~n", [MarkerPath]),
    ok.

%% ---- waits ----
wait_pong_all(Nodes, Seconds) ->
    Deadline = erlang:monotonic_time(second) + Seconds,
    wait_pong_all_loop(Nodes, Deadline).

wait_pong_all_loop(Nodes, Deadline) ->
    Now = erlang:monotonic_time(second),
    case lists:all(fun(N) -> net_adm:ping(N) =:= pong end, Nodes) of
        true -> ok;
        false when Now > Deadline -> throw({ping_timeout, Nodes});
        false -> timer:sleep(1000), wait_pong_all_loop(Nodes, Deadline)
    end.

wait_running_db_nodes(Expected, Seconds) ->
    Deadline = erlang:monotonic_time(second) + Seconds,
    wait_running_db_nodes_loop(lists:sort(Expected), Deadline).

wait_running_db_nodes_loop(Exp, Deadline) ->
    Now = erlang:monotonic_time(second),
    Running = lists:sort(mnesia:system_info(running_db_nodes)),
    io:format("waiting running_db_nodes current=~p expected=~p~n", [Running, Exp]),
    case Running =:= Exp of
        true -> ok;
        false when Now > Deadline -> throw({running_db_nodes_timeout, Running, Exp});
        false -> timer:sleep(1000), wait_running_db_nodes_loop(Exp, Deadline)
    end.
