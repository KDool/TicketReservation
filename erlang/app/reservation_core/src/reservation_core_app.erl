%% erlang/apps/reservation_core/src/reservation_core_app.erl
-module(reservation_core_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    reservation_core_sup:start_link().

stop(_State) ->
    ok.
