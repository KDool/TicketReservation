#!/usr/bin/env bash
set -euo pipefail

COOKIE="${ERLANG_COOKIE:-ticketcookie}"
SNAME="${ERL_SNAME:-res1}"

MNESIA_DIR="${MNESIA_DIR:-/var/lib/mnesia}"
INIT_MNESIA="${INIT_MNESIA:-false}"

DIST_MIN="${DIST_MIN:-9100}"
DIST_MAX="${DIST_MAX:-9105}"

mkdir -p "${MNESIA_DIR}"
epmd -daemon

erlc -o /app /app/init_mnesia.erl

SCHEMA_MARKER="${MNESIA_DIR}/.schema_initialized"

echo "[START] Node ${SNAME} (infra mode, no business logic)"

exec erl -noshell -noinput \
  -sname "${SNAME}" \
  -setcookie "${COOKIE}" \
  -mnesia dir "\"${MNESIA_DIR}\"" \
  -kernel inet_dist_listen_min "${DIST_MIN}" inet_dist_listen_max "${DIST_MAX}" \
  -pa /app \
  -eval "
    io:format(\"Node up: ~p~n\", [node()]),
    Nodes=[res1@res1,res2@res2,res3@res3],
    Marker=\"${SCHEMA_MARKER}\",

    case \"${INIT_MNESIA}\" of
      \"true\" ->
        case filelib:is_file(Marker) of
          true ->
            io:format(\"[BOOTSTRAP] skipped (marker exists)~n\", []);
          false ->
            io:format(\"[BOOTSTRAP] running on ~p~n\", [node()]),
            Res = (catch init_mnesia:bootstrap(Nodes, Marker, seat)),
            io:format(\"[BOOTSTRAP] result=~p~n\", [Res])
        end;
      \"false\" ->
        io:format(\"[FOLLOWER] waiting for marker ~s~n\", [Marker]),
        WaitFun = fun F() ->
          case filelib:is_file(Marker) of
            true -> ok;
            false -> timer:sleep(1000), F()
          end
        end,
        WaitFun(),
        io:format(\"[FOLLOWER] marker found~n\", [])
    end,

    %% Start mnesia locally (after bootstrap or after marker)
    application:start(mnesia),

    %% Start OTP core (no rpc/start_link across temporary node!)
    code:add_patha(\"/app\"),
    code:load_file(reservation_core_sup),
    code:load_file(seat_srv),
    reservation_core_sup:start_link(),

    timer:sleep(infinity).
  "
