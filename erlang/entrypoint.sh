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
        Seed = res1@res1,
        io:format(\"[FOLLOWER] waiting for seed ~p~n\", [Seed]),
        WaitSeed = fun F() ->
          case net_adm:ping(Seed) of
            pong -> ok;
            pang -> timer:sleep(1000), F()
          end
        end,
        WaitSeed(),
        ok = application:set_env(mnesia, extra_db_nodes, [Seed])
    end,

    %% Start mnesia locally (after bootstrap or after marker)
    StartMnesia = fun F() ->
      case application:start(mnesia) of
        ok -> ok;
        {error, {already_started, mnesia}} -> ok;
        _ -> timer:sleep(1000), F()
      end
    end,
    StartMnesia(),
    WaitTable = fun F() ->
      case mnesia:wait_for_tables([seat], 30000) of
        ok -> ok;
        _ -> timer:sleep(1000), F()
      end
    end,
    WaitTable(),

    %% Start OTP core (no rpc/start_link across temporary node!)
    code:add_patha(\"/app\"),
    code:load_file(reservation_core_sup),
    code:load_file(seat_srv),
    reservation_core_sup:start_link(),

    timer:sleep(infinity).
  "
