# Ticket Reservation Sandbox

Lightweight Erlang + Spring Boot demo for a distributed seat-reservation flow. Erlang nodes manage mnesia, and a Java gateway exposes a simple hold API.

## Prerequisites
- Docker + Docker Compose v2
- Java 21 (for local builds) — Maven wrapper (`mvnw`) is included

## Build & Run with Docker Compose
1. From the repo root:
   ```bash
   cd infra
   docker compose build --up
   ```
   This builds three Erlang nodes (`res1`, `res2`, `res3`) plus the Spring Boot gateway.
2. Stop the stack when done:
   ```bash
   docker compose down -v 
   ```

## Build the Gateway JAR Locally
```bash
cd java/demo

./mvnw clean package

mvn install:install-file \                                                                                          
  -Dfile=OtpErlang-otp26-java21-1.14.jar \
  -DgroupId=org.erlang \
  -DartifactId=jinterface \
  -Dversion=1.14 \
  -Dpackaging=jar

mvn clean package -DskipTests

```
The fat jar lands in `java/demo/target/demo-0.0.1-SNAPSHOT.jar`.

## Test the API
With the stack running (gateway on `localhost:8080`), issue a hold request:
```bash
curl -X POST "http://localhost:8080/hold?eventId=E1&seatId=A1&userId=user1&holdSeconds=5"
```
Expected JSON on success (HTTP 200):
```json
{
  "status": "OK",
  "eventId": "E1",
  "seatId": "A1",
  "userId": "user1",
  "holdId": "uuid-generated-hold-id",
  "expiresAtMillis": 1234567890123,
  "correlationId": "...",
  "reply": "{hold_seat_reply,{\"E1\",\"A1\"},{ok,\"hold-uuid\",1234567890123}}"
}
```
If the Erlang side rejects the write, the gateway returns HTTP 502 with an `error` field (e.g., `seat_already_held`, `user_already_holding_seat`, or `timeout_waiting_reply`).

Check seat status:
```bash
curl "http://localhost:8080/check?eventId=E1&seatId=A1"
```
Response shows current state: `free`, `held`, `expired`, or `sold`.

Confirm a hold (purchase) with a JSON body:
```bash
curl -X POST "http://localhost:8080/reservations/confirm" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user1","holdId":"uuid-from-hold-response"}'
```
Success Response:
- Code: 200 OK
Error Responses:
- 409 Conflict: Hold has already expired or seat is already sold.
- 403 Forbidden: User ID does not match the original holder.
- 404 Not Found: Hold ID is invalid.

## Hold Expiration Flow
1. When a hold is created, the seat is marked with `held` status and an expiration timestamp
2. Every 5 seconds, each Erlang node runs a cleanup task that:
   - Scans for seats with `held` status where `expires_at <= now()`
   - Changes their status to `free`
   - Removes the hold from the user's active holds list
   - Sends a `hold_expired` notification to all registered gateways with metadata (eventId, seatId, userId, holdId)
3. The Java gateway receives these notifications on a persistent mailbox and logs them (ready for Kafka publishing)
4. Users can verify expiration by calling GET /check to see if a seat's status changed from `held` to `free`

## Erlang Core (mnesia + seat server)
- Nodes: three Erlang nodes `res1@res1`, `res2@res2`, `res3@res3` (hostnames come from Docker Compose). They all share the same cookie `ticketcookie`.
- Mnesia bootstrap: only `res1` runs `init_mnesia:bootstrap/3` once to create the schema/table and writes a marker file in its mnesia volume. Followers wait for the marker, then start mnesia and join the cluster.
- Table: `seat` with attributes `[key, state, user_id, hold_id, expires_at, order_id]`, replicated to all nodes (`disc_copies`).
- Entry point: each container starts `seat_srv` via `reservation_core_sup`. The server exposes a message API:
  - Expected message: `{write_seat, FromPid, EventId, SeatId}`
  - Reply: `{write_seat_reply, {EventId, SeatId}, ok | {error, Reason}}`
- The Java gateway (JInterface) sends these messages to the registered process `seat_srv` on the routed Erlang node and relays the reply to HTTP callers.

## Routing & Failover (Gateway)
- Primary routing (by seat prefix):
  - `A/B` -> `res1@res1`
  - `C/D` -> `res2@res2`
  - `E/F` -> `res3@res3`
- Failover order (deterministic, per primary):
  - Primary `res1` -> `res1 -> res2 -> res3`
  - Primary `res2` -> `res2 -> res3 -> res1`
  - Primary `res3` -> `res3 -> res1 -> res2`
- A node is marked down on ping failure or request timeout; it is retried after a short cooldown (10s). When it responds again, routing returns to the primary automatically.

## Quick Mnesia Checks (from host)
Verify `seat_srv` is registered on `res1`:
```bash
docker exec -it res1 bash -lc \
"erl -noshell -noinput -sname chk@res1 -setcookie ticketcookie -eval \
'io:format(\"seat_srv pid = ~p~n\", [rpc:call(res1@res1, erlang, whereis, [seat_srv])]), halt().'"
```
Check the PID and current function:
```bash
docker exec -it res1 bash -lc \
"erl -noshell -noinput -sname chk3@res1 -setcookie ticketcookie -eval \
'Pid = rpc:call(res1@res1, erlang, whereis, [seat_srv]), \
 io:format(\"pid=~p~n\", [Pid]), \
 case Pid of \
   undefined -> io:format(\"seat_srv not running~n\", []); \
   _ -> io:format(\"cur_fun=~p~n\", [rpc:call(res1@res1, erlang, process_info, [Pid, current_function])]) \
 end, halt().'"
```
Dump all `seat` keys/records:
```bash
docker exec -it res1 erl -noshell -noinput -sname dump@res1 -setcookie ticketcookie -eval '
rpc:call(res1@res1, application, start, [mnesia]),
Keys = rpc:call(res1@res1, mnesia, dirty_all_keys, [seat]),
Records = [rpc:call(res1@res1, mnesia, dirty_read, [seat, K]) || K <- Keys],
io:format("keys=~p~nrecords=~p~n", [Keys, Records]),
halt().
'
```
