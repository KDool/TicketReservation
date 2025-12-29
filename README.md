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
curl -X POST "http://localhost:8080/hold?eventId=E1&seatId=A1&userId=user1"
```
Expected JSON on success (HTTP 200):
```json
{
  "status": "OK",
  "eventId": "E1",
  "seatId": "A1",
  "userId": "user1",
  "correlationId": "...",
  "reply": "{write_seat_reply,{\"E1\",\"A1\"},ok}"
}
```
If the Erlang side rejects the write, the gateway returns HTTP 502 with an `error` field (e.g., `already_held` or `timeout_waiting_reply`).

## Erlang Core (mnesia + seat server)
- Nodes: three Erlang nodes `res1@res1`, `res2@res2`, `res3@res3` (hostnames come from Docker Compose). They all share the same cookie `ticketcookie`.
- Mnesia bootstrap: only `res1` runs `init_mnesia:bootstrap/3` once to create the schema/table and writes a marker file in its mnesia volume. Followers wait for the marker, then start mnesia and join the cluster.
- Table: `seat` with attributes `[key, state, user_id, hold_id, expires_at, order_id]`, replicated to all nodes (`disc_copies`).
- Entry point: each container starts `seat_srv` via `reservation_core_sup`. The server exposes a message API:
  - Expected message: `{write_seat, FromPid, EventId, SeatId}`
  - Reply: `{write_seat_reply, {EventId, SeatId}, ok | {error, Reason}}`
- The Java gateway (JInterface) sends these messages to the registered process `seat_srv` on `res1@res1` and relays the reply to HTTP callers.
