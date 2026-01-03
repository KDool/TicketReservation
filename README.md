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

## Test Seat Hold Timeout Workflow

The system implements a full event-driven hold expiration workflow with Kafka notifications:

### Prerequisites
Ensure the stack is running:
```bash
cd infra
docker compose up -d
sleep 15
docker compose ps
```

You should see services running:
- `res1`, `res2`, `res3` (Erlang nodes)
- `gateway` (Spring Boot JInterface client)
- `worker` (Kafka consumer for notifications)
- `kafka`, `zookeeper` (event streaming)

### Step 1: Create a hold with 5-second timeout
```bash
curl -X POST "http://localhost:8080/hold?eventId=E1&seatId=A1&userId=user1&holdSeconds=5"
```

Response (200 OK):
```json
{
  "status": "OK",
  "eventId": "E1",
  "seatId": "A1",
  "userId": "user1",
  "holdId": "0ff81641-68b1-4af7-b20f-9cb9ca30ba19",
  "expiresAtMillis": 1767447209799,
  "correlationId": "b29c6836-c185-42c0-b8c5-36b00b069a45"
}
```

### Step 2: Check hold is active (within 5 seconds)
```bash
curl "http://localhost:8080/hold/check?eventId=E1&seatId=A1"
```

Response (200 OK):
```json
{
  "status": "OK",
  "seatState": "held",
  "userId": "user1",
  "holdId": "0ff81641-68b1-4af7-b20f-9cb9ca30ba19",
  "expiresAtMillis": 1767447209799,
  "isExpired": false
}
```

### Step 3: Wait for expiration
```bash
sleep 6
```

### Step 4: Verify hold has expired
```bash
curl "http://localhost:8080/hold/check?eventId=E1&seatId=A1"
```

Response (200 OK) - seat is now free:
```json
{
  "status": "OK",
  "seatState": "free",
  "userId": "#Bin<0>",
  "holdId": "#Bin<0>",
  "isExpired": true,
  "expiresAtMillis": 0
}
```

### Step 5: Verify Kafka event was published and consumed by Worker
```bash
docker compose logs worker | grep -A 8 "EMAIL NOTIFICATION" | head -20
```

You should see output like:
```
worker  | [EMAIL NOTIFICATION]
worker  | To: user1@example.com
worker  | Subject: Your seat hold has expired
worker  | Body:
worker  |   Hold ID: 0ff81641-68b1-4af7-b20f-9cb9ca30ba19
worker  |   Event: E1
worker  |   Seat: A1
worker  |   Expired at: 1767447209799
worker  |   Action: Please reserve again if interested
worker  | ============================================================
```

### Step 6: Verify user can hold the same seat after expiration
```bash
curl -X POST "http://localhost:8080/hold?eventId=E1&seatId=A1&userId=user2&holdSeconds=30"
```

Response (200 OK) - seat is now free and available for a new user.

### Step 7: Test single-hold-per-user constraint (optional)
Try to hold a different seat while user1 already has an active hold:
```bash
# Create hold for user1, seat A1 (5s)
curl -X POST "http://localhost:8080/hold?eventId=E1&seatId=A1&userId=user1&holdSeconds=5"

# Immediately try to hold another seat (should fail - user1 already has a hold)
curl -X POST "http://localhost:8080/hold?eventId=E2&seatId=A2&userId=user1&holdSeconds=5"
```

The second request will fail because user1 can only hold one seat at a time.

### Event-Driven Workflow Architecture

The complete system implements automatic seat hold expiration with event notifications:

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ POST /hold
       ▼
┌──────────────────┐
│  Spring Gateway  │─────────────┐
│  (JInterface)    │             │
└──────────────────┘             │
       │                         │
       │ RPC {hold_seat, ...}   │
       ▼                         │
┌──────────────────────────────┐ │
│  Erlang Cluster (res1/2/3)  │ │
│  Mnesia DB                   │ │
│  - seat table (distributed)  │ │
│  - user_holds table          │ │
│                              │ │
│  Cleanup Task (every 5s):    │ │
│  - Scan expired holds        │ │
│  - Transition to free state  │ │
│  - Send hold_expired message │ │
└──────────────────────────────┘ │
       │                         │
       │ {hold_expired, ...}     │
       └────────────────────────┬┘
                                │
                                ▼
                          ┌─────────────┐
                          │   Kafka     │
                          │ Topic:      │
                          │ seat-hold   │
                          │ -expired    │
                          └─────────────┘
                                │
                                ▼
                          ┌──────────────────┐
                          │ Worker Service   │
                          │ @KafkaListener   │
                          │ Notification     │
                          │ (email, SMS)     │
                          └──────────────────┘
```

### Workflow Summary
1. ✅ **Gateway** receives hold request with expiry duration (in seconds)
2. ✅ **Erlang** stores hold in Mnesia with expiry timestamp, enforces single-hold-per-user constraint
3. ✅ **Cleanup Task** runs every 5 seconds, detects expired holds
4. ✅ **Seat State** automatically transitions from `held` → `free`
5. ✅ **Hold Expired Event** sent via mailbox to Gateway
6. ✅ **Kafka Event** published to `seat-hold-expired` topic with full notification payload
7. ✅ **Worker Service** consumes Kafka event, processes notification (email simulation in logs)
8. ✅ **User** can immediately reserve the same seat again
9. ✅ **Constraint** enforced: user can only hold one seat at a time across the event

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
