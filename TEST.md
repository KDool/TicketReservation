# Ticket Reservation System - Testing Guide

This guide provides step-by-step instructions for testing the Ticket Reservation System. All commands are ready to copy and paste.

---

## Prerequisites

- Docker and Docker Compose installed
- `curl` and `jq` for API testing (jq is optional but recommended for formatted output)

---

## 1. Docker Setup

### Start the System

```bash
cd infra
docker compose up -d --build
```

Wait 15-20 seconds for all services to initialize.

### Check Service Status

```bash
docker compose ps
```

Expected output: All services should be in "running" state:
- res1, res2, res3 (Erlang nodes)
- gateway (Spring Boot)
- kafka, zookeeper
- worker, chat-consumer

### View Logs

```bash
# Gateway logs
docker logs gateway -f

# Erlang node logs (res1)
docker logs res1 -f

# Worker logs
docker logs worker -f
```

### Stop the System

```bash
docker compose down
```

### Stop and Remove All Data (Clean Restart)

```bash
docker compose down -v
```

---

## 2. Basic Seat Hold Tests

### Test 1: Create a Hold for Seat A1 (Routes to res1)

```bash
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E1","seatId":"A1","userId":"user1","holdSeconds":600}' | jq '.'
```

**Expected Response:**
```json
{
  "status": "OK",
  "eventId": "E1",
  "seatId": "A1",
  "userId": "user1",
  "holdId": "uuid-here",
  "expiresAtMillis": 1234567890,
  "correlationId": "uuid-here"
}
```

### Test 2: Create a Hold for Seat C5 (Routes to res2)

```bash
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E2","seatId":"C5","userId":"user2","holdSeconds":600}' | jq '.'
```

### Test 3: Create a Hold for Seat E9 (Routes to res3)

```bash
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E3","seatId":"E9","userId":"user3","holdSeconds":600}' | jq '.'
```

---

## 3. Check Seat Status

### Test 4: Check Seat A1 Status

```bash
curl -s "http://localhost:8080/check?eventId=E1&seatId=A1" | jq '.'
```

**Expected Response (if held):**
```json
{
  "status": "OK",
  "seatStatus": "held",
  "eventId": "E1",
  "seatId": "A1",
  "userId": "user1",
  "holdId": "uuid-here",
  "expiresAtMillis": 1234567890,
  "orderId": ""
}
```

### Test 5: Check a Free Seat

```bash
curl -s "http://localhost:8080/check?eventId=E99&seatId=A99" | jq '.'
```

**Expected Response:**
```json
{
  "status": "OK",
  "seatStatus": "free",
  "eventId": "E99",
  "seatId": "A99",
  "userId": "",
  "holdId": "",
  "expiresAtMillis": 0,
  "orderId": ""
}
```

---

## 4. Confirm Hold (Purchase)

### Test 6: Confirm a Hold

**Step 1:** Create a hold and save the holdId
```bash
RESPONSE=$(curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E10","seatId":"B10","userId":"buyer1","holdSeconds":600}')
echo $RESPONSE | jq '.'
HOLD_ID=$(echo $RESPONSE | jq -r '.holdId')
echo "Hold ID: $HOLD_ID"
```

**Step 2:** Confirm the hold
```bash
curl -s -X POST "http://localhost:8080/reservations/confirm" \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"buyer1\",\"holdId\":\"$HOLD_ID\"}" | jq '.'
```

**Expected Response:**
```json
{
  "status": "success",
  "data": {
    "orderId": "order_123"
  },
  "code": 200
}
```

**Step 3:** Verify seat is now sold
```bash
curl -s "http://localhost:8080/check?eventId=E10&seatId=B10" | jq '.'
```

**Expected:** `seatStatus` should be `"sold"`

---

## 5. Error Handling Tests

### Test 7: Try to Hold an Already Held Seat

```bash
# First hold
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E20","seatId":"D20","userId":"user1","holdSeconds":600}' | jq '.'

# Second hold (should fail)
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E20","seatId":"D20","userId":"user2","holdSeconds":600}' | jq '.'
```

**Expected Response (second request):**
```json
{
  "status": "FAILED",
  "error": "{seat_already_held, ...}"
}
```

### Test 8: Try to Hold an Already Sold Seat

```bash
# Create and confirm a hold
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E21","seatId":"F21","userId":"buyer1","holdSeconds":600}' > /tmp/hold.json

HOLD_ID=$(cat /tmp/hold.json | jq -r '.holdId')

curl -s -X POST "http://localhost:8080/reservations/confirm" \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"buyer1\",\"holdId\":\"$HOLD_ID\"}" | jq '.'

# Try to hold the sold seat (should fail)
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E21","seatId":"F21","userId":"user2","holdSeconds":600}' | jq '.'
```

**Expected Response:**
```json
{
  "status": "FAILED",
  "error": "seat_already_sold"
}
```

### Test 9: User Tries to Hold Multiple Seats Simultaneously

```bash
# First hold
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E30","seatId":"A30","userId":"greedy_user","holdSeconds":600}' | jq '.'

# Try to hold another seat (should fail)
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E30","seatId":"A31","userId":"greedy_user","holdSeconds":600}' | jq '.'
```

**Expected Response (second request):**
```json
{
  "status": "FAILED",
  "error": "user_already_holding_seat:E30:A30"
}
```

---

## 6. Hold Expiration Tests

### Test 10: Verify Hold Expires After Duration

```bash
# Create a hold with 10 second expiration
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E40","seatId":"B40","userId":"shorthold","holdSeconds":10}' | jq '.'

# Immediately check status (should be "held")
curl -s "http://localhost:8080/check?eventId=E40&seatId=B40" | jq '.seatStatus'

# Wait 12 seconds
sleep 12

# Check again (should be "free")
curl -s "http://localhost:8080/check?eventId=E40&seatId=B40" | jq '.seatStatus'
```

**Expected:**
- First check: `"held"`
- After expiration: `"free"`

---

## 7. Concurrency Tests

### Test 11: Concurrent Holds on Same Seat (Automated Script)

```bash
# Run the automated concurrency test
./test-concurrency.sh
```

**Expected Output:**
```
✅ PASS: UserA succeeded, UserB failed (as expected)
   UserA got the seat, UserB received conflict error
```

### Test 12: Manual Concurrent Test

Open two terminal windows and execute these commands **simultaneously** (within 1 second):

**Terminal 1:**
```bash
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E50","seatId":"C50","userId":"racer1","holdSeconds":600}' | jq '.'
```

**Terminal 2:**
```bash
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E50","seatId":"C50","userId":"racer2","holdSeconds":600}' | jq '.'
```

**Expected:** One should succeed with `"status": "OK"`, the other should fail with `"status": "FAILED"`

---

## 8. Load Testing (Multiple Seats)

### Test 13: Create Multiple Holds Rapidly

```bash
for i in {1..10}; do
  curl -s -X POST "http://localhost:8080/hold" \
    -H "Content-Type: application/json" \
    -d "{\"eventId\":\"E60\",\"seatId\":\"A$i\",\"userId\":\"user$i\",\"holdSeconds\":600}" | jq -c '{seatId, status, userId}'
done
```

**Expected:** All 10 requests should succeed since they're for different seats.

---

## 9. Distributed System Tests

### Test 14: Verify Routing to Different Nodes

```bash
# Watch gateway logs to see routing
docker logs gateway -f &

# Make requests to different seat prefixes
curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"E70","seatId":"A70","userId":"user70","holdSeconds":600}' | jq -c '.'

curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"E71","seatId":"C71","userId":"user71","holdSeconds":600}' | jq -c '.'

curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"E72","seatId":"E72","userId":"user72","holdSeconds":600}' | jq -c '.'
```

**Expected in logs:**
```
[JIF] route seatId=A70 -> res1@res1
[JIF] route seatId=C71 -> res2@res2
[JIF] route seatId=E72 -> res3@res3
```

### Test 15: Check All Erlang Nodes are Connected

```bash
docker exec res1 erl -sname test_client -setcookie ticketcookie -eval "net_adm:ping('res2@res2'), net_adm:ping('res3@res3'), init:stop()." -noshell
```

**Expected:** `pong pong` (both nodes respond)

---

## 10. Kafka Event Testing

### Test 16: Verify Hold Expiration Events Published to Kafka

```bash
# Create a hold with short expiration
curl -s -X POST "http://localhost:8080/hold" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"E80","seatId":"F80","userId":"kafka_test","holdSeconds":8}' | jq '.'

# Wait for expiration + cleanup cycle
sleep 10

# Check gateway logs for Kafka publish
docker logs gateway 2>&1 | grep -i "hold_expired.*kafka_test"

# Check worker logs for consumption
docker logs worker 2>&1 | grep -i "kafka_test"
```

**Expected:** Should see messages about hold_expired event being published and consumed.

---

## 11. Seat Routing Reference

| Seat Prefix | Target Node | Example Seats |
|-------------|-------------|---------------|
| A, B        | res1@res1   | A1, A10, B5   |
| C, D        | res2@res2   | C1, C10, D5   |
| E, F        | res3@res3   | E1, E10, F5   |

---

## 12. Quick Health Check

Run this to verify the system is working:

```bash
echo "=== System Health Check ===" && \
curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"HEALTH","seatId":"A1","userId":"healthcheck","holdSeconds":5}' | jq -r '.status' && \
echo "✅ System is working!" || echo "❌ System has issues"
```

---

## 13. Cleanup Between Tests

### Clear All Holds (Let them expire)

If you need to reset the system state:

```bash
# Stop all services
cd infra
docker compose down -v

# Start fresh
docker compose up -d --build

# Wait for initialization
sleep 15
```

---

## Troubleshooting

### Services Won't Start
```bash
# Check if ports are already in use
lsof -i :8080
lsof -i :9092

# View detailed error logs
docker logs gateway
docker logs res1
```

### Erlang Nodes Not Connected
```bash
# Check node status
docker exec res1 epmd -names
docker exec res2 epmd -names
docker exec res3 epmd -names
```

### Kafka Issues
```bash
# Check Kafka is running
docker logs kafka

# Check Zookeeper
docker logs zookeeper
```

---

## Additional Notes

- All timestamps are in milliseconds
- Hold durations are in seconds
- UUIDs are auto-generated for holdId and correlationId
- The system runs periodic cleanup every 5 seconds to release expired holds
- Transaction isolation ensures no race conditions on concurrent requests

---

## Quick Copy-Paste Test Suite

Run all basic tests in sequence:

```bash
# 1. Hold a seat
curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"TEST","seatId":"A1","userId":"tester","holdSeconds":600}' | jq '.'

# 2. Check the seat
curl -s "http://localhost:8080/check?eventId=TEST&seatId=A1" | jq '.'

# 3. Try to hold it again (should fail)
curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"TEST","seatId":"A1","userId":"tester2","holdSeconds":600}' | jq '.'

# 4. Test short expiration
curl -s -X POST "http://localhost:8080/hold" -H "Content-Type: application/json" -d '{"eventId":"TEST","seatId":"B2","userId":"tester","holdSeconds":5}' | jq '.status'
sleep 7
curl -s "http://localhost:8080/check?eventId=TEST&seatId=B2" | jq '.seatStatus'

# 5. Test concurrency
./test-concurrency.sh
```

---

**For more detailed information:**
- See `README.md` for system architecture
- See `CONCURRENCY.md` for concurrency implementation details
