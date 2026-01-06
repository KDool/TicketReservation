#!/bin/bash

# Test script for concurrent seat hold requests
# This simulates the scenario from the diagram where UserA and UserB
# try to hold the same seat X simultaneously

echo "========================================="
echo "Concurrent Seat Hold Test"
echo "========================================="
echo ""

# Define test parameters - use timestamp to ensure unique seat ID
TIMESTAMP=$(date +%s)
SEAT_NUM=$((TIMESTAMP % 100))
EVENT_ID="E1"
SEAT_ID="A${SEAT_NUM}"
HOLD_SECONDS=600

echo "Test Scenario: Two users trying to hold the same seat simultaneously"
echo "Event: $EVENT_ID, Seat: $SEAT_ID"
echo ""

# Clear any existing hold on this seat first
echo "1. Clearing any existing holds..."
curl -s "http://localhost:8080/check?eventId=$EVENT_ID&seatId=$SEAT_ID" > /dev/null
sleep 1

# Launch two concurrent requests
echo "2. Launching concurrent hold requests from UserA and UserB..."
echo ""

# Start both requests in background and capture their outputs
(
  echo "UserA request started at $(date +%H:%M:%S.%N)"
  RESPONSE_A=$(curl -s -X POST "http://localhost:8080/hold" \
    -H "Content-Type: application/json" \
    -d "{\"eventId\":\"$EVENT_ID\",\"seatId\":\"$SEAT_ID\",\"userId\":\"userA\",\"holdSeconds\":$HOLD_SECONDS}")
  echo "UserA response received at $(date +%H:%M:%S.%N)"
  echo "$RESPONSE_A" | jq '.' > /tmp/userA_response.json
  echo "$RESPONSE_A" | jq -r '.status' > /tmp/userA_status.txt
) &
PID_A=$!

(
  echo "UserB request started at $(date +%H:%M:%S.%N)"
  RESPONSE_B=$(curl -s -X POST "http://localhost:8080/hold" \
    -H "Content-Type: application/json" \
    -d "{\"eventId\":\"$EVENT_ID\",\"seatId\":\"$SEAT_ID\",\"userId\":\"userB\",\"holdSeconds\":$HOLD_SECONDS}")
  echo "UserB response received at $(date +%H:%M:%S.%N)"
  echo "$RESPONSE_B" | jq '.' > /tmp/userB_response.json
  echo "$RESPONSE_B" | jq -r '.status' > /tmp/userB_status.txt
) &
PID_B=$!

# Wait for both requests to complete
wait $PID_A
wait $PID_B

echo ""
echo "========================================="
echo "Results:"
echo "========================================="
echo ""

STATUS_A=$(cat /tmp/userA_status.txt)
STATUS_B=$(cat /tmp/userB_status.txt)

echo "UserA Response:"
cat /tmp/userA_response.json
echo ""
echo ""

echo "UserB Response:"
cat /tmp/userB_response.json
echo ""
echo ""

# Verify the expected behavior
echo "========================================="
echo "Verification:"
echo "========================================="

if [ "$STATUS_A" == "OK" ] && [ "$STATUS_B" == "FAILED" ]; then
    echo "✅ PASS: UserA succeeded, UserB failed (as expected)"
    echo "   UserA got the seat, UserB received conflict error"
elif [ "$STATUS_A" == "FAILED" ] && [ "$STATUS_B" == "OK" ]; then
    echo "✅ PASS: UserB succeeded, UserA failed (as expected)"
    echo "   UserB got the seat, UserA received conflict error"
elif [ "$STATUS_A" == "OK" ] && [ "$STATUS_B" == "OK" ]; then
    echo "❌ FAIL: Both users succeeded - RACE CONDITION DETECTED!"
    echo "   This indicates a concurrency bug in the system"
else
    echo "⚠️  UNEXPECTED: Both users failed"
    echo "   UserA: $STATUS_A, UserB: $STATUS_B"
fi

echo ""
echo "3. Checking final seat state..."
FINAL_STATE=$(curl -s "http://localhost:8080/check?eventId=$EVENT_ID&seatId=$SEAT_ID")
echo "$FINAL_STATE" | jq '.'

# Cleanup
rm -f /tmp/userA_response.json /tmp/userB_response.json /tmp/userA_status.txt /tmp/userB_status.txt

echo ""
echo "Test completed!"
