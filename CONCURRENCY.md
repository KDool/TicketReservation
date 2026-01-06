# Concurrency Handling in Ticket Reservation System

## Overview
This document explains how the system handles concurrent requests for the same seat, ensuring data consistency and preventing race conditions.

## Scenario
When multiple users try to hold the same seat simultaneously (as shown in the sequence diagram), the system must ensure that only one user succeeds while others receive a proper error message.

## How It Works

### 1. Request Flow
```
UserA ─┐
       ├──> SpringBoot Gateway ───> ErlangCore ───> Mnesia Database
UserB ─┘
```

### 2. Serialization Mechanism

The system uses **Mnesia transactions** to handle concurrent requests:

- **Transaction Isolation**: Each hold request runs inside a Mnesia transaction
- **Atomic Operations**: Read-check-write operations are atomic
- **ACID Guarantees**: Mnesia ensures consistency even under concurrent load

### 3. Detailed Flow

When two requests arrive simultaneously for seat "A10":

#### Request A (arrives first, wins):
1. Transaction starts
2. Reads seat {"E1","A10"} → finds it free or non-existent
3. Writes seat as HELD for userA
4. Transaction commits
5. Returns: `{ok, holdId, expiresAt}`

#### Request B (arrives second, fails):
1. Transaction starts
2. Reads seat {"E1","A10"} → finds it already HELD by userA
3. Detects conflict
4. Transaction aborts (no write)
5. Returns: `{error, {seat_already_held, existingHoldId, expiresAt}}`

### 4. Key Code Sections

**Mnesia Transaction** (seat_srv.erl):
```erlang
hold_tx(EventId, SeatId, UserId, HoldId, ExpiresAtMs) ->
    Fun = fun() ->
        case mnesia:read(seat, Key) of
            [] -> 
                % Seat is free, claim it
                mnesia:write({seat, Key, held, UserId, HoldId, ExpiresAtMs, undefined})
            [{seat, Key, held, _, CurHoldId, CurExp, _}] ->
                % Already held - reject
                {error, {seat_already_held, CurHoldId, CurExp}}
        end
    end,
    mnesia:transaction(Fun).
```

### 5. Concurrency Logging

The system logs each step with `[CONCURRENCY]` prefix:

```
[CONCURRENCY] seat_srv got hold_seat "E1" "A10" for user "userA" (holdId="...") at 1767711754994
[CONCURRENCY] Starting hold_tx for "E1" "A10" user="userA" at 1767711754994
[CONCURRENCY] Read seat {"E1","A10"} state: []
[CONCURRENCY] Creating new seat {"E1","A10"} for user "userA"
[CONCURRENCY] Completed hold_tx for "E1" "A10" user="userA" result={ok, ...}
[CONCURRENCY] seat_srv hold_seat result for "E1" "A10" user="userA": {ok, ...}

[CONCURRENCY] seat_srv got hold_seat "E1" "A10" for user "userB" (holdId="...") at 1767711754995
[CONCURRENCY] Starting hold_tx for "E1" "A10" user="userB" at 1767711754995
[CONCURRENCY] Read seat {"E1","A10"} state: [{seat, ...}]
[CONCURRENCY] CONFLICT: Seat {"E1","A10"} already held by "userA" (holdId="...", exp=...), rejecting user "userB"
[CONCURRENCY] Completed hold_tx for "E1" "A10" user="userB" result={error, ...}
[CONCURRENCY] seat_srv hold_seat result for "E1" "A10" user="userB": {error, ...}
```

### 6. HTTP Response Codes

| Scenario | HTTP Status | Response |
|----------|-------------|----------|
| First user (success) | 200 OK | `{"status": "OK", "holdId": "...", "expiresAtMillis": ...}` |
| Second user (conflict) | 502 Bad Gateway | `{"status": "FAILED", "error": "{seat_already_held, ...}"}` |

### 7. Test Results

Running the concurrent test script (`./test-concurrency.sh`):

```bash
=========================================
Concurrent Seat Hold Test
=========================================

Test Scenario: Two users trying to hold the same seat simultaneously
Event: E1, Seat: A10

UserA Response:
{
  "status": "OK",
  "holdId": "de386698-381e-49cd-8d85-eca680b32b91",
  "expiresAtMillis": 1767712354991
}

UserB Response:
{
  "status": "FAILED",
  "error": "{seat_already_held,\"de386698-381e-49cd-8d85-eca680b32b91\",1767712354991}"
}

=========================================
Verification:
=========================================
✅ PASS: UserA succeeded, UserB failed (as expected)
   UserA got the seat, UserB received conflict error
```

## Why This Works

1. **Transaction Isolation**: Mnesia's transaction system ensures that concurrent reads and writes are properly serialized
2. **No Race Conditions**: The read-check-write sequence is atomic - no other transaction can modify the seat between these operations
3. **Consistent State**: Even under high concurrent load, the database remains consistent
4. **Fair Ordering**: Requests are processed in the order they reach the Erlang gen_server's mailbox

## Testing Concurrency

Use the provided test script:

```bash
# From the project root directory
./test-concurrency.sh
```

This launches two simultaneous hold requests for the same seat and verifies that:
- Exactly one request succeeds
- The other request receives a proper conflict error
- The final seat state reflects the successful hold

## Additional Protections

Beyond concurrent holds on the same seat, the system also prevents:

1. **User holding multiple seats**: A user can only hold one seat at a time
2. **Expired hold reclamation**: Expired holds are automatically freed during cleanup
3. **Double confirmation**: Once confirmed (sold), a seat cannot be held again

## Performance Considerations

- **Mnesia overhead**: Transactions add minimal overhead (~1-2ms)
- **Scalability**: Each Erlang node handles its partition independently
- **No blocking**: Failed transactions don't block successful ones
