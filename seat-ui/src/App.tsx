import React, { useEffect, useMemo, useState } from "react";

type SeatStatus = "FREE" | "HOLD" | "CONFIRMED";

type Seat = {
  seatId: string;
  row: string;
  col: number;
  status: SeatStatus;
  holdId?: string;
};

type SnapshotSeat = {
  seatId: string;
  status: SeatStatus;
  holdId?: string;
};

const ROWS = ["A", "B", "C", "D", "E", "F"];
const COLS = 50;
const ENV = (import.meta as any).env || {};
const EVENT_ID = ENV.VITE_EVENT_ID || "E1";
const HOLD_SECONDS = Number.parseInt(ENV.VITE_HOLD_SECONDS || "30", 10);
const API_BASE = ENV.VITE_API_BASE || "http://localhost:8080";

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function createInitialSeats(): Seat[] {
  const seats: Seat[] = [];
  for (const r of ROWS) {
    for (let c = 1; c <= COLS; c++) {
      seats.push({
        seatId: `${r}${pad2(c)}`,
        row: r,
        col: c,
        status: "FREE",
      });
    }
  }
  return seats;
}

async function apiHold(params: { eventId: string; userId: string; seatId: string }) {
  const res = await fetch(`${API_BASE}/hold`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...params, holdSeconds: HOLD_SECONDS }),
  });
  const data = await res.json();
  return { ok: res.ok, data };
}

async function apiConfirm(params: { userId: string; holdId: string }) {
  const res = await fetch(`${API_BASE}/reservations/confirm`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });
  const data = await res.json();
  return { ok: res.ok, data };
}

async function apiSnapshot(eventId: string) {
  const res = await fetch(`${API_BASE}/api/events/${eventId}/seats`);
  const data = await res.json();
  if (!res.ok) throw new Error(data.message || "Snapshot failed");
  return data;
}

export default function App() {
  const [seats, setSeats] = useState<Seat[]>(createInitialSeats);
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const seatById = useMemo(() => {
    const m = new Map<string, Seat>();
    seats.forEach(s => m.set(s.seatId, s));
    return m;
  }, [seats]);

  function showToast(payload: {
    status?: string;
    holdId?: string;
    orderId?: string;
    error?: string;
    correlationId?: string;
  }) {
    const parts = [
      payload.status && `status=${payload.status}`,
      payload.holdId && `holdId=${payload.holdId}`,
      payload.orderId && `orderId=${payload.orderId}`,
      payload.error && `error=${payload.error}`,
      payload.correlationId && `correlationId=${payload.correlationId}`,
    ].filter(Boolean);
    setToast(parts.length ? parts.join(" | ") : "Request completed");
    window.setTimeout(() => setToast(null), 4000);
  }

  async function confirmWithPrompt(seatId: string, holdId: string) {
    const userId = prompt("userId") || "";
    if (!userId) return;
    setBusy(true);
    try {
      const res = await apiConfirm({ userId, holdId });
      if (res.ok) {
        setSeats(prev =>
          prev.map(s => s.seatId === seatId ? { ...s, status: "CONFIRMED" } : s)
        );
      }
      const data = res.data || {};
      showToast({
        status: data.status,
        holdId,
        orderId: data?.data?.orderId,
        error: data.error,
        correlationId: data.correlationId,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      showToast({ status: "error", holdId, error: message });
    } finally {
      setBusy(false);
    }
  }

  async function hold(seatId: string) {
    const userId = prompt("userId") || "";
    if (!userId) return;
    let holdRes: { holdId?: string; userId?: string } | null = null;
    setBusy(true);
    try {
      const res = await apiHold({ eventId: EVENT_ID, userId, seatId });
      const data = res.data || {};
      if (res.ok) {
        holdRes = data;
        setSeats(prev =>
          prev.map(s => s.seatId === seatId ? { ...s, status: "HOLD", holdId: data.holdId } : s)
        );
      }
      showToast({
        status: data.status,
        holdId: data.holdId,
        error: data.error,
        correlationId: data.correlationId,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      showToast({ status: "error", error: message });
    } finally {
      setBusy(false);
    }
    if (!holdRes?.holdId) return;
    const shouldConfirm = window.confirm("Hold created. Confirm reservation?");
    if (shouldConfirm) {
      await confirmWithPrompt(seatId, holdRes.holdId);
    }
  }

  async function refresh() {
    setBusy(true);
    try {
      const snap = await apiSnapshot(EVENT_ID);
      const map = new Map<string, SnapshotSeat>();
      snap.seats.forEach((s: SnapshotSeat) => map.set(s.seatId, s));
      setSeats(prev =>
        prev.map(s => {
          const ss = map.get(s.seatId);
          return ss ? { ...s, status: ss.status, holdId: ss.holdId } : { ...s, status: "FREE", holdId: undefined };
        })
      );
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  return (
    <div style={{ padding: 20 }}>
      {toast && (
        <div
          style={{
            position: "fixed",
            top: 16,
            right: 16,
            background: "#222",
            color: "#fff",
            padding: "10px 12px",
            borderRadius: 6,
            boxShadow: "0 2px 8px rgba(0,0,0,0.25)",
            zIndex: 1000,
            maxWidth: 360,
          }}
        >
          {toast}
        </div>
      )}
      <h2>Seat UI</h2>
      <button onClick={refresh} disabled={busy}>Refresh</button>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(440px, 1fr))",
          gap: 16,
          marginTop: 16,
          alignItems: "start",
          maxWidth: 1200,
          width: "100%",
        }}
      >
        {ROWS.map(zone => (
          <div
            key={zone}
            style={{
              border: "1px solid #ddd",
              borderRadius: 8,
              padding: 16,
              background: "#fafafa",
              minHeight: 280,
            }}
          >
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Zone {zone}</div>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(10, 36px)",
                gap: 6,
                justifyContent: "center",
              }}
            >
              {Array.from({ length: COLS }, (_, i) => i + 1).map(c => {
                const id = `${zone}${pad2(c)}`;
                const s = seatById.get(id)!;
                const bg = s.status === "FREE" ? "#fff" : s.status === "HOLD" ? "#f00" : "#0f0";
                return (
                  <button
                    key={id}
                    onClick={async () => {
                      if (s.status === "FREE") {
                        const shouldHold = window.confirm("Seat is free. Hold it?");
                        if (shouldHold) await hold(id);
                        return;
                      }
                      if (s.status === "HOLD" && s.holdId) {
                        const shouldConfirm = window.confirm("Confirm reservation for this hold?");
                        if (shouldConfirm) await confirmWithPrompt(id, s.holdId);
                      }
                    }}
                    style={{
                      background: bg,
                      width: 36,
                      height: 36,
                      border: "1px solid #ccc",
                      borderRadius: 4,
                    }}
                  >
                    {c}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
