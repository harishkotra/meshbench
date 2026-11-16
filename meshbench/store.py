"""Message ledger and event log for a running devnet.

Every local-API event from every node lands here. The ledger correlates the
per-node view (sent / relayed / delivered / received) into one row per message,
which is what makes hop counts and the three delivery levels legible.
"""

from __future__ import annotations

import statistics
import time
from collections import OrderedDict, deque
from typing import Any, Optional

STATE_ORDER = {
    "queued": 1,
    "sent": 2,
    "deferred": 3,
    "relayed": 4,
    "delivered": 5,
    "received": 6,
    "failed": 7,
}

#: Event types worth streaming to the UI feed (everything else lands in the raw
#: log only, to keep the inspector readable).
NOTABLE = {
    "message_sent",
    "message_delivered",
    "message_deferred",
    "message_relayed",
    "message_received",
    "message_failed",
    "message_expired",
    "secure_session_established",
    "neighbor_discovered",
    "neighbor_lost",
    "peer_unreachable",
    "dors_transport_selected",
    "welcome_branch",
}


def _first(event: dict, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in event and event[key] is not None:
            return event[key]
    return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class Ledger:
    def __init__(self, max_messages: int = 400, max_events: int = 3000) -> None:
        self.messages: "OrderedDict[str, dict]" = OrderedDict()
        self.events: deque[dict] = deque(maxlen=max_events)
        self.max_messages = max_messages
        self.seq = 0
        self.counts = {"sent": 0, "delivered": 0, "received": 0, "deferred": 0, "failed": 0}

    # ---------------------------------------------------------------- ingest
    def ingest(self, node_id: str, event: dict, addr_to_node: dict[str, str]) -> dict:
        self.seq += 1
        etype = str(event.get("type") or event.get("event") or "unknown")
        sender_addr = _first(event, "sender", "sender_address", "from")
        recipient_addr = _first(event, "recipient", "recipient_address", "to")

        record = {
            "seq": self.seq,
            "ts": time.time(),
            "node": node_id,
            "type": etype,
            "message_id": _first(event, "message_id", "id"),
            "sender": sender_addr,
            "recipient": recipient_addr,
            "sender_node": addr_to_node.get(sender_addr or "", node_id if etype == "message_sent" else None),
            "recipient_node": addr_to_node.get(recipient_addr or ""),
            "hop_count": _as_int(_first(event, "hop_count", "hops", default=0)),
            "latency_ms": _as_int(_first(event, "latency_ms", "latency", default=0)),
            "transport": _first(event, "transport", "transport_type", default=""),
            "reason": _first(event, "reason", "error", "detail", default=""),
            "priority": _first(event, "priority"),
            "peer_id": _first(event, "peer_id", "peer"),
            "preview": self._preview(_first(event, "content", "payload", "body")),
        }
        if etype in NOTABLE:
            self.events.append(record)

        if etype.startswith("message_") and record["message_id"]:
            self._apply(record, etype, event, addr_to_node)
        return record

    @staticmethod
    def _preview(content: Any) -> str:
        if content is None:
            return ""
        text = content if isinstance(content, str) else str(content)
        if text.startswith("__MLS_ENC__"):
            return ""  # ciphertext on the wire; the recipient event carries plaintext
        return text if len(text) <= 160 else text[:157] + "..."

    def _apply(self, rec: dict, etype: str, event: dict, addr_to_node: dict[str, str]) -> None:
        mid = rec["message_id"]
        row = self.messages.get(mid)
        now = rec["ts"]
        # Control frames (ack/hello/keying traffic) also emit message_delivered;
        # only a real send or a real app receipt opens a ledger row.
        if row is None and etype not in {"message_sent", "message_received"}:
            return
        if row is None:
            row = {
                "id": mid,
                "content": rec["preview"],
                "from_node": rec["sender_node"],
                "to_node": rec["recipient_node"],
                "from_addr": rec["sender"],
                "to_addr": rec["recipient"],
                "state": "queued",
                "created": now,
                "sent_at": None,
                "delivered_at": None,
                "received_at": None,
                "hops": 0,
                "latency_ms": 0,
                "transport": "",
                "relays": [],
                "deferrals": 0,
                "reason": "",
                "priority": rec["priority"],
            }
            self.messages[mid] = row
            if len(self.messages) > self.max_messages:
                self.messages.popitem(last=False)

        if rec["sender_node"]:
            row["from_node"] = row["from_node"] or rec["sender_node"]
        if rec["recipient_node"]:
            row["to_node"] = row["to_node"] or rec["recipient_node"]

        def bump(state: str) -> None:
            if STATE_ORDER.get(state, 0) > STATE_ORDER.get(row["state"], 0):
                row["state"] = state

        if etype == "message_sent":
            row["sent_at"] = row["sent_at"] or now
            if rec["preview"] and not row["content"]:
                row["content"] = rec["preview"]
            bump("sent")
            self.counts["sent"] += 1
        elif etype == "message_deferred":
            row["deferrals"] += 1
            row["reason"] = rec["reason"] or "peer_not_reachable"
            bump("deferred")
            self.counts["deferred"] += 1
        elif etype == "message_relayed":
            if rec["node"] not in row["relays"]:
                row["relays"].append(rec["node"])
            bump("relayed")
        elif etype == "message_delivered":
            row["delivered_at"] = row["delivered_at"] or now
            row["latency_ms"] = rec["latency_ms"] or row["latency_ms"]
            row["hops"] = max(row["hops"], rec["hop_count"])
            row["transport"] = rec["transport"] or row["transport"]
            bump("delivered")
            self.counts["delivered"] += 1
        elif etype == "message_received":
            row["received_at"] = row["received_at"] or now
            row["hops"] = max(row["hops"], rec["hop_count"])
            row["transport"] = rec["transport"] or row["transport"]
            # The recipient's receipt usually lands before the sender's delivery
            # ack; fall back to wall-clock so the ledger never shows a blank time.
            if not row["latency_ms"] and row["sent_at"]:
                row["latency_ms"] = max(0, int((now - row["sent_at"]) * 1000))
            if rec["preview"]:
                row["content"] = rec["preview"]
            if rec["recipient_node"]:
                row["to_node"] = rec["recipient_node"]
            bump("received")
            self.counts["received"] += 1
        elif etype in {"message_failed", "message_expired"}:
            row["reason"] = rec["reason"] or etype
            row["state"] = "failed"
            self.counts["failed"] += 1

    # -------------------------------------------------------------- reading
    def get(self, message_id: str) -> Optional[dict]:
        return self.messages.get(message_id)

    def list_messages(self, limit: int = 200) -> list[dict]:
        rows = list(self.messages.values())[-limit:]
        return [dict(r) for r in reversed(rows)]

    def list_events(self, limit: int = 200) -> list[dict]:
        return [dict(e) for e in list(self.events)[-limit:]]

    def stats(self) -> dict:
        rows = list(self.messages.values())
        latencies = [r["latency_ms"] for r in rows if r["latency_ms"]]
        hops = [r["hops"] for r in rows if r["state"] in {"delivered", "received"}]
        return {
            "messages": len(rows),
            "delivered": sum(1 for r in rows if r["state"] in {"delivered", "received"}),
            "received": self.counts["received"],
            "deferred": sum(1 for r in rows if r["deferrals"] > 0),
            "failed": self.counts["failed"],
            "max_hops": max(hops, default=0),
            "median_hops": statistics.median(hops) if hops else 0,
            "median_latency_ms": int(statistics.median(latencies)) if latencies else 0,
        }
