"""Ledger correlation: several nodes' events must fold into one truthful row."""

from meshbench.store import Ledger

A = "off1aaa"
B = "off1bbb"
C = "off1ccc"
ADDRS = {A: "A", B: "B", C: "C"}


def test_three_node_relay_folds_into_one_row():
    led = Ledger()
    mid = "m-1"
    led.ingest("A", {"type": "message_sent", "message_id": mid, "sender": A, "recipient": C,
                     "content": "__MLS_ENC__ciphertext", "priority": "high"}, ADDRS)
    led.ingest("B", {"type": "message_relayed", "message_id": mid, "hop_count": 1}, ADDRS)
    led.ingest("C", {"type": "message_received", "message_id": mid, "sender": A, "recipient": C,
                     "content": "hello mesh"}, ADDRS)
    led.ingest("A", {"type": "message_delivered", "message_id": mid, "latency_ms": 321,
                     "hop_count": 1, "transport": "wifiDirect"}, ADDRS)

    row = led.get(mid)
    assert row is not None
    assert row["state"] == "received"
    assert row["from_node"] == "A" and row["to_node"] == "C"
    assert row["hops"] == 1
    assert row["relays"] == ["B"]
    assert row["latency_ms"] == 321
    assert row["transport"] == "wifiDirect"
    # Ciphertext on the wire is never shown; the recipient's plaintext is.
    assert row["content"] == "hello mesh"
    assert len(led.list_messages()) == 1


def test_receipt_before_delivery_ack_still_reports_latency():
    led = Ledger()
    mid = "m-2"
    led.ingest("A", {"type": "message_sent", "message_id": mid, "sender": A, "recipient": B,
                     "content": "ping"}, ADDRS)
    sent_at = led.get(mid)["sent_at"]
    led.get(mid)["sent_at"] = sent_at - 0.4  # pretend the send happened 400ms earlier
    led.ingest("B", {"type": "message_received", "message_id": mid, "sender": A,
                     "recipient": B, "content": "ping"}, ADDRS)
    assert 300 <= led.get(mid)["latency_ms"] <= 600
    assert led.get(mid)["state"] == "received"


def test_control_frames_do_not_pollute_the_ledger():
    led = Ledger()
    # The SDK also emits delivered/relayed for ack + keying traffic.
    led.ingest("A", {"type": "message_delivered", "message_id": "ack-1", "latency_ms": 12}, ADDRS)
    led.ingest("A", {"type": "message_relayed", "message_id": "ack-2"}, ADDRS)
    assert led.list_messages() == []


def test_failure_is_terminal():
    led = Ledger()
    mid = "m-3"
    led.ingest("A", {"type": "message_sent", "message_id": mid, "sender": A, "recipient": C,
                     "content": "doomed"}, ADDRS)
    led.ingest("A", {"type": "message_failed", "message_id": mid, "reason": "ttl_expired"}, ADDRS)
    led.ingest("C", {"type": "message_received", "message_id": mid, "sender": A,
                     "recipient": C, "content": "doomed"}, ADDRS)
    row = led.get(mid)
    assert row["state"] == "failed"
    assert row["reason"] == "ttl_expired"
