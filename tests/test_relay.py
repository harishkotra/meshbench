"""End-to-end: real service processes, a real partition, a real relayed delivery.

These tests are slow (~15s) and need the SDK service binary, so they are gated behind
MESHBENCH_E2E=1.  `meshbench doctor` tells you whether the binary is available.
"""

import asyncio
import os

import pytest

from meshbench.devnet import DevNet, find_service

pytestmark = pytest.mark.skipif(
    os.environ.get("MESHBENCH_E2E") != "1" or not find_service(),
    reason="set MESHBENCH_E2E=1 and install offline-protocol-sdk[lan,http]",
)


def run(coro):
    return asyncio.run(coro)


def test_chain_relays_through_the_middle_node(tmp_path):
    async def scenario():
        net = DevNet(home=tmp_path / "chain", nodes=3, topology="chain")
        await net.start()
        try:
            sent = await net.send("A", "C", "relay proof", "High")
            mid = sent["message_id"]
            row = {}
            for _ in range(200):
                row = net.ledger.get(mid) or {}
                if row.get("state") in {"delivered", "received", "failed"}:
                    break
                await asyncio.sleep(0.1)
            return row
        finally:
            await net.stop()

    row = run(scenario())
    assert row["state"] in {"delivered", "received"}, row
    assert row["hops"] >= 1
    assert row["relays"] == ["B"]
    assert row["content"] == "relay proof"


def test_cut_link_holds_then_restore_delivers(tmp_path):
    async def scenario():
        net = DevNet(home=tmp_path / "cut", nodes=3, topology="chain")
        await net.start()
        try:
            await net.set_edge("A-B", "cut")
            snapshot = {n.id: dict(n.neighbors) for n in net.nodes}
            sent = await net.send("A", "C", "held during partition", "High")
            mid = sent["message_id"]
            await asyncio.sleep(6)
            # Ledger rows are live objects: copy the state, or the restore below
            # rewrites the very snapshot this test is asserting on.
            during = dict(net.ledger.get(mid) or {})
            during["_neighbors"] = snapshot
            await net.set_edge("A-B", "up")
            for _ in range(300):
                row = net.ledger.get(mid) or {}
                if row.get("state") in {"delivered", "received", "failed"}:
                    break
                await asyncio.sleep(0.1)
            return during, row
        finally:
            await net.stop()

    during, after = run(scenario())
    assert during["state"] not in {"received"}, during["state"]
    assert during["_neighbors"]["A"] == {}, "A must have no links during the partition"
    assert after["state"] in {"delivered", "received"}, after
    assert after["relays"] == ["B"]


def test_node_restart_rejoins_and_mesh_heals(tmp_path):
    async def scenario():
        net = DevNet(home=tmp_path / "restart", nodes=3, topology="chain")
        await net.start()
        try:
            await net.stop_node("B")
            await asyncio.sleep(1)
            await net.start_node("B")
            state = await net.wait_ready(timeout=45)
            return state, {n.id: dict(n.neighbors) for n in net.nodes}
        finally:
            await net.stop()

    ready, neighbors = run(scenario())
    assert ready["ready"] is True, ready
    assert neighbors["A"] and neighbors["C"]
