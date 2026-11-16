"""DevNet: a set of real Offline Protocol nodes on loopback, plus link control.

Nothing here re-implements the protocol. Every node is an actual
``offline-protocol-service`` process with its own MLS store, state directory and
local-API socket; the devnet only wires them together through controllable TCP
proxies and observes the event stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import secrets
import shutil
import signal
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .localapi import NodeLink
from .proxy import LinkProxy
from .store import Ledger

log = logging.getLogger("meshbench.devnet")

LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def find_service() -> str:
    """Locate the SDK's service binary, preferring the running interpreter's venv."""
    override = os.environ.get("MESHBENCH_SERVICE")
    if override:
        return override
    beside_python = Path(sys.executable).parent / "offline-protocol-service"
    if beside_python.exists():
        return str(beside_python)
    return shutil.which("offline-protocol-service") or ""


def free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return sock.getsockname()[1]


def node_label(index: int) -> str:
    return LABELS[index] if index < len(LABELS) else f"N{index + 1}"


@dataclass
class Node:
    id: str
    index: int
    peer_port: int
    api_port: int
    root: Path
    token_path: Path
    proc: Optional[asyncio.subprocess.Process] = None
    link: Optional[NodeLink] = None
    address: Optional[str] = None
    running: bool = False
    started_at: Optional[float] = None
    restarts: int = 0
    metrics: dict = field(default_factory=dict)
    neighbors: dict[str, str] = field(default_factory=dict)  # peer addr -> transport
    log_path: Optional[Path] = None

    @property
    def short(self) -> str:
        return self.address[-8:] if self.address else "-"


@dataclass
class Edge:
    id: str
    source: str
    target: str
    forward: LinkProxy
    backward: LinkProxy
    state: str = "up"


class DevNet:
    def __init__(
        self,
        home: str | Path,
        nodes: int = 3,
        topology: str = "chain",
        host: str = "127.0.0.1",
        app_id: str = "meshbench",
        keep_home: bool = False,
    ) -> None:
        self.home = Path(home).expanduser().resolve()
        self.node_count = max(2, nodes)
        self.topology = topology if topology in {"chain", "star", "ring", "full"} else "chain"
        self.host = host
        self.app_id = app_id
        self.keep_home = keep_home
        self.id = uuid.uuid4().hex[:8]
        self.started_at = time.time()
        self.ledger = Ledger()
        self.nodes: list[Node] = []
        self.edges: list[Edge] = []
        self.addr_to_node: dict[str, str] = {}
        self.service_bin = find_service()
        self.sdk_version = ""
        self._metrics_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------- topology
    def _pairs(self) -> list[tuple[int, int]]:
        n = self.node_count
        if self.topology == "chain":
            return [(i, i + 1) for i in range(n - 1)]
        if self.topology == "ring":
            return [(i, (i + 1) % n) for i in range(n)] if n > 2 else [(0, 1)]
        if self.topology == "star":
            return [(0, i) for i in range(1, n)]
        # full mesh
        return [(i, j) for i in range(n) for j in range(i + 1, n)]

    def _position(self, index: int) -> tuple[float, float]:
        n = self.node_count
        if self.topology == "chain":
            return ((index + 0.5) / n, 0.5)
        if self.topology == "star":
            if index == 0:
                return (0.5, 0.5)
            angle = 2 * 3.141592653589793 * (index - 1) / max(1, n - 1) - 3.141592653589793 / 2
            return (0.5 + 0.36 * _cos(angle), 0.5 + 0.36 * _sin(angle))
        angle = 2 * 3.141592653589793 * index / n - 3.141592653589793 / 2
        return (0.5 + 0.34 * _cos(angle), 0.5 + 0.34 * _sin(angle))

    # ---------------------------------------------------------------- setup
    def _prepare_home(self) -> None:
        for sub in ("nodes", "run"):
            (self.home / sub).mkdir(parents=True, exist_ok=True)
        key_file = self.home / "store.key"
        if not key_file.exists():
            key_file.write_text(secrets.token_hex(32))
            key_file.chmod(0o600)
        config = {
            "app_id": self.app_id,
            "profile": "gateway",
            "ble_enabled": False,
            "wifi_direct_enabled": True,
            "internet_enabled": False,
            "reticulum_enabled": False,
            "nostr_enabled": False,
            "prefer_online": False,
            "initial_ttl": 8,
            "encryption_enabled": True,
            "auto_key_exchange": True,
            "store_pending": True,
            "require_encryption": True,
            "max_pending_per_peer": 64,
            "max_pending_global": 4096,
            "pending_ttl_ms": 86400000,
            "overflow_policy": "DropOldest",
        }
        (self.home / "config.json").write_text(json.dumps(config, indent=2))

    def _build_topology(self) -> None:
        for i in range(self.node_count):
            label = node_label(i)
            root = self.home / "nodes" / label
            (root / "mls").mkdir(parents=True, exist_ok=True)
            (root / "state").mkdir(parents=True, exist_ok=True)
            self.nodes.append(
                Node(
                    id=label,
                    index=i,
                    peer_port=free_port(self.host),
                    api_port=free_port(self.host),
                    root=root,
                    token_path=root / "token",
                    log_path=root / "service.log",
                )
            )
        seen: set[str] = set()
        for a, b in self._pairs():
            key = f"{min(a, b)}-{max(a, b)}"
            if key in seen:
                continue
            seen.add(key)
            src, dst = self.nodes[a], self.nodes[b]
            self.edges.append(
                Edge(
                    id=f"{src.id}-{dst.id}",
                    source=src.id,
                    target=dst.id,
                    forward=LinkProxy(free_port(self.host), dst.peer_port, self.host),
                    backward=LinkProxy(free_port(self.host), src.peer_port, self.host),
                )
            )

    # -------------------------------------------------------------- process
    async def _spawn(self, node: Node) -> None:
        if not self.service_bin:
            raise RuntimeError(
                "offline-protocol-service not found. Install the SDK:\n"
                "  pip install 'offline-protocol-sdk[lan,http]==0.33.0'"
            )
        peers: list[str] = []
        for edge in self.edges:
            if edge.source == node.id:
                peers.append(f"{self.host}:{edge.forward.listen_port}")
            elif edge.target == node.id:
                peers.append(f"{self.host}:{edge.backward.listen_port}")
        args = [
            self.service_bin,
            "--config", str(self.home / "config.json"),
            "--tcp", str(node.api_port),
            "--token-file", str(node.token_path),
            "--mls-root", str(node.root / "mls"),
            "--state-root", str(node.root / "state"),
            "--listen", f"{self.host}:{node.peer_port}",
            "--no-health",
            "--log-level", "info",
        ]
        for peer in peers:
            args += ["--peer", peer]
        env = dict(os.environ)
        env["OFFLINE_PROTOCOL_STORE_KEY"] = (self.home / "store.key").read_text().strip()
        env["RUST_LOG"] = "warn"
        if node.link:
            node.link.expect_hello()
        logf = open(node.log_path, "ab", buffering=0)
        logf.write(f"\n--- start {time.strftime('%H:%M:%S')} peers={peers} ---\n".encode())
        node.proc = await asyncio.create_subprocess_exec(
            *args, env=env, stdout=logf, stderr=logf, start_new_session=True
        )
        node.running = True
        node.started_at = time.time()
        for _ in range(200):
            if node.proc.returncode is not None:
                raise RuntimeError(f"node {node.id} exited early:\n{self._log_tail(node)}")
            if node.token_path.exists() and node.link:
                if await node.link.wait_hello(timeout=0.4):
                    return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"node {node.id} did not become ready:\n{self._log_tail(node)}")

    def _log_tail(self, node: Node, lines: int = 12) -> str:
        try:
            text = node.log_path.read_text(errors="replace").splitlines()
            return "\n".join(text[-lines:])
        except OSError:
            return "(no log)"

    async def start(self) -> None:
        if not self.service_bin:
            raise RuntimeError(
                "offline-protocol-service not found. Install the SDK:\n"
                "  pip install 'offline-protocol-sdk[lan,http]==0.33.0'"
            )
        self._prepare_home()
        self._build_topology()
        for edge in self.edges:
            await edge.forward.start()
            await edge.backward.start()
        for node in self.nodes:
            node.link = NodeLink(
                node.id,
                node.api_port,
                str(node.token_path),
                on_event=lambda ev, nid=node.id: self._on_event(nid, ev),
                on_status=lambda _up, nid=node.id: None,
                app_id=self.app_id,
                host=self.host,
            )
            await node.link.start()
        for node in self.nodes:
            await self._spawn(node)
        await self.wait_ready(timeout=30)
        for node in self.nodes:
            if node.link and node.link.address:
                node.address = node.link.address
                self.addr_to_node[node.address] = node.id
            if node.link:
                await node.link.subscribe()
        self._metrics_task = asyncio.create_task(self._poll_metrics(), name="metrics")
        self.sdk_version = await self._read_version()

    async def _read_version(self) -> str:
        try:
            from importlib.metadata import version

            return version("offline-protocol-sdk")
        except Exception:
            return ""

    async def wait_ready(self, timeout: float = 30.0) -> dict:
        """Wait until every node has an address and each edge has been discovered."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for node in self.nodes:
                if node.link and node.link.address and not node.address:
                    node.address = node.link.address
                    self.addr_to_node.setdefault(node.address, node.id)
            missing: list[str] = []
            for edge in self.edges:
                src, dst = self._node(edge.source), self._node(edge.target)
                if not src or not dst:
                    continue
                if dst.address not in src.neighbors:
                    missing.append(f"{edge.source}->{edge.target}")
                if src.address not in dst.neighbors:
                    missing.append(f"{edge.target}->{edge.source}")
            if not missing and all(n.address for n in self.nodes):
                return {"ready": True, "missing": []}
            await asyncio.sleep(0.25)
        pending = [
            f"{e.source}->{e.target}"
            for e in self.edges
            if (src := self._node(e.source)) and (dst := self._node(e.target))
            and dst.address not in src.neighbors
        ]
        return {"ready": False, "missing": pending}

    def _node(self, node_id: str) -> Optional[Node]:
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    # ---------------------------------------------------------------- events
    def _on_event(self, node_id: str, event: dict) -> None:
        record = self.ledger.ingest(node_id, event, self.addr_to_node)
        if record["type"] == "neighbor_discovered":
            node = self._node(node_id)
            peer = record.get("peer_id") or record.get("sender") or event.get("peer_address")
            transport = str(event.get("transport") or event.get("transport_type") or "link")
            if node and peer:
                node.neighbors[str(peer)] = transport
        elif record["type"] == "neighbor_lost":
            node = self._node(node_id)
            peer = record.get("peer_id") or record.get("sender") or event.get("peer_address")
            if node and peer:
                node.neighbors.pop(str(peer), None)
        self.on_change()

    def on_change(self) -> None:
        """Hook overridden by the control server to push live updates."""

    async def _poll_metrics(self) -> None:
        methods = {
            "message_stats": "get_message_stats",
            "success_rate": "get_delivery_success_rate",
            "median_latency": "get_median_latency",
            "median_hops": "get_median_hops",
            "retry_queue": "get_retry_queue_size",
            "relay": "get_mesh_relay_stats",
        }
        while not self._stopping:
            for node in self.nodes:
                if not (node.running and node.link and node.link.connected):
                    continue
                node.metrics["state"] = "running"
                for key, method in methods.items():
                    with contextlib.suppress(Exception):
                        node.metrics[key] = await node.link.call(method, {}, timeout=6)
                with contextlib.suppress(Exception):
                    sessions = await node.link.call("mls_list_sessions", {}, timeout=6)
                    node.metrics["sessions"] = len(sessions) if isinstance(sessions, list) else 0
                with contextlib.suppress(Exception):
                    node.metrics["is_relay"] = await node.link.call("is_relay", {}, timeout=6)
                node.metrics["neighbors"] = len(node.neighbors)
            self.on_change()
            await asyncio.sleep(2.0)

    # ------------------------------------------------------------- commands
    async def send(
        self,
        from_node: str,
        to_node: str,
        content: str,
        priority: str = "Medium",
    ) -> dict:
        src = self._node(from_node)
        dst = self._node(to_node)
        if not src or not dst:
            raise ValueError("unknown node")
        if not src.link or not src.link.connected:
            raise RuntimeError(f"node {from_node} is down")
        result = await src.link.call(
            "send_message",
            {"recipient": dst.address, "content": content, "priority": priority},
            timeout=25,
        )
        message_id = result if isinstance(result, str) else (result or {}).get("message_id")
        if message_id:
            row = self.ledger.messages.get(message_id)
            if row is None:
                self.ledger.ingest(
                    src.id,
                    {
                        "type": "message_sent",
                        "message_id": message_id,
                        "sender": src.address,
                        "recipient": dst.address,
                        "content": content,
                        "priority": priority,
                    },
                    self.addr_to_node,
                )
        self.on_change()
        return {"message_id": message_id, "from": src.id, "to": dst.id, "content": content}

    async def stop_node(self, node_id: str) -> dict:
        node = self._node(node_id)
        if not node:
            raise ValueError("unknown node")
        if node.proc and node.proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(node.proc.pid), signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(node.proc.wait(), 8)
        node.running = False
        node.metrics = {"state": "down"}
        node.neighbors.clear()
        self.on_change()
        return {"node": node_id, "running": False}

    async def start_node(self, node_id: str) -> dict:
        node = self._node(node_id)
        if not node:
            raise ValueError("unknown node")
        if node.proc and node.proc.returncode is None:
            return {"node": node_id, "running": True}
        await self._spawn(node)
        node.restarts += 1
        self.on_change()
        return {"node": node_id, "running": True}

    async def set_edge(self, edge_id: str, state: str) -> dict:
        edge = self._edge(edge_id)
        if not edge:
            raise ValueError("unknown edge")
        if state == "cut":
            await edge.forward.cut()
            await edge.backward.cut()
            edge.state = "cut"
            # Closing sockets is not the same as the SDK noticing. Wait until both
            # ends have actually dropped the peer, otherwise a send issued right
            # after a cut can still ride a half-dead socket and look delivered.
            await self._await_partition(edge)
        else:
            await edge.forward.restore()
            await edge.backward.restore()
            edge.state = "up"
            asyncio.create_task(self._nudge_reconnect(edge))
        self.on_change()
        return {"edge": edge.id, "state": edge.state}

    async def _await_partition(self, edge: Edge, timeout: float = 5.0) -> None:
        src, dst = self._node(edge.source), self._node(edge.target)
        if not src or not dst or not src.address or not dst.address:
            return
        deadline = time.time() + timeout
        while time.time() < deadline:
            if dst.address not in src.neighbors and src.address not in dst.neighbors:
                return
            await asyncio.sleep(0.05)

    def _edge(self, edge_id: str) -> Optional[Edge]:
        for edge in self.edges:
            if edge.id == edge_id:
                return edge
        return None

    async def _nudge_reconnect(self, edge: Edge) -> None:
        """Some transports do not re-dial after a hard close; restart the ends."""
        await asyncio.sleep(3.0)
        src = self._node(edge.source)
        dst = self._node(edge.target)
        if not src or not dst or edge.state == "cut":
            return
        healthy = (
            dst.address in src.neighbors
            and src.address in dst.neighbors
            and src.running
            and dst.running
        )
        if healthy:
            return
        for node in (src, dst):
            if node.running:
                log.info("edge %s did not re-dial, restarting %s", edge.id, node.id)
                await self.stop_node(node.id)
                await self.start_node(node.id)

    # --------------------------------------------------------------- status
    def snapshot(self) -> dict:
        nodes = []
        for node in self.nodes:
            x, y = self._position(node.index)
            nodes.append(
                {
                    "id": node.id,
                    "address": node.address,
                    "short": node.short,
                    "api_port": node.api_port,
                    "peer_port": node.peer_port,
                    "running": node.running and (node.proc is not None and node.proc.returncode is None),
                    "connected": bool(node.link and node.link.connected),
                    "restarts": node.restarts,
                    "neighbors": [self.addr_to_node.get(a, a) for a in node.neighbors],
                    "position": {"x": round(x, 4), "y": round(y, 4)},
                    "metrics": node.metrics,
                }
            )
        return {
            "devnet": {
                "id": self.id,
                "topology": self.topology,
                "node_count": self.node_count,
                "host": self.host,
                "home": str(self.home),
                "started_at": self.started_at,
                "uptime_s": int(time.time() - self.started_at),
                "sdk": "offline-protocol-sdk",
            },
            "nodes": nodes,
            "edges": [{"id": e.id, "source": e.source, "target": e.target, "state": e.state} for e in self.edges],
            "messages": self.ledger.list_messages(120),
            "events": self.ledger.list_events(160),
            "stats": self.ledger.stats(),
        }

    # --------------------------------------------------------------- teardown
    async def stop(self) -> None:
        self._stopping = True
        if self._metrics_task:
            self._metrics_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._metrics_task
        for edge in self.edges:
            with contextlib.suppress(Exception):
                await edge.forward.stop()
            with contextlib.suppress(Exception):
                await edge.backward.stop()
        for node in self.nodes:
            await self.stop_node(node.id)
            if node.link:
                await node.link.close()
        for node in self.nodes:
            if node.proc and node.proc.returncode is None:
                with contextlib.suppress(Exception):
                    os.killpg(os.getpgid(node.proc.pid), signal.SIGKILL)


def _cos(x: float) -> float:
    import math

    return math.cos(x)


def _sin(x: float) -> float:
    import math

    return math.sin(x)
