"""MCP server (stdio) that exposes a live mesh to coding agents.

The point: an agent can scaffold an Offline Protocol app from the docs, then use
this server to actually run a mesh and verify delivery — including hop counts,
relay path and the application-acceptance level the protocol separates from
transport delivery.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

from . import __version__
from .cli import read_runtime, runtime_path

SERVER_NAME = "meshbench"
PROTOCOL_DEFAULT = "2025-06-18"

TOOLS = [
    {
        "name": "mesh_status",
        "description": "Snapshot of the running devnet: nodes, addresses, link states, message ledger stats.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "mesh_peers",
        "description": "List mesh peers with full off1 addresses, liveness and discovered neighbors.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "mesh_send",
        "description": "Send a message between two devnet nodes and wait for the delivery evidence "
                       "(hop count, relay path, latency, protocol-delivered vs app-accepted).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "from": {"type": "string", "description": "sender node id, e.g. A"},
                "to": {"type": "string", "description": "recipient node id, e.g. C (or a full off1 address)"},
                "content": {"type": "string"},
                "priority": {"type": "string", "enum": ["Low", "Medium", "High", "Critical"]},
                "timeout_s": {"type": "number", "default": 20},
            },
            "required": ["to"],
        },
    },
    {
        "name": "mesh_inbox",
        "description": "Messages a node accepted (application-level receipt), newest first.",
        "inputSchema": {
            "type": "object",
            "properties": {"node": {"type": "string", "description": "node id; omit for all nodes"},
                           "limit": {"type": "number", "default": 20}},
        },
    },
    {
        "name": "mesh_fault",
        "description": "Inject a fault into the live mesh: cut or restore a link, stop or start a node.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["cut", "restore", "stop", "start"]},
                "edge": {"type": "string", "description": "edge id such as A-B (required for cut/restore)"},
                "node": {"type": "string", "description": "node id (required for stop/start)"},
            },
            "required": ["action"],
        },
    },
    {
        "name": "mesh_scenario",
        "description": "End-to-end proof: start a devnet if needed, send from the first node to the last, "
                       "and return delivery evidence. Use this to verify an integration without hardware.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "nodes": {"type": "number", "default": 3},
                "topology": {"type": "string", "enum": ["chain", "ring", "star", "full"], "default": "chain"},
                "content": {"type": "string", "default": "scenario probe"},
            },
        },
    },
]


class Rpc:
    def __init__(self, home: Path) -> None:
        self.home = home

    # ------------------------------------------------------------- transport
    def base(self) -> Optional[str]:
        info = read_runtime(self.home)
        if not info:
            return None
        try:
            self._http("GET", f"{info['url']}/api/health", timeout=2)
            return info["url"]
        except Exception:  # noqa: BLE001
            return None

    def _http(self, method: str, url: str, body: dict | None = None, timeout: float = 30) -> dict:
        import urllib.error
        import urllib.request

        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                return json.loads(res.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(exc.read().decode(errors="replace") or str(exc.code)) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"devnet unreachable: {exc.reason}") from exc

    def require(self) -> str:
        base = self.base()
        if not base:
            raise RuntimeError(
                "no devnet is running. Start one with `meshbench up --detach` "
                "or call mesh_scenario to boot one automatically."
            )
        return base

    # ---------------------------------------------------------------- tools
    def tool_status(self, _args: dict) -> dict:
        base = self.require()
        snap = self._http("GET", f"{base}/api/state")
        snap.pop("events", None)
        snap["messages"] = snap.get("messages", [])[:10]
        return snap

    def tool_peers(self, _args: dict) -> dict:
        base = self.require()
        snap = self._http("GET", f"{base}/api/state")
        return {
            "nodes": [
                {
                    "id": n["id"],
                    "address": n["address"],
                    "running": n["running"],
                    "neighbors": n["neighbors"],
                    "sessions": (n.get("metrics") or {}).get("sessions", 0),
                }
                for n in snap["nodes"]
            ],
            "edges": snap["edges"],
        }

    def tool_send(self, args: dict) -> dict:
        base = self.require()
        src = args.get("from", "A")
        dst = args.get("to")
        if not dst:
            raise ValueError("`to` is required")
        sent = self._http(
            "POST",
            f"{base}/api/send",
            {
                "from": src,
                "to": dst,
                "content": args.get("content", "probe"),
                "priority": args.get("priority", "Medium"),
            },
        )
        deadline = time.time() + float(args.get("timeout_s", 20))
        row: dict = {}
        while time.time() < deadline:
            row = self._http("GET", f"{base}/api/messages/{sent['message_id']}")
            if row.get("state") in {"delivered", "received", "failed"}:
                break
            time.sleep(0.2)
        return {
            "message_id": sent["message_id"],
            "from": row.get("from_node"),
            "to": row.get("to_node"),
            "state": row.get("state"),
            "protocol_delivered": row.get("state") in {"delivered", "received"},
            "app_accepted": row.get("state") == "received",
            "hops": row.get("hops", 0),
            "relay_path": [row.get("from_node")] + (row.get("relays") or []) + [row.get("to_node")],
            "latency_ms": row.get("latency_ms", 0),
            "transport": row.get("transport", ""),
            "content": row.get("content", ""),
        }

    def tool_inbox(self, args: dict) -> dict:
        base = self.require()
        snap = self._http("GET", f"{base}/api/state")
        node = args.get("node")
        limit = int(args.get("limit", 20))
        rows = [
            m
            for m in snap.get("messages", [])
            if m.get("received_at") and (not node or m.get("to_node") == node)
        ][:limit]
        return {"node": node or "all", "count": len(rows), "messages": rows}

    def tool_fault(self, args: dict) -> dict:
        base = self.require()
        action = args.get("action")
        if action in {"cut", "restore"}:
            edge = args.get("edge")
            if not edge:
                raise ValueError("`edge` is required for cut/restore")
            return self._http("POST", f"{base}/api/edges/{edge}/{action}", {})
        if action in {"stop", "start"}:
            node = args.get("node")
            if not node:
                raise ValueError("`node` is required for stop/start")
            return self._http("POST", f"{base}/api/nodes/{node}/{action}", {})
        raise ValueError(f"unknown action {action}")

    def tool_scenario(self, args: dict) -> dict:
        base = self.base()
        if not base:
            base = self._boot(int(args.get("nodes", 3)), str(args.get("topology", "chain")))
        snap = self._http("GET", f"{base}/api/state")
        nodes = snap["nodes"]
        sent = self.tool_send(
            {
                "from": nodes[0]["id"],
                "to": nodes[-1]["id"],
                "content": args.get("content", "scenario probe"),
                "priority": "High",
                "timeout_s": 25,
            }
        )
        return {
            "devnet": {"id": snap["devnet"]["id"], "topology": snap["devnet"]["topology"],
                       "nodes": len(nodes), "url": base},
            "evidence": sent,
            "pass": sent["app_accepted"] or (sent["protocol_delivered"] and len(nodes) == 2),
        }

    def _boot(self, nodes: int, topology: str) -> str:
        port = _free_port()
        cmd = [
            sys.executable, "-m", "meshbench", "up", "--detach",
            "--nodes", str(nodes), "--topology", topology,
            "--port", str(port), "--home", str(self.home),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if proc.returncode != 0:
            raise RuntimeError(
                "could not start devnet:\n" + (proc.stderr or proc.stdout or "").strip()[-1200:]
            )
        base = self.base()
        if not base:
            raise RuntimeError("devnet started but is not answering")
        return base


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _reply(rid: Any, result: Any = None, error: dict | None = None) -> dict:
    msg: dict = {"jsonrpc": "2.0", "id": rid}
    if error:
        msg["error"] = error
    else:
        msg["result"] = result
    return msg


def _text(payload: Any) -> dict:
    body = payload if isinstance(payload, str) else json.dumps(payload, indent=2, default=str)
    return {"content": [{"type": "text", "text": body}]}


def serve(home: Path) -> int:
    rpc = Rpc(home)
    out = sys.stdout
    out.reconfigure(line_buffering=True) if hasattr(out, "reconfigure") else None

    def send(msg: dict) -> None:
        out.write(json.dumps(msg, default=str) + "\n")
        out.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except ValueError:
            continue
        method = req.get("method")
        rid = req.get("id")
        params = req.get("params") or {}
        if rid is None and method and method.startswith("notifications/"):
            continue
        try:
            if method == "initialize":
                send(_reply(rid, {
                    "protocolVersion": params.get("protocolVersion") or PROTOCOL_DEFAULT,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": SERVER_NAME, "version": __version__},
                }))
            elif method == "notifications/initialized":
                continue
            elif method == "ping":
                send(_reply(rid, {}))
            elif method == "tools/list":
                send(_reply(rid, {"tools": TOOLS}))
            elif method == "tools/call":
                name = params.get("name")
                args = params.get("arguments") or {}
                handler = {
                    "mesh_status": rpc.tool_status,
                    "mesh_peers": rpc.tool_peers,
                    "mesh_send": rpc.tool_send,
                    "mesh_inbox": rpc.tool_inbox,
                    "mesh_fault": rpc.tool_fault,
                    "mesh_scenario": rpc.tool_scenario,
                }.get(name)
                if not handler:
                    send(_reply(rid, _text(f"unknown tool {name}")))
                    continue
                try:
                    send(_reply(rid, _text(handler(args))))
                except Exception as exc:  # noqa: BLE001 - tool failures are data
                    send(_reply(rid, {"content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}],
                                     "isError": True}))
            elif method in {"resources/list", "prompts/list"}:
                send(_reply(rid, {"resources": []} if method.startswith("resources") else {"prompts": []}))
            else:
                if rid is not None:
                    send(_reply(rid, error={"code": -32601, "message": f"method not found: {method}"}))
        except Exception as exc:  # noqa: BLE001
            if rid is not None:
                send(_reply(rid, error={"code": -32603, "message": str(exc)}))
    return 0
