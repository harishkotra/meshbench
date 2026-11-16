"""Control plane: JSON API, live SSE stream and the dashboard it serves."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from pathlib import Path
from typing import Optional

from aiohttp import web

from .devnet import DevNet

log = logging.getLogger("meshbench.control")

WEB_DIR = Path(__file__).parent / "web"


class ControlServer:
    def __init__(self, devnet: DevNet, host: str = "127.0.0.1", port: int = 8787) -> None:
        self.devnet = devnet
        self.host = host
        self.port = port
        self.app = web.Application()
        self._clients: set[asyncio.Queue] = set()
        self._dirty = asyncio.Event()
        self._broadcast_task: Optional[asyncio.Task] = None
        self._runner: Optional[web.AppRunner] = None
        self._last_event_seq = 0
        self.shutdown_event = asyncio.Event()
        self._wire()

    # ----------------------------------------------------------------- html
    def _wire(self) -> None:
        r = self.app.router
        r.add_get("/", self.index)
        r.add_get("/api/state", self.state)
        r.add_get("/api/stream", self.stream)
        r.add_get("/api/health", self.health)
        r.add_get("/api/messages/{mid}", self.message)
        r.add_get("/api/logs/{node}", self.node_log)
        r.add_post("/api/send", self.send)
        r.add_post("/api/nodes/{node}/stop", self.node_stop)
        r.add_post("/api/nodes/{node}/start", self.node_start)
        r.add_post("/api/edges/{edge}/cut", self.edge_cut)
        r.add_post("/api/edges/{edge}/restore", self.edge_restore)
        r.add_post("/api/shutdown", self.shutdown)

    async def index(self, _request: web.Request) -> web.Response:
        return web.FileResponse(WEB_DIR / "index.html")

    async def state(self, _request: web.Request) -> web.Response:
        return web.json_response(self.devnet.snapshot())

    async def health(self, _request: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "devnet": self.devnet.id,
                "nodes": len(self.devnet.nodes),
                "port": self.port,
                "pid": __import__("os").getpid(),
            }
        )

    async def message(self, request: web.Request) -> web.Response:
        row = self.devnet.ledger.get(request.match_info["mid"])
        if not row:
            raise web.HTTPNotFound(text="unknown message")
        return web.json_response(row)

    async def node_log(self, request: web.Request) -> web.Response:
        node = self.devnet._node(request.match_info["node"])
        if not node or not node.log_path:
            raise web.HTTPNotFound(text="unknown node")
        try:
            lines = node.log_path.read_text(errors="replace").splitlines()[-120:]
        except OSError:
            lines = []
        return web.json_response({"node": node.id, "lines": lines})

    # -------------------------------------------------------------- commands
    async def send(self, request: web.Request) -> web.Response:
        body = await request.json()
        try:
            result = await self.devnet.send(
                body.get("from", "A"),
                body.get("to", "B"),
                body.get("content", "hello"),
                body.get("priority", "Medium"),
            )
        except Exception as exc:  # noqa: BLE001 - surfaced to the dashboard
            return web.json_response({"error": str(exc)}, status=400)
        return web.json_response(result)

    async def node_stop(self, request: web.Request) -> web.Response:
        try:
            return web.json_response(await self.devnet.stop_node(request.match_info["node"]))
        except Exception as exc:  # noqa: BLE001
            return web.json_response({"error": str(exc)}, status=400)

    async def node_start(self, request: web.Request) -> web.Response:
        try:
            return web.json_response(await self.devnet.start_node(request.match_info["node"]))
        except Exception as exc:  # noqa: BLE001
            return web.json_response({"error": str(exc)}, status=400)

    async def edge_cut(self, request: web.Request) -> web.Response:
        try:
            return web.json_response(await self.devnet.set_edge(request.match_info["edge"], "cut"))
        except Exception as exc:  # noqa: BLE001
            return web.json_response({"error": str(exc)}, status=400)

    async def edge_restore(self, request: web.Request) -> web.Response:
        try:
            return web.json_response(await self.devnet.set_edge(request.match_info["edge"], "up"))
        except Exception as exc:  # noqa: BLE001
            return web.json_response({"error": str(exc)}, status=400)

    async def shutdown(self, _request: web.Request) -> web.Response:
        self.shutdown_event.set()
        return web.json_response({"ok": True, "stopping": True})

    # ------------------------------------------------------------------ sse
    async def stream(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._clients.add(queue)
        try:
            await queue.put(("snapshot", self.devnet.snapshot()))
            await queue.put(("hello", {"devnet": self.devnet.id}))
            while True:
                try:
                    kind, payload = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    await response.write(b": ping\n\n")
                    continue
                body = json.dumps(payload, default=str)
                await response.write(f"event: {kind}\ndata: {body}\n\n".encode())
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
            pass
        finally:
            self._clients.discard(queue)
        return response

    def mark_dirty(self) -> None:
        self._dirty.set()

    async def _broadcast(self) -> None:
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            if not self._clients:
                await asyncio.sleep(0.2)
                continue
            snapshot = self.devnet.snapshot()
            events = [e for e in self.devnet.ledger.events if e["seq"] > self._last_event_seq]
            if events:
                self._last_event_seq = events[-1]["seq"]
            for queue in list(self._clients):
                with contextlib.suppress(asyncio.QueueFull):
                    queue.put_nowait(("snapshot", snapshot))
                    for event in events:
                        if event["type"].startswith("message_"):
                            queue.put_nowait(("packet", event))
            await asyncio.sleep(0.12)

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> str:
        self._runner = web.AppRunner(self.app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        if self.port == 0:
            for site_obj in self._runner.addresses:
                self.port = site_obj[1]
                break
        self.devnet.on_change = self.mark_dirty
        self._broadcast_task = asyncio.create_task(self._broadcast(), name="broadcast")
        return f"http://{self.host}:{self.port}"

    async def stop(self) -> None:
        if self._broadcast_task:
            self._broadcast_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._broadcast_task
        if self._runner:
            with contextlib.suppress(Exception):
                await self._runner.cleanup()

    async def serve_until_shutdown(self) -> None:
        await self.shutdown_event.wait()
