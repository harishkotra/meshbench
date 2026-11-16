"""Minimal JSON-RPC client for the Offline Protocol local API (WebSocket)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any, Awaitable, Callable, Optional

import websockets

log = logging.getLogger("meshbench.localapi")


class ApiError(RuntimeError):
    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(f"{message} (code {code})")
        self.code = code
        self.data = data


class NodeLink:
    """Maintains one authenticated local-API connection to a mesh service.

    Reconnects with backoff and re-sends ``hello``; events are funnelled to
    ``on_event`` and call replies are resolved by request id.
    """

    def __init__(
        self,
        node_id: str,
        port: int,
        token_path: str,
        on_event: Callable[[dict], None],
        on_status: Callable[[bool], None] | None = None,
        app_id: str = "meshbench",
        host: str = "127.0.0.1",
    ) -> None:
        self.node_id = node_id
        self.port = port
        self.token_path = token_path
        self.on_event = on_event
        self.on_status = on_status or (lambda _up: None)
        self.app_id = app_id
        self.host = host
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._task: Optional[asyncio.Task] = None
        self.address: Optional[str] = None
        self.connected = False
        self._closing = False
        self._hello = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"link-{self.node_id}")

    async def close(self) -> None:
        self._closing = True
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._ws.close(), 2)
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task

    def expect_hello(self) -> None:
        """Clear the hello latch so a restarted service must re-handshake."""
        self._hello.clear()

    async def wait_hello(self, timeout: float = 30.0) -> bool:
        try:
            await asyncio.wait_for(self._hello.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _token(self) -> str | None:
        try:
            with open(self.token_path) as fh:
                return fh.read().strip()
        except OSError:
            return None

    async def _run(self) -> None:
        backoff = 0.25
        while not self._closing:
            token = self._token()
            if not token:
                await asyncio.sleep(0.2)
                continue
            try:
                ws = await websockets.connect(
                    f"ws://{self.host}:{self.port}/",
                    max_size=16 << 20,
                    ping_interval=None,
                    open_timeout=10,
                )
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.6, 5.0)
                continue
            self._ws = ws
            reader: Optional[asyncio.Task] = None
            try:
                reader = asyncio.create_task(self._read_loop(ws), name=f"read-{self.node_id}")
                await self._hello_exchange(ws, token)
                backoff = 0.25
                self.connected = True
                self.on_status(True)
                await reader
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - transport errors are expected
                log.debug("%s link dropped: %s", self.node_id, exc)
            finally:
                if reader is not None:
                    reader.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await reader
                self.connected = False
                self.on_status(False)
                self._ws = None
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(ConnectionError("local API connection lost"))
                self._pending.clear()
                with contextlib.suppress(Exception):
                    await ws.close()
            await asyncio.sleep(0.4)

    async def _read_loop(self, ws) -> None:
        try:
            async for raw in ws:
                self._dispatch(raw)
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def _hello_exchange(self, ws, token: str) -> None:
        reply = await self._request(ws, "hello", {"app_id": self.app_id, "token": token}, timeout=15)
        self.address = reply.get("local_address") or self.address
        self._hello.set()

    def _dispatch(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(msg, dict):
            return
        if "id" in msg and msg["id"] in self._pending:
            fut = self._pending.pop(msg["id"])
            if fut.done():
                return
            if msg.get("error"):
                err = msg["error"] or {}
                fut.set_exception(ApiError(err.get("code", -32000), err.get("message", "error"), err.get("data")))
            else:
                fut.set_result(msg.get("result"))
            return
        event = msg.get("event") or msg.get("method")
        params = msg.get("params") if isinstance(msg.get("params"), dict) else msg
        if event:
            payload = dict(params or {})
            payload.setdefault("type", event)
            self.on_event(payload)

    async def _request(self, ws, method: str, params: dict, timeout: float) -> Any:
        self._next_id += 1
        rid = self._next_id
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}))
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(rid, None)

    async def call(self, method: str, params: dict | None = None, timeout: float = 25.0) -> Any:
        ws = self._ws
        if ws is None:
            raise ConnectionError(f"{self.node_id}: local API not connected")
        return await self._request(ws, method, params or {}, timeout)

    async def subscribe(self, types: list[str] | None = None) -> None:
        with contextlib.suppress(Exception):
            await self.call("subscribe", {"types": types or "all"}, timeout=10)
