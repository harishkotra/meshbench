"""Controllable TCP proxy used to cut and restore links between virtual nodes.

Each directed edge (A -> B) gets its own proxy: node A is configured to dial the
proxy, the proxy dials B. Cutting the proxy closes every live connection and
refuses new ones, which produces a real partition without restarting a node.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Optional

log = logging.getLogger("meshbench.proxy")


class LinkProxy:
    def __init__(self, listen_port: int, target_port: int, host: str = "127.0.0.1") -> None:
        self.listen_port = listen_port
        self.target_port = target_port
        self.host = host
        self._server: Optional[asyncio.AbstractServer] = None
        self._conns: set[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = set()
        self._handlers: set[asyncio.Task] = set()
        self.cut_state = False

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._spawn, self.host, self.listen_port, reuse_address=True, backlog=64
        )
        self.cut_state = False

    def _spawn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._handle(reader, writer))
        self._handlers.add(task)
        task.add_done_callback(self._handlers.discard)

    async def cut(self) -> None:
        """Close every live connection hard and stop accepting new ones."""
        self.cut_state = True
        # Abort before closing the listener: since 3.12 `wait_closed()` waits on
        # live handlers, so closing the server first deadlocks against its own pumps.
        for _reader, writer in list(self._conns):
            with contextlib.suppress(Exception):
                writer.transport.abort()
        self._conns.clear()
        for task in list(self._handlers):
            task.cancel()
        if self._handlers:
            await asyncio.gather(*list(self._handlers), return_exceptions=True)
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), 2)
            self._server = None

    async def restore(self) -> None:
        if self._server is None:
            await asyncio.sleep(0.05)
            await self.start()

    async def stop(self) -> None:
        await self.cut()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            target_reader, target_writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.target_port), timeout=5
            )
        except Exception:
            with contextlib.suppress(Exception):
                writer.close()
            return
        pair = (reader, writer)
        self._conns.add(pair)
        try:
            await asyncio.gather(
                self._pump(reader, target_writer),
                self._pump(target_reader, writer),
            )
        finally:
            self._conns.discard(pair)
            for w in (writer, target_writer):
                with contextlib.suppress(Exception):
                    w.close()

    async def _pump(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (asyncio.CancelledError, Exception):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()
