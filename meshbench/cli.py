"""meshbench CLI: up / down / status / send / demo / mcp."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from . import __version__
from .control import ControlServer
from .devnet import DevNet

DEFAULT_HOME = ".meshbench"


# ---------------------------------------------------------------- helpers
def home_path(value: Optional[str]) -> Path:
    return Path(value or os.environ.get("MESHBENCH_HOME") or DEFAULT_HOME).expanduser().resolve()


def runtime_path(home: Path) -> Path:
    return home / "runtime.json"


def read_runtime(home: Path) -> Optional[dict]:
    try:
        return json.loads(runtime_path(home).read_text())
    except (OSError, ValueError):
        return None


def write_runtime(home: Path, data: dict) -> None:
    runtime_path(home).write_text(json.dumps(data, indent=2))


def api(method: str, url: str, body: dict | None = None, timeout: float = 30) -> dict:
    import urllib.error
    import urllib.request

    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            raw = res.read().decode() or "{}"
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"{exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"control plane unreachable at {url} ({exc.reason})") from exc


def running_base(home: Path) -> str:
    info = read_runtime(home)
    if not info:
        raise SystemExit(f"no devnet running in {home} (run `meshbench up`)")
    url = info.get("url", "")
    try:
        api("GET", f"{url}/api/health", timeout=3)
    except RuntimeError as exc:
        raise SystemExit(f"devnet {info.get('devnet_id')} is not answering ({exc}); run `meshbench down`")
    return url


def banner(snapshot: dict, url: str, home: Path) -> None:
    dev = snapshot["devnet"]
    print()
    print(f"  meshbench {__version__}   devnet {dev['id']}   {dev['topology']} · {dev['node_count']} nodes")
    print("  " + "-" * 62)
    for node in snapshot["nodes"]:
        state = "up" if node["running"] else "down"
        print(f"  {node['id']:<2} {node['address'] or '(no address)':<58} {state}")
    print("  " + "-" * 62)
    print(f"  dashboard  {url}")
    print(f"  home       {home}")
    print(f"  mcp        meshbench mcp --home {home}")
    print()


# ------------------------------------------------------------------- up
async def _serve(args, home: Path) -> int:
    devnet = DevNet(
        home=home,
        nodes=args.nodes,
        topology=args.topology,
        host=args.host,
        keep_home=not args.fresh,
    )
    control = ControlServer(devnet, host=args.host, port=args.port)
    url = await control.start()
    try:
        await devnet.start()
        snap = devnet.snapshot()
        snap["devnet"]["sdk_version"] = devnet.sdk_version
        write_runtime(
            home,
            {
                "url": url,
                "pid": os.getpid(),
                "devnet_id": devnet.id,
                "topology": devnet.topology,
                "nodes": devnet.node_count,
                "home": str(home),
                "started_at": devnet.started_at,
                "sdk_version": devnet.sdk_version,
            },
        )
        banner(snap, url, home)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, control.shutdown_event.set)
            except NotImplementedError:
                pass
        await control.serve_until_shutdown()
        return 0
    finally:
        print("  stopping nodes…", flush=True)
        await devnet.stop()
        await control.stop()
        try:
            runtime_path(home).unlink()
        except OSError:
            pass


def cmd_up(args) -> int:
    home = home_path(args.home)
    if args.fresh and home.exists():
        for node in (home / "nodes").glob("*"):
            if (node / "service.pid").exists():
                pass
        shutil.rmtree(home, ignore_errors=True)
    home.mkdir(parents=True, exist_ok=True)
    if not args.detach:
        return asyncio.run(_serve(args, home))

    if read_runtime(home) and _healthy(home):
        raise SystemExit(f"a devnet is already running in {home}; run `meshbench down` first")
    cmd = [sys.executable, "-m", "meshbench", "up", "--foreground",
           "--nodes", str(args.nodes), "--topology", args.topology,
           "--host", args.host, "--port", str(args.port), "--home", str(home)]
    logf = open(home / "control.log", "ab", buffering=0)
    proc = subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True, cwd=os.getcwd())
    deadline = time.time() + 60
    while time.time() < deadline:
        info = read_runtime(home)
        if info and _healthy(home):
            snap = api("GET", f"{info['url']}/api/state")
            snap["devnet"]["sdk_version"] = info.get("sdk_version", "")
            banner(snap, info["url"], home)
            return 0
        if proc.poll() is not None:
            tail = (home / "control.log").read_text(errors="replace").splitlines()[-20:]
            raise SystemExit("devnet failed to start:\n" + "\n".join(tail))
        time.sleep(0.4)
    raise SystemExit("devnet did not come up in 60s; see .meshbench/control.log")


def _healthy(home: Path) -> bool:
    info = read_runtime(home)
    if not info:
        return False
    try:
        api("GET", f"{info['url']}/api/health", timeout=2)
        return True
    except RuntimeError:
        return False


# ----------------------------------------------------------------- down
def cmd_down(args) -> int:
    home = home_path(args.home)
    info = read_runtime(home)
    if not info:
        print(f"no devnet recorded in {home}")
        return 0
    try:
        api("POST", f"{info['url']}/api/shutdown", {}, timeout=5)
    except RuntimeError as exc:
        print(f"control plane already stopped ({exc})")
    deadline = time.time() + 20
    while time.time() < deadline:
        if not _healthy(home):
            break
        time.sleep(0.3)
    try:
        runtime_path(home).unlink()
    except OSError:
        pass
    print(f"devnet {info.get('devnet_id')} stopped")
    return 0


# --------------------------------------------------------------- status
def cmd_status(args) -> int:
    home = home_path(args.home)
    base = running_base(home)
    snap = api("GET", f"{base}/api/state")
    if args.json:
        print(json.dumps(snap, indent=2, default=str))
        return 0
    dev = snap["devnet"]
    print(f"devnet {dev['id']}  {dev['topology']} · {dev['node_count']} nodes · up {dev['uptime_s']}s")
    for n in snap["nodes"]:
        m = n.get("metrics") or {}
        print(f"  {n['id']}  {'up  ' if n['running'] else 'down'}  {n['address'] or '-'}")
        print(f"      sessions={m.get('sessions', 0)} neighbors={len(n['neighbors'])} "
              f"retry_queue={m.get('retry_queue', 0)} relays={(m.get('relay') or {}).get('forwarded', 0)}")
    for e in snap["edges"]:
        print(f"  link {e['source']} ↔ {e['target']}  {e['state']}")
    s = snap["stats"]
    print(f"  messages={s['messages']} delivered={s['delivered']} deferred={s['deferred']} "
          f"max_hops={s['max_hops']} median_rtt={s['median_latency_ms']}ms")
    return 0


# ----------------------------------------------------------------- send
def cmd_send(args) -> int:
    home = home_path(args.home)
    base = running_base(home)
    res = api("POST", f"{base}/api/send",
              {"from": args.from_node, "to": args.to, "content": args.content, "priority": args.priority})
    print(f"sent {res['from']} → {res['to']}  id={res['message_id']}")
    if args.wait:
        row = wait_for_message(base, res["message_id"], args.timeout)
        print(format_message(row))
        return 0 if row.get("state") in {"delivered", "received"} else 1
    return 0


def wait_for_message(base: str, message_id: str, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    row: dict = {}
    while time.time() < deadline:
        row = api("GET", f"{base}/api/messages/{message_id}")
        if row.get("state") in {"delivered", "received", "failed"}:
            break
        time.sleep(0.25)
    return row


def format_message(row: dict) -> str:
    hops = row.get("hops", 0)
    via = f" via {' → '.join(row['relays'])}" if row.get("relays") else ""
    lat = row.get("latency_ms")
    return (f"{row.get('from_node')} → {row.get('to_node')}   "
            f"{'direct' if hops == 0 else f'{hops} hop' + ('' if hops == 1 else 's')}{via}   "
            f"{str(lat) + ' ms' if lat else '—'}   state={row.get('state')}")


# ----------------------------------------------------------------- demo
async def _demo(home: Path, nodes: int, topology: str, content: str, timeout: float) -> int:
    devnet = DevNet(home=home, nodes=nodes, topology=topology)
    control = ControlServer(devnet, host="127.0.0.1", port=0)
    await control.start()
    await devnet.start()
    try:
        src, dst = devnet.nodes[0], devnet.nodes[-1]
        started = time.time()
        print(f"\n  mesh benchmark · {topology} · {nodes} nodes")
        print("  " + "-" * 62)
        print(f"  {src.id} → {dst.id}  {src.address}  ⇒  {dst.address}")
        res = await devnet.send(src.id, dst.id, content, "High")
        row = {}
        while time.time() - started < timeout:
            row = devnet.ledger.get(res["message_id"] or "") or {}
            if row.get("state") in {"delivered", "received", "failed"}:
                break
            await asyncio.sleep(0.1)
        print("  " + "-" * 62)
        print("  " + format_message(row))
        ok = row.get("state") == "received" or (row.get("state") == "delivered" and nodes == 2)
        print(f"  protocol delivered : {row.get('state') in {'delivered', 'received'}}")
        print(f"  app accepted       : {row.get('state') == 'received'}")
        print(f"  relay path         : {' → '.join([src.id] + (row.get('relays') or []) + [dst.id])}")
        print(f"  verdict            : {'PASS' if ok else 'FAIL'}")
        print()
        return 0 if ok else 1
    finally:
        await devnet.stop()
        await control.stop()


def cmd_demo(args) -> int:
    home = home_path(args.home)
    return asyncio.run(_demo(home, args.nodes, args.topology, args.content, args.timeout))


# ------------------------------------------------------------------ mcp
def cmd_mcp(args) -> int:
    from .mcp import serve

    return serve(home_path(args.home))


# ---------------------------------------------------------------- doctor
def cmd_doctor(args) -> int:
    from .devnet import find_service

    exe = find_service()
    print(f"meshbench      {__version__}")
    print(f"python         {sys.version.split()[0]}")
    print(f"service binary {exe or 'NOT FOUND'}")
    if not exe:
        print("\ninstall the SDK:\n  pip install 'offline-protocol-sdk[lan,http]==0.33.0'")
        return 1
    try:
        from importlib.metadata import version

        print(f"sdk version     {version('offline-protocol-sdk')}")
    except Exception as exc:  # noqa: BLE001
        print(f"sdk version     unknown ({exc})")
    home = home_path(args.home)
    print(f"home           {home}  {'present' if home.exists() else 'absent'}")
    info = read_runtime(home)
    print(f"devnet         {info['devnet_id'] + ' at ' + info['url'] if info else 'not running'}")
    return 0


# ------------------------------------------------------------------ main
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="meshbench", description="Local devnet for the Offline Protocol Mesh SDK")
    p.add_argument("--version", action="version", version=f"meshbench {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    up = sub.add_parser("up", help="start a local devnet and dashboard")
    up.add_argument("--nodes", type=int, default=3)
    up.add_argument("--topology", default="chain", choices=["chain", "ring", "star", "full"])
    up.add_argument("--host", default="127.0.0.1")
    up.add_argument("--port", type=int, default=8787)
    up.add_argument("--home", default=None)
    up.add_argument("--detach", action="store_true")
    up.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    up.add_argument("--fresh", action="store_true", help="wipe node state first")
    up.set_defaults(func=cmd_up)

    down = sub.add_parser("down", help="stop the devnet")
    down.add_argument("--home", default=None)
    down.set_defaults(func=cmd_down)

    st = sub.add_parser("status", help="inspect a running devnet")
    st.add_argument("--home", default=None)
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_status)

    send = sub.add_parser("send", help="send a message across the mesh")
    send.add_argument("content", nargs="?", default="hello from meshbench")
    send.add_argument("--from", dest="from_node", default="A")
    send.add_argument("--to", default=None)
    send.add_argument("--priority", default="Medium", choices=["Low", "Medium", "High", "Critical"])
    send.add_argument("--home", default=None)
    send.add_argument("--wait", action="store_true", default=True)
    send.add_argument("--timeout", type=float, default=20)
    send.set_defaults(func=cmd_send)

    demo = sub.add_parser("demo", help="boot a devnet, prove a relayed delivery, exit")
    demo.add_argument("--nodes", type=int, default=3)
    demo.add_argument("--topology", default="chain", choices=["chain", "ring", "star", "full"])
    demo.add_argument("--content", default="meshbench self-test")
    demo.add_argument("--timeout", type=float, default=30)
    demo.add_argument("--home", default=None)
    demo.set_defaults(func=cmd_demo)

    mcp = sub.add_parser("mcp", help="run the MCP server on stdio")
    mcp.add_argument("--home", default=None)
    mcp.set_defaults(func=cmd_mcp)

    doc = sub.add_parser("doctor", help="check the environment")
    doc.add_argument("--home", default=None)
    doc.set_defaults(func=cmd_doctor)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "send" and not args.to:
        args.to = "B"
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
