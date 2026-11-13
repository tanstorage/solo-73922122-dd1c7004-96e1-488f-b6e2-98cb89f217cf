"""Focused check: the last waiter's disconnect cancels the remote read,
and the source actually observes an aborted connection (no full download).

Delivery model: the API buffers the whole verified fragment before sending a
response, so a client that hangs up while the read is still running never
receives a status line or any WAV byte. A large per-chunk delay makes the
audio read slow enough to abandon it in flight; the client closes the socket
before the extraction completes, the shared read is cancelled, and the
staging file is removed.
"""
from __future__ import annotations

import asyncio
import contextlib
import os

import httpx

API = os.environ.get("API_URL", "http://127.0.0.1:8008")
SRC = os.environ.get("SOURCE_URL", "http://127.0.0.1:8090")
SPOOL = os.environ.get("SPOOL_DIR", "/tmp/birdtest/spool9")


async def main() -> int:
    async with httpx.AsyncClient(timeout=30.0) as c:
        await c.post(f"{SRC}/control/reset", json={})
        await c.post(f"{SRC}/control/stats/reset", json={})
        # Audio body streams from the source in slow 8 KiB blocks, so the
        # buffered extraction takes far longer than the client's patience.
        await c.post(
            f"{SRC}/control/recordings/rail/behavior",
            json={"delay_seconds": 0.0, "cut_after_bytes": 0,
                  "chunk_delay_seconds": 1.0},
        )

        got_status: int | None = None
        got_bytes = 0

        async def quit_early() -> None:
            nonlocal got_status, got_bytes
            try:
                async with c.stream(
                    "GET", f"{API}/recordings/rail/frames",
                    params={"start": 0, "end": 110250},  # 220500 bytes of audio
                ) as r:
                    got_status = r.status_code
                    async for chunk in r.aiter_bytes():
                        got_bytes += len(chunk)
            except (httpx.ReadError, httpx.RemoteProtocolError,
                    httpx.ConnectError, httpx.ReadTimeout):
                # Expected: the client gives up before any response is sent.
                pass

        task = asyncio.create_task(quit_early())
        await asyncio.sleep(3.0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        # Let cancellation propagate to the upstream connection and cleanup run.
        await asyncio.sleep(1.5)

        jobs = (await c.get(f"{API}/jobs")).json()["jobs"]
        stats = (await c.get(f"{SRC}/stats")).json()
        leftovers = [f for f in os.listdir(SPOOL) if "rail" in f]

        print("status seen by client (None = no response at all):", got_status)
        print("bytes received by client:", got_bytes)
        print("jobs after last-waiter disconnect (empty = fully cleaned up):", jobs)
        print("source stats:", stats)
        print("spool leftovers:", leftovers)

        full_size = 44 + 110250 * 2
        ok = (
            got_status is None
            and got_bytes == 0
            and not any(j["key"]["recording_id"] == "rail" for j in jobs)
            and leftovers == []
            # The remote read was aborted well before the full body transferred.
            and 0 < stats["bytes_served"] < full_size
        )
        print("RESULT:", "PASS" if ok else "FAIL")

        await c.post(f"{SRC}/control/reset", json={})
        return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
