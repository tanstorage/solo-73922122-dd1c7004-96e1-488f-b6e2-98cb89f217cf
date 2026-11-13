"""Focused check: the last waiter's disconnect cancels the remote read,
and the source actually observes an aborted connection (no full download).

Uses the source's per-chunk delay so response headers arrive immediately but
the audio body streams slowly, giving the client time to disconnect.
"""
from __future__ import annotations

import asyncio
import os

import httpx

API = os.environ.get("API_URL", "http://127.0.0.1:8008")
SRC = os.environ.get("SOURCE_URL", "http://127.0.0.1:8090")
SPOOL = os.environ.get("SPOOL_DIR", "/tmp/birdtest/spool9")


async def main() -> int:
    async with httpx.AsyncClient(timeout=30.0) as c:
        await c.post(f"{SRC}/control/reset", json={})
        await c.post(f"{SRC}/control/stats/reset", json={})
        # Headers return at once; each 8 KiB audio block then takes 1 s, so
        # the body streams slowly enough to abandon it mid-flight.
        await c.post(
            f"{SRC}/control/recordings/rail/behavior",
            json={"delay_seconds": 0.0, "cut_after_bytes": 0,
                  "chunk_delay_seconds": 1.0},
        )

        async def quit_early() -> None:
            async with c.stream(
                "GET", f"{API}/recordings/rail/frames",
                params={"start": 0, "end": 110250},  # 220500 bytes, ~27 blocks
            ) as r:
                assert r.status_code == 200, r.status_code
                # Read one chunk then abandon the sole (last) waiter.
                first = await r.aiter_bytes().__anext__()
                assert first[:4] == b"RIFF"
                await r.aclose()

        await asyncio.wait_for(quit_early(), timeout=45)
        # Let cancellation propagate to the upstream connection and cleanup run.
        await asyncio.sleep(1.5)

        jobs = (await c.get(f"{API}/jobs")).json()["jobs"]
        stats = (await c.get(f"{SRC}/stats")).json()
        leftovers = [f for f in os.listdir(SPOOL) if "rail" in f]

        print("jobs after last-waiter disconnect (empty = fully cleaned up):", jobs)
        print("source stats:", stats)
        print("spool leftovers:", leftovers)

        full_size = 44 + 110250 * 2
        ok = (
            all(j["state"] == "cancelled" for j in jobs if j["key"]["recording_id"] == "rail")
            and leftovers == []
            # The remote read was aborted well before the full body transferred.
            and 0 < stats["bytes_served"] < full_size
        )
        print("RESULT:", "PASS" if ok else "FAIL")

        await c.post(f"{SRC}/control/reset", json={})
        return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
