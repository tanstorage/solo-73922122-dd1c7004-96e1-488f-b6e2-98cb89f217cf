"""Coalescing under disconnect: one quitter among several waiters must not
cancel the shared read; every keeper receives the complete fragment.
"""
from __future__ import annotations

import asyncio
import io
import os
import wave

import httpx

API = os.environ.get("API_URL", "http://127.0.0.1:8015")
SRC = os.environ.get("SOURCE_URL", "http://127.0.0.1:8092")


def valid_wav(data: bytes, frames: int, rate: int) -> bool:
    try:
        with wave.open(io.BytesIO(data), "rb") as w:
            return (
                w.getnframes() == frames
                and w.getframerate() == rate
                and w.getsampwidth() == 2
            )
    except wave.Error:
        return False


async def main() -> int:
    async with httpx.AsyncClient(timeout=60.0) as c:
        await c.post(f"{SRC}/control/reset", json={})
        await c.post(f"{SRC}/control/stats/reset", json={})
        # Slow audio body so clients connect and are mid-stream when one quits.
        await c.post(
            f"{SRC}/control/recordings/nightjar/behavior",
            json={"delay_seconds": 0.0, "cut_after_bytes": 0,
                  "chunk_delay_seconds": 0.15},
        )

        async def client(idx: int, quit_after_first: bool) -> tuple[int, int, bytes]:
            try:
                async with c.stream(
                    "GET", f"{API}/recordings/nightjar/frames",
                    params={"start": 0, "end": 144000},  # 288000 bytes
                ) as r:
                    if r.status_code != 200:
                        return idx, r.status_code, b""
                    chunks = []
                    async for chunk in r.aiter_bytes():
                        chunks.append(chunk)
                        if quit_after_first:
                            await r.aclose()
                            return idx, -1, b"quit"
                    return idx, 200, b"".join(chunks)
            except httpx.HTTPError as exc:
                return idx, -2, str(exc).encode()

        results = await asyncio.gather(
            client(0, True),   # quits after the first chunk
            client(1, False),  # keeps
            client(2, False),  # keeps
            client(3, False),  # keeps
        )
        stats = (await c.get(f"{SRC}/stats")).json()
        spool = os.environ.get("SPOOL_DIR", "/tmp/birdtest/spool16")
        await asyncio.sleep(0.5)
        leftovers = os.listdir(spool)

    keepers = [r for r in results if r[0] != 0]
    keepers_ok = all(
        status == 200 and valid_wav(body, 144000, 48000) for _, status, body in keepers
    )
    identical = len({body for _, _, body in keepers}) == 1
    full_size = 44 + 144000 * 2
    # The shared read must have run to completion for the keepers (one audio
    # range request transferring the full body, headers read separately).
    read_completed = stats["range_requests"] == 2 and stats["bytes_served"] >= full_size
    ok = keepers_ok and identical and read_completed and leftovers == []
    print("results:", [(i, s, len(b)) for i, s, b in results])
    print("source stats:", stats)
    print("spool leftovers:", leftovers)
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
