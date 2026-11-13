"""Coalescing under disconnect: one quitter among several waiters must not
cancel the shared read; every keeper receives the complete fragment.

Delivery model: the API finishes the single ETag-bound upstream read before
any response is sent. The quitter closes its connection while that read is
still running (during the wait). It receives no status line and no bytes, the
shared read keeps going for the keepers, and each keeper then downloads its
own complete, identical WAV from the verified staging file.
"""
from __future__ import annotations

import asyncio
import io
import os
import wave

import httpx

API = os.environ.get("API_URL", "http://127.0.0.1:8015")
SRC = os.environ.get("SOURCE_URL", "http://127.0.0.1:8092")
SPOOL = os.environ.get("SPOOL_DIR", "/tmp/birdtest/spool16")


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
        # Slow audio body so all four requests join the one running job and
        # the quitter can leave while the read is still in flight.
        await c.post(
            f"{SRC}/control/recordings/nightjar/behavior",
            json={"delay_seconds": 0.0, "cut_after_bytes": 0,
                  "chunk_delay_seconds": 0.15},
        )

        async def client(idx: int, quitter: bool) -> tuple[int, int, bytes]:
            try:
                async with c.stream(
                    "GET", f"{API}/recordings/nightjar/frames",
                    params={"start": 0, "end": 144000},  # 288000 audio bytes
                ) as r:
                    if quitter:
                        # Abandon while the buffered extraction is still
                        # reading; no status line should have arrived yet.
                        await asyncio.sleep(0.4)
                        await r.aclose()
                        return idx, -1, b"quit"
                    if r.status_code != 200:
                        return idx, r.status_code, b""
                    chunks = []
                    async for chunk in r.aiter_bytes():
                        chunks.append(chunk)
                    return idx, 200, b"".join(chunks)
            except (httpx.HTTPError, httpx.RemoteProtocolError):
                return idx, -2, b"aborted"

        results = await asyncio.gather(
            client(0, True),   # quits during the wait
            client(1, False),  # keeps
            client(2, False),  # keeps
            client(3, False),  # keeps
        )
        stats = (await c.get(f"{SRC}/stats")).json()
        await asyncio.sleep(0.5)
        leftovers = os.listdir(SPOOL)

    keepers = [r for r in results if r[0] != 0]
    keepers_ok = all(
        status == 200 and valid_wav(body, 144000, 48000) for _, status, body in keepers
    )
    identical = len({body for _, _, body in keepers}) == 1
    full_size = 44 + 144000 * 2
    # The shared read ran once to completion for the keepers: one audio range
    # request transferring the full body (headers are read separately).
    read_completed = stats["range_requests"] == 2 and stats["bytes_served"] >= full_size
    ok = keepers_ok and identical and read_completed and leftovers == []
    print("results:", [(i, s, len(b)) for i, s, b in results])
    print("source stats:", stats)
    print("spool leftovers:", leftovers)
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
