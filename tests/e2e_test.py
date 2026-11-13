"""End-to-end behavior tests for the WAV extractor.

Runs against a live pair of services (mock source + API) started locally or
via docker compose. Pure stdlib + the project venv's httpx.

Scenarios
---------
1. Basic extraction produces a valid PCM16 WAV of exactly the asked frames.
2. Concurrent identical requests share one upstream read (source stats).
3. One of N concurrent clients disconnecting does not hurt the others; the
   shared read is cancelled only when the last waiter leaves.
4. A mid-flight source version change (ETag) fails the whole request; no
   half WAV is returned.
5. A mid-stream cut fails the whole extraction.
6. Unsupported WAVs (float32, two data chunks) are rejected.
7. No fragment caching: repeated identical extractions re-read the source.
8. Different fragments run independently.
9. Staging files are always cleaned up; cleanup of an old job never removes
   a new job's file.
"""
from __future__ import annotations

import asyncio
import io
import os
import wave

import httpx
API = os.environ.get("API_URL", "http://127.0.0.1:8006")
SRC = os.environ.get("SOURCE_URL", "http://127.0.0.1:8080")

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name} {detail}")


async def source_post(path: str, payload: dict) -> dict:
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SRC}{path}", json=payload)
        r.raise_for_status()
        return r.json()


async def source_get(path: str) -> dict:
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SRC}{path}")
        r.raise_for_status()
        return r.json()


async def reset_all() -> None:
    await source_post("/control/reset", {})
    await source_post("/control/stats/reset", {})


def assert_wav(data: bytes, frames: int, rate: int = 48000, channels: int = 1) -> None:
    with wave.open(io.BytesIO(data), "rb") as w:
        assert w.getnframes() == frames, (w.getnframes(), frames)
        assert w.getframerate() == rate
        assert w.getnchannels() == channels
        assert w.getsampwidth() == 2
    assert len(data) == 44 + frames * channels * 2


async def scenario_1_basic() -> None:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(
            f"{API}/recordings/nightjar/frames",
            params={"start": 48000, "end": 96000},
        )
        ok = r.status_code == 200
        detail = f"status={r.status_code}"
        if ok:
            try:
                assert_wav(r.content, 48000)
                detail += f", etag={r.headers['x-source-etag']}, bytes={len(r.content)}"
            except AssertionError as exc:
                ok = False
                detail += f", {exc}"
        check("1. basic extraction returns valid exact-length PCM16 WAV", ok, detail)


async def scenario_2_coalescing() -> None:
    await reset_all()
    # Slow source so several requests overlap on the same running job.
    await source_post(
        "/control/recordings/nightjar/behavior",
        {"delay_seconds": 1.5, "cut_after_bytes": 0},
    )

    async with httpx.AsyncClient(timeout=30) as c:
        async def one(i: int) -> tuple[int, bytes, str]:
            r = await c.get(
                f"{API}/recordings/nightjar/frames",
                params={"start": 1000, "end": 25000},
            )
            return r.status_code, r.content, r.headers.get("x-coalesced", "")

        responses = await asyncio.gather(*(one(i) for i in range(5)))

    ok = all(status == 200 for status, _, _ in responses)
    same = len({body for _, body, _ in responses}) == 1
    coalesced = [flag for _, _, flag in responses]
    stats = await source_get("/stats")
    # Header inspect requests + exactly one audio range request: the shared
    # job performs the reads once for all five clients.
    check(
        "2. five concurrent identical requests share one extraction",
        ok and same and coalesced.count("0") == 1 and coalesced.count("1") == 4,
        f"coalesced-flags={coalesced}, identical={same}",
    )
    check(
        "2b. source received only one audio range read for the five clients",
        stats["range_requests"] == 2,  # one header fetch + one audio stream
        f"range_requests={stats['range_requests']}, stats={stats}",
    )
    await reset_all()


async def scenario_3_disconnects() -> None:
    await reset_all()
    await source_post(
        "/control/recordings/warbler/behavior",
        {"delay_seconds": 3.0, "cut_after_bytes": 0},
    )

    async with httpx.AsyncClient(timeout=30) as c:
        async def slow_client(keep: bool, tag: str) -> tuple[str, int, bytes]:
            try:
                async with c.stream(
                    "GET",
                    f"{API}/recordings/warbler/frames",
                    params={"start": 0, "end": 44100},
                ) as r:
                    if not keep:
                        # Abort before the delayed upstream read completes.
                        await r.aclose()
                        return tag, -1, b""
                    chunks = []
                    async for chunk in r.aiter_bytes():
                        chunks.append(chunk)
                    return tag, r.status_code, b"".join(chunks)
            except (httpx.ReadError, httpx.RemoteProtocolError,
                    httpx.ReadTimeout, httpx.ConnectError) as exc:
                return tag, -2, str(exc).encode()

        res = await asyncio.gather(
            slow_client(False, "quitter-a"),
            slow_client(False, "quitter-b"),
            slow_client(True, "keeper"),
        )

    keeper = next(r for r in res if r[0] == "keeper")
    ok = keeper[1] == 200
    detail = f"keeper status={keeper[1]}, len={len(keeper[2])}"
    if ok:
        try:
            assert_wav(keeper[2], 44100, rate=44100)
        except AssertionError as exc:
            ok = False
            detail += f", {exc}"
    check("3. two clients disconnecting does not break the remaining waiter", ok, detail)

    # Now: last waiter leaves -> remote read must be cancelled and spool cleaned.
    spool = os.environ.get("SPOOL_DIR", "/tmp/birdtest/spool7")
    await asyncio.sleep(0.5)
    leftovers = [f for f in os.listdir(spool) if "warbler" in f]
    check("3b. cancelled job leaves no staging file behind", leftovers == [], str(leftovers))
    await reset_all()


async def scenario_4_version_change() -> None:
    await reset_all()

    async with httpx.AsyncClient(timeout=30) as c:
        # Slow *audio body* (headers still validate promptly), so the version
        # switch lands while audio bytes are streaming.
        await source_post(
            "/control/recordings/nightjar/behavior",
            {"delay_seconds": 0.0, "cut_after_bytes": 0,
             "chunk_delay_seconds": 0.7},
        )

        async def nightjar_extract() -> tuple[str, int, bytes]:
            try:
                r = await c.get(
                    f"{API}/recordings/nightjar/frames",
                    params={"start": 100, "end": 48000},
                )
                return "status", r.status_code, r.content
            except httpx.HTTPError as exc:
                # Headers were already sent, so an in-flight version change
                # must abort the body rather than deliver a half WAV.
                return "aborted", -1, str(exc).encode()

        nj_task = asyncio.create_task(nightjar_extract())
        await asyncio.sleep(1.2)
        switched = await source_post(
            "/control/recordings/nightjar/version", {"version": "v2"}
        )
        kind, status, body = await nj_task

    no_half_wav = True
    if kind == "status" and status == 200:
        try:
            assert_wav(body, 47900)
            no_half_wav = False  # a 200 full WAV after a swap is wrong
        except Exception:
            no_half_wav = True
    ok = (
        switched["active_version"] == "v2"
        and no_half_wav
        and (status == 409 or kind == "aborted")
    )
    check(
        "4. mid-flight ETag change fails whole extraction, no partial WAV",
        ok,
        f"kind={kind}, status={status}, etag now={switched['etag']}, no_half_wav={no_half_wav}",
    )
    await reset_all()


async def scenario_5_midstream_cut() -> None:
    await reset_all()
    # Cut shortly into the audio body -> truncated stream after headers.
    await source_post(
        "/control/recordings/nightjar/behavior",
        {"delay_seconds": 0.0, "cut_after_bytes": 200,
         "chunk_delay_seconds": 0.2},
    )
    kind = "status"
    status = 0
    body = b""
    async with httpx.AsyncClient(timeout=30) as c:
        try:
            r = await c.get(
                f"{API}/recordings/nightjar/frames",
                params={"start": 0, "end": 96000},
            )
            status, body = r.status_code, r.content
        except httpx.HTTPError as exc:
            kind, status = "aborted", -1
            body = str(exc).encode()
    # Either an explicit 502 (failure before streaming) or a clean connection
    # abort (failure mid-stream); never a complete WAV.
    delivered_full_wav = False
    if status == 200:
        try:
            assert_wav(body, 96000)
            delivered_full_wav = True
        except Exception:
            delivered_full_wav = False
    ok = not delivered_full_wav and (status in (502, 503) or kind == "aborted")
    check(
        "5. mid-stream connection cut fails the whole request, no half WAV",
        ok,
        f"kind={kind}, status={status}, detail={body[:100]!r}",
    )
    await reset_all()


async def scenario_6_unsupported() -> None:
    async with httpx.AsyncClient(timeout=30) as c:
        r1 = await c.get(
            f"{API}/recordings/float32/frames",
            params={"start": 0, "end": 1000},
        )
        r2 = await c.get(
            f"{API}/recordings/two_data_chunks/frames",
            params={"start": 0, "end": 1000},
        )
    check(
        "6. 32-bit float and multi-data-chunk WAVs are rejected (415)",
        r1.status_code == 415 and r2.status_code == 415,
        f"float32={r1.status_code}, two_data={r2.status_code}",
    )


async def scenario_7_no_cache() -> None:
    await reset_all()
    async with httpx.AsyncClient(timeout=30) as c:
        for _ in range(3):
            r = await c.get(
                f"{API}/recordings/sparrow/frames",
                params={"start": 0, "end": 12000},
            )
            assert r.status_code == 200
    stats = await source_get("/stats")
    # Each serial extraction re-reads: 2 range requests per run (header+audio).
    check(
        "7. completed fragments are not cached; each request re-reads source",
        stats["range_requests"] == 6,
        f"range_requests={stats['range_requests']}",
    )
    await reset_all()


async def scenario_8_independent_fragments() -> None:
    await reset_all()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await asyncio.gather(
            c.get(f"{API}/recordings/nightjar/frames",
                  params={"start": 0, "end": 24000}),
            c.get(f"{API}/recordings/nightjar/frames",
                  params={"start": 24000, "end": 48000}),
            c.get(f"{API}/recordings/warbler/frames",
                  params={"start": 0, "end": 22050}),
        )
    ok = all(x.status_code == 200 for x in r)
    try:
        assert_wav(r[0].content, 24000)
        assert_wav(r[1].content, 24000)
        assert_wav(r[2].content, 22050, rate=44100)
        distinct = len({x.content for x in r}) == 3
    except AssertionError:
        ok, distinct = False, False
    stats = await source_get("/stats")
    check(
        "8. different fragments/recordings proceed independently (3 jobs, 6 reads)",
        ok and distinct and stats["range_requests"] == 6,
        f"statuses={[x.status_code for x in r]}, ranges={stats['range_requests']}",
    )
    await reset_all()


async def scenario_9_cleanup_safety() -> None:
    spool = os.environ.get("SPOOL_DIR", "/tmp/birdtest/spool7")
    await reset_all()
    await source_post(
        "/control/recordings/nightjar/behavior",
        {"delay_seconds": 1.5, "cut_after_bytes": 0},
    )

    async with httpx.AsyncClient(timeout=30) as c:
        async def grab_and_quit() -> None:
            async with c.stream(
                "GET",
                f"{API}/recordings/nightjar/frames",
                params={"start": 500, "end": 5000},
            ) as r:
                assert r.status_code == 200
                await r.aclose()  # last waiter -> cancel, then cleanup runs

        await grab_and_quit()
        # Immediately start a new job for the same key while cleanup is pending.
        r = await c.get(
            f"{API}/recordings/nightjar/frames",
            params={"start": 500, "end": 5000},
        )
        ok_status = r.status_code
        try:
            assert_wav(r.content, 4500)
            valid = True
        except AssertionError:
            valid = False

    await asyncio.sleep(1.0)
    leftovers = os.listdir(spool)
    check(
        "9. new request during cleanup succeeds and old cleanup never deletes new file",
        ok_status == 200 and valid and leftovers == [],
        f"status={ok_status}, valid={valid}, spool_left={leftovers}",
    )
    await reset_all()


async def scenario_10_validation() -> None:
    async with httpx.AsyncClient(timeout=30) as c:
        bad_range = await c.get(
            f"{API}/recordings/nightjar/frames",
            params={"start": 100, "end": 50},
        )
        missing = await c.get(
            f"{API}/recordings/ghost-bird/frames",
            params={"start": 0, "end": 100},
        )
        too_long = await c.get(
            f"{API}/recordings/nightjar/frames",
            params={"start": 0, "end": 10_000_000},
        )
    check(
        "10. validation: reversed range 422, missing recording 404, oversize 422",
        bad_range.status_code == 422
        and missing.status_code == 404
        and too_long.status_code == 422,
        f"statuses=({bad_range.status_code},{missing.status_code},{too_long.status_code})",
    )


async def main() -> int:
    print(f"API={API}  SOURCE={SRC}\n")
    await scenario_1_basic()
    await scenario_2_coalescing()
    await scenario_3_disconnects()
    await scenario_4_version_change()
    await scenario_5_midstream_cut()
    await scenario_6_unsupported()
    await scenario_7_no_cache()
    await scenario_8_independent_fragments()
    await scenario_9_cleanup_safety()
    await scenario_10_validation()

    print("\n=== summary ===")
    failed = [name for name, ok, _ in results if not ok]
    for name, ok, detail in results:
        print(f"[{'OK ' if ok else 'BAD'}] {name} {detail}")
    print(f"\n{len(results) - len(failed)}/{len(results)} scenarios passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
