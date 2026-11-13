"""Stdlib-only behavioral verification of the new JobManager contract.

Runs without FastAPI/httpx: a fake runner emulates the extraction semantics
(slow full read, mid-stream cut, version change, cancellation) and fake
clients emulate waiting, disconnecting and downloading the staging file.
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from app.jobs import ClientGoneError, JobManager, JobState  # noqa: E402

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name} {detail}")


class FakeError(Exception):
    pass


class VersionChanged(FakeError):
    pass


class StreamCut(FakeError):
    pass


class FakeRunner:
    """Writes a deterministic complete fragment; supports failure injection."""

    def __init__(self) -> None:
        self.runs = 0
        self.cancelled_runs = 0
        self.mode: dict[tuple, str] = {}
        self.mode_event: dict[tuple, asyncio.Event] = {}
        self.read_bytes = 0

    def set_mode(self, key, mode: str) -> None:
        self.mode[key] = mode

    async def __call__(self, job) -> None:
        key = job.key
        self.runs += 1
        gate = self.mode_event.setdefault(key, asyncio.Event())
        try:
            # Slow start: lets multiple clients join and lets tests disconnect
            # a client during the waiting (pre-delivery) phase.
            await asyncio.sleep(0.15)
            mode = self.mode.get(key, "ok")
            if mode == "cut":
                with open(job.path, "wb") as fh:
                    fh.write(b"RIFF-partial")  # remnant must be cleaned
                raise StreamCut("simulated mid-stream cut")
            if mode == "version":
                raise VersionChanged("etag changed mid read")

            total = 1024
            written = 0
            with open(job.path, "wb") as fh:
                fh.write(b"HDR")
                while written < total:
                    await asyncio.sleep(0.01)  # slow, cancellable read
                    block = b"x" * 128
                    fh.write(block)
                    written += len(block)
                    self.read_bytes += len(block)
            gate.set()
        except asyncio.CancelledError:
            self.cancelled_runs += 1
            raise


async def download(job, manager, *, disconnect_after: int | None = None) -> bytes:
    manager.begin_delivery(job)
    got = bytearray()
    try:
        with open(job.path, "rb") as fh:
            while True:
                chunk = fh.read(64)
                if not chunk:
                    break
                got += chunk
                if disconnect_after is not None and len(got) >= disconnect_after:
                    raise asyncio.CancelledError("client hung up mid download")
                await asyncio.sleep(0)
        return bytes(got)
    finally:
        await manager.end_delivery(job)


async def client(manager, runner, key, spool, *, disconnect_while_waiting=False,
                 disconnect_in_download=False) -> tuple[str, object, bytes]:
    """Mirror the endpoint lifecycle: join -> wait -> map error -> download."""
    job, joined = manager.create_or_join(key)
    disconnected = asyncio.Event()
    if disconnect_while_waiting:
        async def flag():
            await asyncio.sleep(0.05)
            disconnected.set()
        asyncio.create_task(flag())

    async def is_disconnected() -> bool:
        await disconnected.wait()
        return True

    try:
        await manager.wait_until_done(job, is_disconnected)
    except ClientGoneError:
        return "gone", None, b""

    if job.state == JobState.FAILED:
        await manager.leave(job)
        err = job.error
        if isinstance(err, VersionChanged):
            return "409", err, b""
        if isinstance(err, StreamCut):
            return "502", err, b""
        return "502?", err, b""
    if job.state != JobState.SUCCEEDED:
        await manager.leave(job)
        return "503", job.state, b""

    try:
        body = await download(
            job, manager, disconnect_after=128 if disconnect_in_download else None
        )
    except asyncio.CancelledError:
        return "download-gone", None, b""
    return "200", joined, body


async def main() -> int:
    spool = tempfile.mkdtemp(prefix="jmtest-")
    runner = FakeRunner()
    manager = JobManager(spool_dir=spool, runner=runner)

    # 1. coalescing: 5 identical concurrent requests -> one runner run, all
    #    receive identical complete fragments.
    key = ("nightjar", 0, 100)
    results5 = await asyncio.gather(*(client(manager, runner, key, spool) for _ in range(5)))
    bodies = {r[2] for r in results5}
    check(
        "1. five concurrent identical requests share one run and get full WAV",
        runner.runs == 1
        and all(r[0] == "200" for r in results5)
        and len(bodies) == 1
        and len(next(iter(bodies))) == 3 + 1024,
        f"runs={runner.runs}, kinds={[r[0] for r in results5]}",
    )
    await asyncio.sleep(0.1)
    check("1b. spool empty and job retired after all downloads end",
          os.listdir(spool) == [] and manager.list_jobs() == [], str(os.listdir(spool)))

    # 2. no caching: a later identical request starts a new run (re-read).
    before = runner.runs
    r = await client(manager, runner, key, spool)
    check("2. completed fragments are not cached; serial request re-runs read",
          r[0] == "200" and runner.runs == before + 1, f"runs={runner.runs}")
    await asyncio.sleep(0.1)
    check("2b. spool clean again", os.listdir(spool) == [], str(os.listdir(spool)))

    # 3. mid-stream cut: all waiters fail with 502, no 200/body, no remnant.
    cut_key = ("rail", 0, 50)
    runner.set_mode(cut_key, "cut")
    cut_res = await asyncio.gather(*(client(manager, runner, cut_key, spool)
                                     for _ in range(3)))
    await asyncio.sleep(0.1)
    check(
        "3. mid-stream cut -> 502 for every waiter, no WAV bytes, no remnant",
        all(r[0] == "502" for r in cut_res)
        and all(r[2] == b"" for r in cut_res)
        and not any("rail" in f for f in os.listdir(spool))
        and not any(j["key"]["recording_id"] == "rail" for j in manager.list_jobs()),
        f"kinds={[r[0] for r in cut_res]}, spool={os.listdir(spool)}",
    )

    # 4. confirmed version change -> 409 for every waiter, no remnant.
    ver_key = ("warbler", 0, 40)
    runner.set_mode(ver_key, "version")
    ver_res = await asyncio.gather(*(client(manager, runner, ver_key, spool)
                                     for _ in range(3)))
    await asyncio.sleep(0.1)
    check(
        "4. confirmed version change -> 409 for every waiter, no remnant",
        all(r[0] == "409" for r in ver_res)
        and not any("warbler" in f for f in os.listdir(spool)),
        f"kinds={[r[0] for r in ver_res]}, spool={os.listdir(spool)}",
    )

    # 5. one waiter disconnects during the wait; others still get full WAV.
    coop_key = ("sparrow", 0, 70)
    coop = await asyncio.gather(
        client(manager, runner, coop_key, spool, disconnect_while_waiting=True),
        client(manager, runner, coop_key, spool),
        client(manager, runner, coop_key, spool),
    )
    kinds = [r[0] for r in coop]
    check(
        "5. one disconnect while waiting does not hurt other waiters",
        kinds.count("gone") == 1 and kinds.count("200") == 2
        and all(len(r[2]) == 3 + 1024 for r in coop if r[0] == "200"),
        f"kinds={kinds}",
    )
    await asyncio.sleep(0.1)
    check("5b. spool clean after keepers finish", os.listdir(spool) == [],
          str(os.listdir(spool)))

    # 6. last (sole) waiter disconnecting while waiting cancels the read.
    sole_key = ("cuckoo", 0, 10)
    cancelled_before = runner.cancelled_runs
    sole = await client(manager, runner, sole_key, spool,
                        disconnect_while_waiting=True)
    await asyncio.sleep(0.2)
    check(
        "6. last waiter leaving during wait cancels the shared read",
        sole[0] == "gone" and runner.cancelled_runs == cancelled_before + 1,
        f"kind={sole[0]}, cancelled_runs={runner.cancelled_runs}",
    )
    check("6b. cancelled run leaves no staging file and no job",
          os.listdir(spool) == [] and manager.list_jobs() == [], str(os.listdir(spool)))

    # 7. a disconnect during the download phase does not cancel anything;
    #    the other downloader still gets the full fragment; file cleaned after.
    dl_key = ("finch", 0, 20)
    dl = await asyncio.gather(
        client(manager, runner, dl_key, spool, disconnect_in_download=True),
        client(manager, runner, dl_key, spool),
    )
    dl_kinds = [r[0] for r in dl]
    await asyncio.sleep(0.1)
    check(
        "7. mid-download disconnect leaves the other downloader intact",
        dl_kinds.count("download-gone") == 1
        and dl_kinds.count("200") == 1
        and len(dl[1][2]) == 3 + 1024
        and os.listdir(spool) == [],
        f"kinds={dl_kinds}, spool={os.listdir(spool)}",
    )

    # 8. a late request shares the succeeded job's staging file; when both
    #    downloads end the job retires and the file is unlinked exactly once.
    rep_key = ("heron", 0, 5)
    j1, _ = manager.create_or_join(rep_key)
    await manager.wait_until_done(j1, lambda: _never())
    j2, joined2 = manager.create_or_join(rep_key)  # j1 still registered -> share
    await manager.wait_until_done(j2, lambda: _never())
    check("8. late join shares the succeeded job", joined2 and j2 is j1, "")
    # Each of the two clients holds one waiter ref; downloads add reader refs.
    manager.begin_delivery(j1)
    manager.begin_delivery(j2)
    await manager.end_delivery(j1)
    await manager.end_delivery(j2)
    await asyncio.sleep(0.15)
    check("8b. everything retired and cleaned", os.listdir(spool) == []
          and manager.list_jobs() == [], str(os.listdir(spool)))

    failed = [n for n, ok, _ in results if not ok]
    print("\n=== summary ===")
    for n, ok, d in results:
        print(f"[{'OK ' if ok else 'BAD'}] {n} {d}")
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


async def _never() -> bool:
    await asyncio.Event().wait()
    return False  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
