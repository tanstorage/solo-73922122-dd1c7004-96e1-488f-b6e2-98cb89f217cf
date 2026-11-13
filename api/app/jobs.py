"""In-process coalescing extraction jobs.

Architecture
------------
One job per unique ``(recording_id, start_frame, end_frame)`` key. The single
runner performs the ETag-bound upstream read exactly once and writes the
*complete* fragment to a UUID-named staging file. No HTTP response (and no WAV
byte) is sent to any client until the runner has finished reading and the
strong-ETag / exact-length checks have all passed. Every joined HTTP request
then streams its own independent copy out of the staging file, so each client
gets the complete fragment even if others disconnect.

Result delivery contract
------------------------
* A job reaches ``SUCCEEDED`` only after the whole requested byte range has
  been read and every chunk matched the bound strong ETag and the expected
  length. Only then does the endpoint return ``200``.
* A mid-stream cut / protocol error leaves the job ``FAILED`` and every
  waiter receives ``502`` before any byte is sent.
* A confirmed version change (412 / changed ETag / changed length) leaves the
  job ``FAILED`` with ``UpstreamVersionChanged`` and every waiter receives
  ``409`` before any byte is sent.
* On failure or cancellation the staging file is unlinked; no downloadable
  remnant ever exists. On success nothing is cached: once the last delivery
  ends the file is deleted and a later request starts a fresh upstream read.

Reference-count model
---------------------
* ``waiters`` - HTTP clients still relying on the job (header-waiting or
  downloading). One reference is taken at ``create_or_join`` and released
  exactly once per client: via ``leave`` on a pre-delivery failure /
  disconnect, or via ``end_delivery`` after a download. When the count
  reaches zero while the remote read is still running, the read is
  cancelled. Only the last waiter leaving can trigger cancellation; a single
  disconnect among several waiters never affects the others.
* ``readers`` - open references to the staging file: the runner while writing
  plus one per in-progress download. The file is unlinked only after the
  runner task truly finished and every reader has been released. Staging
  paths carry a UUID so a replacement job's file can never be removed by an
  older job's cleanup.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import uuid
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("extractor.jobs")

# How often the disconnect watcher re-checks the ASGI receive channel while a
# shared extraction is still running. Starlette's is_disconnected() resolves
# immediately (False while connected), so polling on a timer both avoids a
# busy loop and bounds how quickly a hang-up is noticed.
DISCONNECT_POLL_INTERVAL = 0.1


class JobState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Sentinel raised in an endpoint when its own client disconnects while it is
# still waiting for the shared extraction to finish.
class ClientGoneError(Exception):
    pass


@dataclass
class Job:
    key: tuple[str, int, int]
    path: str
    task: asyncio.Task | None = None
    state: JobState = JobState.RUNNING
    error: BaseException | None = None
    etag: str | None = None
    waiters: int = 0
    readers: int = 0
    # Set once the single upstream read has fully completed (success or
    # failure). Waiters block on it, so validation/version/stream errors are
    # returned as proper HTTP statuses before any response byte is produced.
    done_event: asyncio.Event = field(default_factory=asyncio.Event)
    # Notified whenever a waiter or reader reference is released; cleanup
    # waits on it until both counts reach zero.
    refs_released: asyncio.Condition = field(default_factory=asyncio.Condition)

    def snapshot(self) -> dict:
        return {
            "key": {
                "recording_id": self.key[0],
                "start_frame": self.key[1],
                "end_frame": self.key[2],
            },
            "state": self.state.value,
            "waiters": self.waiters,
            "readers": self.readers,
            "staging_path": self.path,
        }


class JobManager:
    def __init__(self, spool_dir: str, runner):
        self._jobs: dict[tuple[str, int, int], Job] = {}
        self._spool_dir = spool_dir
        self._runner = runner
        os.makedirs(spool_dir, exist_ok=True)

    def list_jobs(self) -> list[dict]:
        return [job.snapshot() for job in self._jobs.values()]

    def create_or_join(self, key: tuple[str, int, int]) -> tuple[Job, bool]:
        job = self._jobs.get(key)
        # Only live or completed-successful jobs can be shared. A failed /
        # cancelled job is being torn down, so a new request gets a brand new
        # job with its own UUID staging path.
        if job is not None and job.state in (JobState.RUNNING, JobState.SUCCEEDED):
            job.waiters += 1
            return job, True

        path = os.path.join(
            self._spool_dir,
            f"{key[0]}_{key[1]}_{key[2]}_{uuid.uuid4().hex}.wav.part",
        )
        job = Job(key=key, path=path)
        self._jobs[key] = job
        job.waiters = 1
        job.task = asyncio.create_task(
            self._run(job), name=f"extract:{key[0]}:{key[1]}-{key[2]}"
        )
        return job, False

    async def leave(self, job: Job) -> None:
        """Release a waiter before delivery starts; cancel the read when the
        last client goes away while the job is still running.

        The job stays registered until its staging file is actually removed
        (see ``_cleanup``): an in-flight cleanup must not unlink the file out
        from under clients that are still waiting or downloading.
        """
        job.waiters -= 1
        if job.waiters <= 0 and job.state == JobState.RUNNING:
            if job.task is not None and not job.task.done():
                job.task.cancel()
        async with job.refs_released:
            job.refs_released.notify_all()

    async def wait_until_done(self, job: Job, is_disconnected) -> None:
        """Wait for the single shared read to finish.

        The wait is disconnect-aware: if this client's connection drops while
        the header/body is still being read, its waiter reference is released
        immediately (cancelling the shared read only when it was the last
        waiter) and ``ClientGoneError`` is raised - before any response is
        produced.

        ``is_disconnected`` is Starlette's ``Request.is_disconnected``, which
        resolves immediately with ``False`` for a live connection, so it is
        driven through a bounded polling watcher rather than awaited directly.
        """
        async def watch_disconnect() -> None:
            while True:
                if await is_disconnected():
                    return
                await asyncio.sleep(DISCONNECT_POLL_INTERVAL)

        done_task = asyncio.create_task(job.done_event.wait())
        watcher = asyncio.create_task(watch_disconnect())
        try:
            await asyncio.wait({done_task, watcher}, return_when=asyncio.FIRST_COMPLETED)
            if watcher.done() and not done_task.done():
                # The watcher returns only on a real client hang-up.
                await self.leave(job)
                raise ClientGoneError(f"client disconnected while waiting on {job.key}")
        finally:
            done_task.cancel()
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await done_task
            with contextlib.suppress(asyncio.CancelledError):
                await watcher

    def begin_delivery(self, job: Job) -> None:
        """Take a staging-file reference for one response download.

        The client's waiter reference already exists from ``create_or_join``
        and is held across the whole wait + download lifecycle, so it is not
        touched here.
        """
        job.readers += 1

    async def end_delivery(self, job: Job) -> None:
        """Release one download and its waiter. The last completed delivery
        drops every client reference; the cleanup task then removes the
        staging file. Nothing is cached after completion."""
        job.waiters -= 1
        job.readers -= 1
        async with job.refs_released:
            job.refs_released.notify_all()

    def _detach_if_last(self, job: Job) -> None:
        """Unregister failed/cancelled jobs so new requests start fresh jobs
        (with distinct UUID staging paths) instead of joining a dead one."""
        if job.waiters <= 0 and self._jobs.get(job.key) is job:
            del self._jobs[job.key]

    async def _run(self, job: Job) -> None:
        job.readers += 1  # the runner's own reference to the staging file
        try:
            await self._runner(job)
            job.state = JobState.SUCCEEDED
        except asyncio.CancelledError:
            job.state = JobState.CANCELLED
            raise
        except BaseException as exc:  # noqa: BLE001 - surfaced to all waiters
            job.state = JobState.FAILED
            job.error = exc
            logger.warning("extraction %s failed: %s", job.key, exc)
        finally:
            job.done_event.set()
            job.readers -= 1
            async with job.refs_released:
                job.refs_released.notify_all()
            # A failed/cancelled job must not accept new waiters; detach it
            # immediately (its file is removed by the cleanup task below).
            if job.state != JobState.SUCCEEDED:
                self._detach_if_last(job)
            asyncio.create_task(self._cleanup(job), name="cleanup")

    async def _cleanup(self, job: Job) -> None:
        # Wait until the runner task itself has truly ended.
        if job.task is not None and not job.task.done():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await job.task
        # The staging file may be removed only once nothing references it:
        # the runner has finished (readers excludes it) and every client has
        # left its wait or finished its download (waiters == 0, readers == 0).
        async with job.refs_released:
            await job.refs_released.wait_for(
                lambda: job.waiters == 0 and job.readers == 0
            )
        # Only ever unlink our own path; a replacement job registered under
        # the same key owns a distinct UUID file and is never touched here.
        if self._jobs.get(job.key) is job:
            del self._jobs[job.key]
        try:
            os.unlink(job.path)
        except FileNotFoundError:
            pass
        except OSError as exc:  # pragma: no cover - defensive
            logger.error("could not remove staging file %s: %s", job.path, exc)
