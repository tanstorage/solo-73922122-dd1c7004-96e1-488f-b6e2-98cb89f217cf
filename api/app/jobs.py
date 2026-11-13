"""In-process coalescing extraction jobs.

Architecture
------------
One job per unique ``(recording_id, start_frame, end_frame)`` key. The single
runner performs the ETag-bound upstream read exactly once, writing every
chunk both to a UUID-named staging file and onto a fan-out relay. Every
joined HTTP request consumes the relay independently, so each client gets the
complete fragment even if others disconnect.

Reference-count model
---------------------
* ``waiters`` - HTTP clients still relying on the job (waiting or streaming).
  When it reaches zero while the remote read is still running, the read is
  cancelled. Only the last waiter leaving can trigger cancellation.
* ``readers`` - the runner's own reference while it writes the staging file.
  The file is unlinked only after the task truly finished and that reference
  is released; staging paths carry a UUID so concurrent replacement jobs can
  never delete each other's files.

Relay rules
-----------
* Every published chunk is buffered until the job finishes, so a response
  that subscribes after the first bytes (once header validation has cleared)
  still receives the complete fragment in order.
* FAILURE / CANCELLATION is stored on the relay and re-raised in every
  consumer; a partial WAV is never delivered.

A replacement job created while an old job is cleaning up gets a distinct
staging path; ``_cleanup`` unlinks only while its own job is still the one
registered for the key, so old cleanup can never remove a new job's file.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import AsyncIterator

logger = logging.getLogger("extractor.jobs")


class JobState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Relay:
    """Single-producer, multi-consumer chunk relay for one job.

    Published chunks are buffered until the job finishes, so a subscriber
    joining after the first bytes (e.g. once header validation completes)
    still receives the complete fragment in order.
    """

    queues: set[asyncio.Queue] = field(default_factory=set)
    buffer: list[bytes] = field(default_factory=list)
    done: bool = False
    error: BaseException | None = None

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=8)
        for historical in self.buffer:
            queue.put_nowait(historical)
        if self.done:
            queue.put_nowait(None)
            return queue
        self.queues.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self.queues.discard(queue)

    async def publish(self, chunk: bytes) -> None:
        self.buffer.append(chunk)
        # A slow/disappearing consumer must not stall the single upstream
        # read: bounded buffers that stay full get detached; their client
        # sees an incomplete response rather than holding everyone else back.
        for queue in list(self.queues):
            try:
                queue.put_nowait(chunk)
            except asyncio.QueueFull:
                self.queues.discard(queue)

    def finish(self, error: BaseException | None = None) -> None:
        self.done = True
        self.error = error
        for queue in list(self.queues):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)
        self.queues.clear()
        # The staging file is the durable copy after completion; the relay
        # buffer only has to cover in-flight subscribers.
        self.buffer.clear()


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
    # Set once headers have been validated upstream; waiters block on it so
    # validation/version errors are returned as proper HTTP statuses before
    # any response byte is streamed.
    started_event: asyncio.Event = field(default_factory=asyncio.Event)
    no_readers: asyncio.Condition = field(default_factory=asyncio.Condition)
    relay: Relay = field(default_factory=Relay)

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
        if job is not None:
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

    def leave(self, job: Job) -> None:
        """Release a waiter; cancel the read when the last client goes."""
        job.waiters -= 1
        if (
            job.waiters == 0
            and job.state == JobState.RUNNING
            and job.task is not None
            and not job.task.done()
        ):
            job.task.cancel()

    def mark_started(self, job: Job) -> None:
        """Called by the runner once upstream header validation succeeded."""
        job.started_event.set()

    def add_reader(self, job: Job) -> None:
        job.readers += 1

    async def release_reader(self, job: Job) -> None:
        job.readers -= 1
        if job.readers == 0:
            async with job.no_readers:
                job.no_readers.notify_all()

    def stream(self, job: Job) -> AsyncIterator[bytes]:
        """Return a relay iterator. Works for RUNNING jobs (live chunks) and
        SUCCEEDED jobs (replayed buffer) alike; callers must not call this for
        FAILED/CANCELLED jobs."""
        return _relay_iterator(job, job.relay.subscribe())

    async def _run(self, job: Job) -> None:
        self.add_reader(job)
        try:
            await self._runner(job, job.relay.publish, self.mark_started)
            job.state = JobState.SUCCEEDED
        except asyncio.CancelledError:
            job.state = JobState.CANCELLED
            job.relay.finish(asyncio.CancelledError())
            raise
        except BaseException as exc:  # noqa: BLE001 - surfaced to all waiters
            job.state = JobState.FAILED
            job.error = exc
            logger.warning("extraction %s failed: %s", job.key, exc)
            job.relay.finish(exc)
        else:
            job.relay.finish(None)
        finally:
            job.started_event.set()
            await self.release_reader(job)
            asyncio.create_task(self._cleanup(job), name="cleanup")

    async def _cleanup(self, job: Job) -> None:
        if job.task is not None and not job.task.done():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await job.task
        async with job.no_readers:
            await job.no_readers.wait_for(lambda: job.readers == 0)
        if self._jobs.get(job.key) is job:
            del self._jobs[job.key]
        try:
            os.unlink(job.path)
        except FileNotFoundError:
            pass
        except OSError as exc:  # pragma: no cover - defensive
            logger.error("could not remove staging file %s: %s", job.path, exc)


async def _relay_iterator(job: Job, queue: asyncio.Queue) -> AsyncIterator[bytes]:
    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                if job.relay.error is not None:
                    raise job.relay.error
                return
            yield chunk
    finally:
        job.relay.unsubscribe(queue)
