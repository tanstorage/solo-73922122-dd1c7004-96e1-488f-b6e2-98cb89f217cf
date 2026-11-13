"""WAV fragment extraction API.

Endpoints
---------
GET /health                                              liveness
GET /recordings/{recording_id}/info                      header/metadata probe
GET /recordings/{recording_id}/frames?start=&end=        fragment extraction
GET /jobs                                                internal coalescing state
GET /docs                                                interactive demo
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import re

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from .config import load_settings
from .extractor import run_extraction
from .jobs import JobManager, JobState
from .upstream import (
    RemoteSource,
    UpstreamError,
    UpstreamNotFound,
    UpstreamUnavailable,
    UpstreamVersionChanged,
)
from .wav import WavError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("extractor.api")

RECORDING_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
CHUNK_SIZE = 256 * 1024

settings = load_settings()
manager: JobManager | None = None
client: httpx.AsyncClient | None = None


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global manager, client
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.request_timeout),
        follow_redirects=False,
    )
    manager = JobManager(
        spool_dir=settings.spool_dir,
        runner=lambda job, publish, mark_started: run_extraction(
            job, client, settings.source_base_url, publish, mark_started
        ),
    )
    logger.info("using source %s, spool %s", settings.source_base_url, settings.spool_dir)
    try:
        yield
    finally:
        await client.aclose()


app = FastAPI(
    title="Birdcall WAV Fragment Extractor",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Extract 16-bit PCM WAV fragments from a fixed remote recording store "
        "using HTTP Range requests. Concurrent identical requests share one "
        "upstream read, bound to a single strong ETag."
    ),
)


def _validate(recording_id: str, start: int, end: int) -> None:
    if not RECORDING_ID_RE.match(recording_id):
        raise HTTPException(status_code=400, detail="invalid recording id")
    if start < 0:
        raise HTTPException(status_code=422, detail="start_frame must be >= 0")
    if end <= start:
        raise HTTPException(status_code=422, detail="end_frame must be > start_frame")
    if end - start > settings.max_frames:
        raise HTTPException(
            status_code=422,
            detail=f"fragment too large: max {settings.max_frames} frames per request",
        )


def _map_upstream(exc: BaseException) -> HTTPException:
    if isinstance(exc, UpstreamVersionChanged):
        return HTTPException(status_code=409, detail=f"source version changed: {exc}")
    if isinstance(exc, UpstreamNotFound):
        return HTTPException(status_code=404, detail=f"not found: {exc}")
    if isinstance(exc, UpstreamUnavailable):
        return HTTPException(status_code=502, detail=f"source unavailable: {exc}")
    if isinstance(exc, WavError):
        return HTTPException(status_code=415, detail=f"unsupported WAV: {exc}")
    if isinstance(exc, UpstreamError):
        return HTTPException(status_code=502, detail=str(exc))
    if isinstance(exc, ValueError):
        return HTTPException(status_code=422, detail=str(exc))
    logger.exception("unexpected extraction error")
    return HTTPException(status_code=500, detail="internal extraction error")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/recordings/{recording_id}/info")
async def info(recording_id: str) -> dict:
    if not RECORDING_ID_RE.match(recording_id):
        raise HTTPException(status_code=400, detail="invalid recording id")
    assert client is not None
    source = RemoteSource(client, settings.source_base_url, recording_id)
    try:
        wav = await source.inspect()
    except UpstreamError as exc:
        raise _map_upstream(exc)
    except WavError as exc:
        raise _map_upstream(exc)
    return {
        "recording_id": recording_id,
        "etag": source.etag,
        "content_length": source.content_length,
        "audio_format": "PCM",
        "channels": wav.channels,
        "sample_rate": wav.sample_rate,
        "bits_per_sample": wav.bits_per_sample,
        "frame_count": wav.frame_count,
        "data_offset": wav.data_offset,
        "data_size": wav.data_size,
    }


@app.get("/recordings/{recording_id}/frames")
async def extract_frames(
    recording_id: str,
    start_frame: int = Query(..., ge=0, alias="start"),
    end_frame: int = Query(..., ge=1, alias="end"),
):
    _validate(recording_id, start_frame, end_frame)
    assert manager is not None
    key = (recording_id, start_frame, end_frame)

    # The waiter reference spans waiting AND streaming; it is released only
    # when the response finishes or the client disconnects. Releasing it while
    # the job is still RUNNING is what cancels the shared upstream read when
    # the last client goes away.
    job, joined = manager.create_or_join(key)

    # Block until header validation finishes so upstream/format/version errors
    # surface as proper HTTP statuses before any byte is sent. This wait is
    # cancellation-aware: a disconnect here releases the waiter and may cancel
    # the shared job.
    await job.started_event.wait()
    if job.state == JobState.FAILED:
        manager.leave(job)
        raise _map_upstream(job.error or RuntimeError("extraction failed"))
    if job.state not in (JobState.RUNNING, JobState.SUCCEEDED):
        manager.leave(job)
        raise HTTPException(status_code=503, detail=f"job state is {job.state.value}")

    # The relay replays buffered chunks for late subscribers, so this works
    # identically whether the job is still running or already succeeded.
    relay_iter = manager.stream(job)
    assert relay_iter is not None

    async def body():
        try:
            # Header + audio chunks arrive from the single upstream read via
            # the relay (live or replayed). A relay error aborts this response
            # without ever serving a partial WAV.
            try:
                async for chunk in relay_iter:
                    yield chunk
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - mapped for the client
                logger.info("relay stream aborted for %s: %s", job.key, exc)
                raise
        finally:
            manager.leave(job)

    headers = {
        "Content-Disposition": (
            f'attachment; filename="{recording_id}_{start_frame}-{end_frame}.wav"'
        ),
        "X-Source-ETag": job.etag or "",
        "X-Coalesced": "1" if joined else "0",
    }
    return StreamingResponse(body(), media_type="audio/wav", headers=headers)


@app.get("/jobs")
async def jobs() -> dict:
    assert manager is not None
    return {"jobs": manager.list_jobs()}
