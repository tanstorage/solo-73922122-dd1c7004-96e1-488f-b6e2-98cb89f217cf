"""Extraction runner: one upstream read per coalesced job.

Every audio chunk read from the source is both appended to the staging file
and published onto the job's relay, so waiting clients receive bytes as they
arrive while the single ETag-bound read is in flight.
"""
from __future__ import annotations

from .jobs import Job
from .upstream import RemoteSource
from .wav import build_pcm_wav_header


async def run_extraction(
    job: Job,
    client,
    source_base_url: str,
    publish,
    mark_started,
) -> str:
    recording_id, start_frame, end_frame = job.key
    source = RemoteSource(client, source_base_url, recording_id)

    info = await source.inspect()
    job.etag = source.etag
    if end_frame > info.frame_count:
        raise ValueError(
            f"end_frame {end_frame} exceeds recording length {info.frame_count} frames"
        )
    # Header validation is complete: from here on waiters may stream. Any
    # later failure (version swap, cut) aborts the in-flight body rather than
    # returning a half WAV.
    mark_started(job)

    frame_size = info.frame_size
    byte_start = info.data_offset + start_frame * frame_size
    byte_end = info.data_offset + end_frame * frame_size - 1
    data_size = (end_frame - start_frame) * frame_size

    with open(job.path, "wb") as fh:
        header = build_pcm_wav_header(info.channels, info.sample_rate, data_size)
        fh.write(header)
        await publish(header)
        async for chunk in source.iter_range(byte_start, byte_end):
            fh.write(chunk)
            await publish(chunk)

    # The staging path is an internal artifact (cleaned up by JobManager);
    # clients receive bytes exclusively through the relay.
    return job.path
