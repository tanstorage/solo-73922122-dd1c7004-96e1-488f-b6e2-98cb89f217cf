"""Extraction runner: one complete upstream read per coalesced job.

The runner reads the whole requested fragment from the source before any
client can receive a response. Every upstream range call is bound to the same
strong ETag and the final byte count must equal the requested length; a cut,
a version swap or any length mismatch raises and the job fails without ever
exposing a partial WAV.
"""
from __future__ import annotations

from .upstream import RemoteSource, UpstreamUnavailable
from .wav import build_pcm_wav_header


async def run_extraction(job, client, source_base_url: str) -> None:
    recording_id, start_frame, end_frame = job.key
    source = RemoteSource(client, source_base_url, recording_id)

    # Sparse Range reads of the WAV header only; this also pins the strong
    # ETag and total length for every subsequent request.
    info = await source.inspect()
    job.etag = source.etag
    if end_frame > info.frame_count:
        raise ValueError(
            f"end_frame {end_frame} exceeds recording length {info.frame_count} frames"
        )

    frame_size = info.frame_size
    byte_start = info.data_offset + start_frame * frame_size
    byte_end = info.data_offset + end_frame * frame_size - 1
    data_size = (end_frame - start_frame) * frame_size
    expected_audio = byte_end - byte_start + 1

    # Write to a UUID-named .part staging file. It is unlinked on every
    # failure/cancellation path and after all successful downloads end, so no
    # partial fragment is ever downloadable or left behind.
    written = 0
    with open(job.path, "wb") as fh:
        fh.write(build_pcm_wav_header(info.channels, info.sample_rate, data_size))
        try:
            async for chunk in source.iter_range(byte_start, byte_end):
                fh.write(chunk)
                written += len(chunk)
        except UpstreamUnavailable:
            # The body was cut or dropped. Distinguish a confirmed version
            # swap (-> 409) from a plain source fault (-> 502) before failing.
            await source.confirm_version_after_failure()
            raise
        fh.flush()

    # Defence in depth: iter_range already proves the exact length, but never
    # mark the job successful unless the complete, exact fragment is on disk.
    if written != expected_audio:
        raise UpstreamUnavailable(
            f"incomplete fragment read: {written}/{expected_audio} audio bytes"
        )
