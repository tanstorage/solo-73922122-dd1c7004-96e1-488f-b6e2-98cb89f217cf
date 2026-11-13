"""WAV container handling.

Only single-``data``-chunk, uncompressed 16-bit PCM WAV files are supported.
The inspection routine reads *only* headers via sparse HTTP range requests -
audio samples are never downloaded during inspection.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

RIFF_MAGIC = b"RIFF"
WAVE_MAGIC = b"WAVE"
FMT_ID = b"fmt "
DATA_ID = b"data"

# Header scanning is capped: a well formed WAV header is a few dozen bytes.
# This bounds the amount of header data fetched when chunks look malformed.
MAX_HEADER_SCAN = 1024 * 1024
# 8-byte chunk headers (id + size) fetched sparsely after the data chunk.
CHUNK_HEADER_LEN = 8


class WavError(Exception):
    """Raised when the remote file is not an accepted PCM WAV."""


@dataclass(frozen=True)
class WavInfo:
    audio_format: int
    channels: int
    sample_rate: int
    bits_per_sample: int
    block_align: int
    data_offset: int
    data_size: int  # bytes
    frame_count: int

    @property
    def frame_size(self) -> int:
        return self.block_align


def _u32(buf: bytes, pos: int) -> int:
    return struct.unpack_from("<I", buf, pos)[0]


def _u16(buf: bytes, pos: int) -> int:
    return struct.unpack_from("<H", buf, pos)[0]


async def inspect_wav(fetch, get_size) -> WavInfo:
    """Validate a remote WAV using range requests only.

    ``fetch(start, end)`` returns ``bytes`` for the inclusive byte range.
    ``get_size()`` returns the total remote file size in bytes.

    The first fetch pins the ETag/size on the remote source, so it must
    happen before ``get_size()`` is consulted.
    """
    have = 64
    buf = await fetch(0, have - 1)
    total = int(get_size())
    if total < 44:
        raise WavError("file too small to be a WAV container")
    if len(buf) != min(total, have):
        raise WavError("short read while fetching WAV header")
    if buf[0:4] != RIFF_MAGIC or buf[8:12] != WAVE_MAGIC:
        raise WavError("not a RIFF/WAVE container")
    if _u32(buf, 4) + 8 != total:
        raise WavError("RIFF chunk size does not match content length")

    pos = 12
    fmt = None
    data_offset = -1
    data_size = -1

    while True:
        if pos + CHUNK_HEADER_LEN > len(buf):
            want = min(total, max(len(buf) * 2, pos + CHUNK_HEADER_LEN))
            if want > MAX_HEADER_SCAN and want != total:
                # Stop growing once we are far past any plausible header.
                want = min(total, MAX_HEADER_SCAN)
            extra = await fetch(len(buf), want - 1)
            if len(extra) != want - len(buf):
                raise WavError("short read while fetching chunk header")
            buf += extra
        if pos + CHUNK_HEADER_LEN > total:
            raise WavError("truncated chunk header")

        chunk_id = bytes(buf[pos:pos + 4])
        chunk_size = _u32(buf, pos + 4)
        body_start = pos + CHUNK_HEADER_LEN
        body_end = body_start + chunk_size  # exclusive
        if body_end > total:
            raise WavError(f"chunk {chunk_id!r} extends past end of file")

        if chunk_id == FMT_ID:
            if fmt is not None:
                raise WavError("multiple fmt chunks")
            if body_end > len(buf):
                extra = await fetch(len(buf), body_end - 1)
                if len(extra) != body_end - len(buf):
                    raise WavError("short read while fetching fmt chunk")
                buf += extra
            if chunk_size < 16:
                raise WavError("fmt chunk too small for PCM format")
            fmt = {
                "audio_format": _u16(buf, body_start + 0),
                "channels": _u16(buf, body_start + 2),
                "sample_rate": _u32(buf, body_start + 4),
                "byte_rate": _u32(buf, body_start + 8),
                "block_align": _u16(buf, body_start + 12),
                "bits_per_sample": _u16(buf, body_start + 14),
            }
        elif chunk_id == DATA_ID:
            if fmt is None:
                raise WavError("data chunk appears before fmt chunk")
            data_offset = body_start
            data_size = chunk_size
            break
        # Any other pre-data chunk (LIST, fact, ...) is skipped.
        pos = body_end + (chunk_size & 1)  # chunks are word aligned

    if data_offset < 0:
        raise WavError("no data chunk found")
    if data_size == 0:
        raise WavError("empty data chunk")
    if data_offset > MAX_HEADER_SCAN:
        raise WavError("header region implausibly large; refusing to scan")

    # Reject any trailing chunks: exactly one data chunk is supported.
    trailing = data_offset + data_size
    trailing += data_size & 1  # account for data chunk padding
    while trailing < total:
        head = await fetch(trailing, trailing + CHUNK_HEADER_LEN - 1)
        if len(head) < 4:
            raise WavError("truncated trailing chunk header")
        if bytes(head[0:4]) == DATA_ID:
            raise WavError("multiple data chunks; only single data chunk WAV supported")
        if len(head) < CHUNK_HEADER_LEN:
            break  # trailing padding byte only
        trailing_size = _u32(head, 4)
        trailing += CHUNK_HEADER_LEN + trailing_size + (trailing_size & 1)

    assert fmt is not None
    if fmt["audio_format"] != 1:
        raise WavError(
            f"unsupported audio format {fmt['audio_format']}; only PCM (1)"
        )
    if fmt["bits_per_sample"] != 16:
        raise WavError(
            f"unsupported sample width {fmt['bits_per_sample']} bits; only 16-bit PCM"
        )
    if fmt["channels"] < 1 or fmt["channels"] > 64:
        raise WavError(f"implausible channel count {fmt['channels']}")
    if fmt["block_align"] != fmt["channels"] * fmt["bits_per_sample"] // 8:
        raise WavError("block align does not match channels/bits")
    frame_size = fmt["block_align"]
    if data_size % frame_size != 0:
        raise WavError("data size is not a whole number of frames")

    frame_count = data_size // frame_size
    return WavInfo(
        audio_format=fmt["audio_format"],
        channels=fmt["channels"],
        sample_rate=fmt["sample_rate"],
        bits_per_sample=fmt["bits_per_sample"],
        block_align=frame_size,
        data_offset=data_offset,
        data_size=data_size,
        frame_count=frame_count,
    )


def build_pcm_wav_header(channels: int, sample_rate: int, data_size: int) -> bytes:
    """Build a canonical 44-byte single-data-chunk PCM WAV header."""
    byte_rate = sample_rate * channels * 2
    block_align = channels * 2
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + data_size,
        b"WAVE",
        b"fmt ",
        16,
        1,  # PCM
        channels,
        sample_rate,
        byte_rate,
        block_align,
        16,  # bits per sample
        b"data",
        data_size,
    )
