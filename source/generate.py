"""Generate synthetic, PII-free WAV fixtures.

Everything is deterministic pseudo-random / synthesized tones - no field
recordings, no metadata chunks, no personal information. The generator only
uses the Python standard library so it can run inside a plain build image.

Outputs under OUT_DIR (default /samples):

    nightjar_v1/, nightjar_v2/ ...    two versions -> ETag change demo
    warbler/, rail/, sparrow/         single version, different lengths/rates
    float32/                           32-bit float -> 415 unsupported demo
    two_data_chunks/                   two data chunks -> 415 unsupported demo
"""
from __future__ import annotations

import math
import os
import random
import struct
import wave

OUT_DIR = os.environ.get("OUT_DIR", "/samples")


def _samples(seconds: float, rate: int, freqs: list[float], seed: int) -> list[int]:
    rng = random.Random(seed)
    count = int(seconds * rate)
    frames: list[int] = []
    for n in range(count):
        # Slow amplitude envelope mimicking a bird call burst, plus gentle
        # background "wind" noise. All values are fully synthetic.
        envelope = 0.55 + 0.45 * math.sin(2 * math.pi * (0.7 + seed % 5) * n / rate)
        tone = sum(
            math.sin(2 * math.pi * f * n / rate + phase)
            for f, phase in zip(freqs, (0.0, 0.6, 1.2))
        ) / len(freqs)
        noise = (rng.random() * 2 - 1) * 0.04
        value = int(32767 * 0.6 * envelope * tone + 32767 * noise)
        frames.append(max(-32768, min(32767, value)))
    return frames


def write_pcm16(path: str, frames: list[int], rate: int, channels: int = 1) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with wave.open(path, "wb") as wav:
        wav.setnchannels(channels)
        wav.setsampwidth(2)
        wav.setframerate(rate)
        if channels == 1:
            wav.writeframes(struct.pack(f"<{len(frames)}h", *frames))
        else:
            stereo: list[int] = []
            for idx, value in enumerate(frames):
                stereo.append(value)
                stereo.append(frames[(idx + 1103) % len(frames)])
            wav.writeframes(struct.pack(f"<{len(stereo)}h", *stereo))


def write_float32(path: str, frames: list[int], rate: int) -> None:
    """32-bit IEEE float WAV (audio format 3) - must be rejected by the API."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    floats = [v / 32768.0 for v in frames]
    data = struct.pack(f"<{len(floats)}f", *floats)
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", 36 + len(data), b"WAVE", b"fmt ", 16,
        3,  # IEEE float
        1, rate, rate * 4, 4, 32,
        b"data", len(data),
    )
    with open(path, "wb") as fh:
        fh.write(header + data)


def write_two_data_chunks(path: str, frames: list[int], rate: int) -> None:
    """Two separate data chunks - must be rejected (single data chunk only)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mid = len(frames) // 2
    part1 = struct.pack(f"<{mid}h", *frames[:mid])
    part2 = struct.pack(f"<{len(frames) - mid}h", *frames[mid:])
    fmt_chunk = struct.pack(
        "<4sIHHIIHH",
        b"fmt ", 16, 1, 1, rate, rate * 2, 2, 16,
    )

    def data_chunk(payload: bytes) -> bytes:
        return struct.pack("<4sI", b"data", len(payload)) + payload

    body = fmt_chunk + data_chunk(part1) + data_chunk(part2)
    with open(path, "wb") as fh:
        fh.write(struct.pack("<4sI", b"RIFF", 4 + len(body)) + b"WAVE" + body)


def main() -> None:
    specs = [
        # id, version, seconds, rate, freqs, seed
        ("nightjar", "v1", 6.0, 48000, [2200.0, 3300.0], 11),
        ("nightjar", "v2", 6.0, 48000, [2450.0, 3675.0], 22),  # re-cut/re-encoded
        ("warbler", "v1", 4.0, 44100, [3100.0, 4650.0, 6200.0], 33),
        ("rail", "v1", 9.0, 22050, [900.0, 1350.0], 44),
        ("sparrow", "v1", 5.0, 48000, [4100.0, 5200.0], 55),
    ]
    for rec_id, version, seconds, rate, freqs, seed in specs:
        frames = _samples(seconds, rate, freqs, seed)
        write_pcm16(
            os.path.join(OUT_DIR, rec_id, f"{version}.wav"),
            frames, rate, channels=2 if rec_id == "sparrow" else 1,
        )

    base = _samples(3.0, 48000, [1800.0, 2700.0], 66)
    write_float32(os.path.join(OUT_DIR, "float32", "v1.wav"), base, 48000)
    write_two_data_chunks(
        os.path.join(OUT_DIR, "two_data_chunks", "v1.wav"), base, 48000
    )

    total = 0
    for root, _dirs, files in os.walk(OUT_DIR):
        for name in files:
            size = os.path.getsize(os.path.join(root, name))
            total += size
            print(f"generated {os.path.join(root, name)} ({size} bytes)")
    print(f"done: {total} bytes under {OUT_DIR}")


if __name__ == "__main__":
    main()
