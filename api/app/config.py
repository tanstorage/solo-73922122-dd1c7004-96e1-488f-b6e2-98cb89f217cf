import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    source_base_url: str
    spool_dir: str
    request_timeout: float
    max_frames: int


def load_settings() -> Settings:
    return Settings(
        source_base_url=os.environ.get(
            "SOURCE_BASE_URL", "http://source:8080"
        ).rstrip("/"),
        spool_dir=os.environ.get("SPOOL_DIR", "/tmp/wav-spool"),
        request_timeout=float(os.environ.get("REQUEST_TIMEOUT", "60")),
        max_frames=int(os.environ.get("MAX_FRAMES", "480000")),  # 10s @48k
    )
