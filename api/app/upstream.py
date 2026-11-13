"""HTTP access to the fixed recording source.

Every request must carry the same strong ETag (``If-Match``) and every
response must echo it. A changed version, a missing ETag, a weak ETag or an
interrupted stream aborts the whole extraction - partial WAV data is never
returned to a client.
"""
from __future__ import annotations

import httpx

from .wav import WavInfo, inspect_wav


class UpstreamError(Exception):
    """Base class for upstream source failures."""


class UpstreamVersionChanged(UpstreamError):
    """The source ETag changed (or vanished) mid extraction."""


class UpstreamUnavailable(UpstreamError):
    """The source could not be read (connection / HTTP / protocol error)."""


class UpstreamNotFound(UpstreamError):
    """The recording does not exist on the source."""


def _check_etag(response: httpx.Response, etag: str | None) -> str:
    matched = response.headers.get("etag")
    if not matched or matched.startswith(("W/", "w/")):
        raise UpstreamVersionChanged(
            f"source did not return a strong ETag (got {matched!r})"
        )
    if etag is not None and matched != etag:
        raise UpstreamVersionChanged(
            f"source ETag changed: expected {etag!r}, got {matched!r}"
        )
    return matched


class RemoteSource:
    def __init__(self, client: httpx.AsyncClient, base_url: str, recording_id: str):
        self._client = client
        self._url = f"{base_url.rstrip('/')}/recordings/{recording_id}"
        self.etag: str | None = None
        self.content_length: int | None = None
        self.info: WavInfo | None = None

    async def _range(self, start: int, end: int) -> httpx.Response:
        headers = {"Range": f"bytes={start}-{end}"}
        if self.etag is not None:
            headers["If-Match"] = self.etag
        try:
            req = self._client.build_request("GET", self._url, headers=headers)
            resp = await self._client.send(req, stream=True)
        except httpx.HTTPError as exc:
            raise UpstreamUnavailable(f"connection failed: {exc}") from exc

        if resp.status_code == 412:
            await resp.aclose()
            raise UpstreamVersionChanged("source rejected If-Match (412): version changed")
        if resp.status_code == 404:
            await resp.aclose()
            raise UpstreamNotFound("recording not found on source")
        if resp.status_code != 206:
            await resp.aclose()
            raise UpstreamUnavailable(
                f"source returned HTTP {resp.status_code}; range support is required"
            )

        cr = resp.headers.get("content-range", "")
        if not cr.startswith("bytes "):
            await resp.aclose()
            raise UpstreamUnavailable("source response missing Content-Range")
        try:
            total = int(cr.split("/", 1)[1])
        except (IndexError, ValueError):
            await resp.aclose()
            raise UpstreamUnavailable("malformed Content-Range header")

        try:
            etag = _check_etag(resp, self.etag)
        except UpstreamError:
            await resp.aclose()
            raise

        clen = resp.headers.get("content-length")
        if clen is not None and int(clen) != (end - start + 1):
            await resp.aclose()
            raise UpstreamUnavailable("Content-Length does not match requested range")

        if self.etag is None:
            self.etag = etag
            self.content_length = total
        elif self.content_length != total:
            await resp.aclose()
            raise UpstreamVersionChanged("source content length changed mid extraction")
        return resp

    async def fetch(self, start: int, end: int) -> bytes:
        """Return the inclusive byte range; validates ETag on every call."""
        resp = await self._range(start, end)
        try:
            return await resp.aread()
        except httpx.HTTPError as exc:
            raise UpstreamUnavailable(f"stream interrupted: {exc}") from exc
        finally:
            await resp.aclose()

    def get_size(self) -> int:
        if self.content_length is None:  # pragma: no cover - guarded by callers
            raise UpstreamUnavailable("source size unknown before first read")
        return self.content_length

    async def iter_range(self, start: int, end: int):
        """Yield chunks for an inclusive byte range, ETag-checked, and verify
        that exactly the requested number of bytes arrives."""
        resp = await self._range(start, end)
        expected = end - start + 1
        received = 0
        try:
            async for chunk in resp.aiter_bytes():
                if not chunk:
                    continue
                received += len(chunk)
                if received > expected:
                    raise UpstreamUnavailable("source sent more bytes than requested")
                yield chunk
            if received != expected:
                raise UpstreamUnavailable(
                    f"stream truncated: received {received}/{expected} bytes"
                )
        except httpx.HTTPError as exc:
            raise UpstreamUnavailable(f"stream interrupted: {exc}") from exc
        finally:
            await resp.aclose()

    async def inspect(self) -> WavInfo:
        # First fetch pins the ETag and content length for the whole extraction.
        self.info = await inspect_wav(self.fetch, self.get_size)
        return self.info
