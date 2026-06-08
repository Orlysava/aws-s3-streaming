"""Streaming download from S3 with range-request support."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import BinaryIO, Callable

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB


@dataclass
class DownloadProgress:
    """Tracks download progress."""

    total_bytes: int = 0
    downloaded_bytes: int = 0
    chunks_completed: int = 0

    @property
    def percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return min(100.0, (self.downloaded_bytes / self.total_bytes) * 100)


class StreamingDownload:
    """Downloads S3 objects in chunks, with optional concurrency and resume.

    Usage::

        dl = StreamingDownload(bucket="my-bucket", key="large-file.bin")
        with open("output.bin", "wb") as f:
            dl.download(f)
        print(dl.progress.percent)  # 100.0
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_concurrency: int = 4,
        s3_client=None,
        region_name: str | None = None,
        on_progress: Callable[[DownloadProgress], None] | None = None,
    ):
        self.bucket = bucket
        self.key = key
        self.chunk_size = chunk_size
        self.max_concurrency = max(1, max_concurrency)
        self.on_progress = on_progress

        self._s3 = s3_client or boto3.client(
            "s3",
            region_name=region_name,
            config=BotoConfig(
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=self.max_concurrency + 4,
            ),
        )

        self.progress = DownloadProgress()
        self._lock = threading.Lock()

    def download(self, dest: BinaryIO, *, offset: int = 0) -> int:
        """Download the object into *dest* starting at *offset* bytes.

        Returns the total bytes written.
        """
        head = self._s3.head_object(Bucket=self.bucket, Key=self.key)
        total_size = head["ContentLength"]
        self.progress.total_bytes = total_size

        if offset >= total_size:
            return 0

        ranges = self._build_ranges(offset, total_size)

        if len(ranges) <= 1 or self.max_concurrency <= 1:
            return self._sequential_download(dest, offset, total_size)
        return self._concurrent_download(dest, ranges, total_size)

    def download_file(self, path: str | os.PathLike, *, offset: int = 0) -> int:
        """Convenience: download to a file path. Supports resume via offset."""
        mode = "ab" if offset > 0 else "wb"
        with open(path, mode) as f:
            return self.download(f, offset=offset)

    def get_size(self) -> int:
        """Return the object size without downloading."""
        head = self._s3.head_object(Bucket=self.bucket, Key=self.key)
        return head["ContentLength"]

    def _build_ranges(self, offset: int, total: int) -> list[tuple[int, int]]:
        """Build (start, end) byte ranges for chunked download."""
        ranges = []
        pos = offset
        while pos < total:
            end = min(pos + self.chunk_size - 1, total - 1)
            ranges.append((pos, end))
            pos = end + 1
        return ranges

    def _sequential_download(self, dest: BinaryIO, offset: int, total_size: int) -> int:
        """Single-threaded download with Range header."""
        range_header = f"bytes={offset}-{total_size - 1}" if offset > 0 else None

        kwargs = {"Bucket": self.bucket, "Key": self.key}
        if range_header:
            kwargs["Range"] = range_header

        resp = self._s3.get_object(**kwargs)
        body = resp["Body"]
        written = 0

        while chunk := body.read(self.chunk_size):
            dest.write(chunk)
            written += len(chunk)
            with self._lock:
                self.progress.downloaded_bytes += len(chunk)
                self.progress.chunks_completed += 1
            self._notify()

        return written

    def _concurrent_download(self, dest: BinaryIO, ranges: list[tuple[int, int]], total_size: int) -> int:
        """Download chunks concurrently, writing to dest at the correct offsets."""
        # Pre-allocate a buffer to hold chunks in order.
        # For very large files, consider a different strategy.
        results: dict[int, bytes] = {}
        semaphore = threading.Semaphore(self.max_concurrency)
        errors: list[Exception] = []
        threads: list[threading.Thread] = []

        def fetch_chunk(idx: int, start: int, end: int):
            try:
                resp = self._s3.get_object(
                    Bucket=self.bucket,
                    Key=self.key,
                    Range=f"bytes={start}-{end}",
                )
                data = resp["Body"].read()
                with self._lock:
                    results[idx] = data
                    self.progress.downloaded_bytes += len(data)
                    self.progress.chunks_completed += 1
                self._notify()
            except Exception as e:
                with self._lock:
                    errors.append(e)
                logger.error("Failed to download chunk %d: %s", idx, e)
            finally:
                semaphore.release()

        for idx, (start, end) in enumerate(ranges):
            semaphore.acquire()
            t = threading.Thread(target=fetch_chunk, args=(idx, start, end), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if errors:
            raise errors[0]

        # Write chunks in order.
        written = 0
        for idx in range(len(ranges)):
            chunk = results.get(idx, b"")
            dest.write(chunk)
            written += len(chunk)

        return written

    def _notify(self) -> None:
        if self.on_progress:
            try:
                self.on_progress(self.progress)
            except Exception:
                logger.debug("Progress callback error", exc_info=True)
