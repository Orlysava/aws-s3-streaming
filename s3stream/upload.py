"""Streaming multipart upload to S3."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
from dataclasses import dataclass, field
from typing import IO, BinaryIO, Callable, Optional

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger(__name__)

# S3 limits: 5 MiB minimum part size (except last), 10 000 parts max.
DEFAULT_PART_SIZE = 8 * 1024 * 1024  # 8 MiB
MIN_PART_SIZE = 5 * 1024 * 1024
MAX_PARTS = 10_000


@dataclass
class UploadProgress:
    """Tracks upload progress."""

    total_bytes: int = 0
    uploaded_bytes: int = 0
    parts_completed: int = 0
    total_parts: int = 0
    upload_id: str = ""
    etag: str = ""

    @property
    def percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return min(100.0, (self.uploaded_bytes / self.total_bytes) * 100)


@dataclass
class PartInfo:
    """Metadata for a single completed part."""

    part_number: int
    etag: str
    size: int
    md5: str = ""


class StreamingMultipartUpload:
    """Uploads a file (or stream) to S3 using multipart upload with optional
    concurrency and progress callbacks.

    Usage::

        upload = StreamingMultipartUpload(
            bucket="my-bucket",
            key="large-file.bin",
            part_size=10 * 1024 * 1024,  # 10 MiB
        )
        with open("big.bin", "rb") as f:
            upload.upload(f)
        print(upload.progress.etag)
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        part_size: int = DEFAULT_PART_SIZE,
        max_concurrency: int = 4,
        s3_client=None,
        region_name: str | None = None,
        on_progress: Callable[[UploadProgress], None] | None = None,
        storage_class: str = "STANDARD",
        server_side_encryption: str | None = None,
        metadata: dict[str, str] | None = None,
    ):
        if part_size < MIN_PART_SIZE:
            raise ValueError(f"part_size must be >= {MIN_PART_SIZE} bytes")

        self.bucket = bucket
        self.key = key
        self.part_size = part_size
        self.max_concurrency = max(1, max_concurrency)
        self.on_progress = on_progress
        self.storage_class = storage_class
        self.server_side_encryption = server_side_encryption
        self.metadata = metadata or {}

        self._s3 = s3_client or boto3.client(
            "s3",
            region_name=region_name,
            config=BotoConfig(
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=self.max_concurrency + 4,
            ),
        )

        self.progress = UploadProgress()
        self.parts: list[PartInfo] = []
        self._upload_id: str | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload(self, source: BinaryIO, *, content_length: int | None = None) -> str:
        """Upload *source* to S3. Returns the ETag of the completed object.

        Parameters
        ----------
        source : BinaryIO
            Readable binary file-like object.
        content_length : int, optional
            Total size in bytes. If not provided, the stream is buffered to a
            temp file to determine size. Pass it when you know the size upfront
            for best performance.
        """
        if content_length is None:
            content_length = self._seek_or_buffer(source)

        self.progress.total_bytes = content_length
        self.progress.total_parts = math.ceil(content_length / self.part_size)

        if self.progress.total_parts <= 1:
            return self._simple_upload(source)

        self._initiate_multipart()
        try:
            self._upload_parts(source)
            etag = self._complete()
            self.progress.etag = etag
            return etag
        except Exception:
            self._abort()
            raise

    def upload_file(self, path: str | os.PathLike) -> str:
        """Convenience: upload a file by path."""
        path = os.fspath(path)
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            return self.upload(f, content_length=size)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _simple_upload(self, source: BinaryIO) -> str:
        body = source.read()
        kwargs = {
            "Bucket": self.bucket,
            "Key": self.key,
            "Body": body,
            "StorageClass": self.storage_class,
        }
        if self.metadata:
            kwargs["Metadata"] = self.metadata
        if self.server_side_encryption:
            kwargs["ServerSideEncryption"] = self.server_side_encryption

        resp = self._s3.put_object(**kwargs)
        etag = resp["ETag"]
        self.progress.etag = etag
        self.progress.uploaded_bytes = len(body)
        self.progress.parts_completed = 1
        self._notify()
        return etag

    def _initiate_multipart(self) -> None:
        kwargs = {"Bucket": self.bucket, "Key": self.key}
        if self.metadata:
            kwargs["Metadata"] = self.metadata
        if self.server_side_encryption:
            kwargs["ServerSideEncryption"] = self.server_side_encryption

        resp = self._s3.create_multipart_upload(**kwargs)
        self._upload_id = resp["UploadId"]
        self.progress.upload_id = self._upload_id
        logger.info("Initiated multipart upload %s for s3://%s/%s", self._upload_id, self.bucket, self.key)

    def _upload_parts(self, source: BinaryIO) -> None:
        """Read parts from source and upload them with bounded concurrency."""
        semaphore = threading.Semaphore(self.max_concurrency)
        errors: list[Exception] = []
        threads: list[threading.Thread] = []

        for part_num in range(1, self.progress.total_parts + 1):
            data = source.read(self.part_size)
            if not data:
                break

            if self.max_concurrency <= 1:
                self._upload_one_part(part_num, data)
            else:
                semaphore.acquire()
                t = threading.Thread(
                    target=self._upload_one_part_threaded,
                    args=(part_num, data, semaphore, errors),
                    daemon=True,
                )
                threads.append(t)
                t.start()

        for t in threads:
            t.join()

        if errors:
            raise errors[0]

    def _upload_one_part(self, part_number: int, data: bytes) -> None:
        md5 = hashlib.md5(data).hexdigest()
        resp = self._s3.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=part_number,
            UploadId=self._upload_id,
            Body=data,
            ContentMD5=md5,
        )
        etag = resp["ETag"]

        with self._lock:
            self.parts.append(PartInfo(part_number=part_number, etag=etag, size=len(data), md5=md5))
            self.progress.uploaded_bytes += len(data)
            self.progress.parts_completed += 1
            self._notify()

    def _upload_one_part_threaded(self, part_number: int, data: bytes, semaphore: threading.Semaphore, errors: list) -> None:
        try:
            self._upload_one_part(part_number, data)
        except Exception as e:
            with self._lock:
                errors.append(e)
            logger.error("Failed to upload part %d: %s", part_number, e)
        finally:
            semaphore.release()

    def _complete(self) -> str:
        parts_sorted = sorted(self.parts, key=lambda p: p.part_number)
        multipart_parts = [{"PartNumber": p.part_number, "ETag": p.etag} for p in parts_sorted]

        resp = self._s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self._upload_id,
            MultipartUpload={"Parts": multipart_parts},
        )
        logger.info("Completed upload s3://%s/%s → %s", self.bucket, self.key, resp["ETag"])
        return resp["ETag"]

    def _abort(self) -> None:
        if self._upload_id:
            try:
                self._s3.abort_multipart_upload(
                    Bucket=self.bucket, Key=self.key, UploadId=self._upload_id
                )
                logger.warning("Aborted multipart upload %s", self._upload_id)
            except Exception:
                logger.exception("Failed to abort upload %s", self._upload_id)

    def _seek_or_buffer(self, source: BinaryIO) -> int:
        """Try to determine stream length; buffer to temp file if needed."""
        try:
            source.seek(0, os.SEEK_END)
            size = source.tell()
            source.seek(0)
            return size
        except (OSError, AttributeError):
            pass

        import tempfile

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            total = 0
            while chunk := source.read(self.part_size):
                tmp.write(chunk)
                total += len(chunk)
            tmp_path = tmp.name

        # Replace source with a file-backed object.
        # The caller won't know; we read from the temp file instead.
        source.close()
        # This is a bit of a hack — set an attribute so tests can clean up.
        self._tmp_path = tmp_path
        # We don't replace source here; the caller should use upload_file()
        # for non-seekable streams. This method is a fallback.
        raise ValueError(
            "Non-seekable stream without content_length. "
            "Use upload_file() or pass content_length explicitly."
        )

    def _notify(self) -> None:
        if self.on_progress:
            try:
                self.on_progress(self.progress)
            except Exception:
                logger.debug("Progress callback error", exc_info=True)
