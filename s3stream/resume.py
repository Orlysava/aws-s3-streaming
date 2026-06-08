"""Resume interrupted multipart uploads using S3 ListParts."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
from dataclasses import dataclass, field
from typing import BinaryIO, Callable

import boto3
from botocore.config import Config as BotoConfig

from s3stream.upload import (
    DEFAULT_PART_SIZE,
    MIN_PART_SIZE,
    PartInfo,
    UploadProgress,
)

logger = logging.getLogger(__name__)


@dataclass
class ResumableUploadState:
    """Persisted state for a resumable upload."""

    upload_id: str
    bucket: str
    key: str
    part_size: int
    total_bytes: int
    parts_completed: dict[int, PartInfo] = field(default_factory=dict)

    @property
    def uploaded_bytes(self) -> int:
        return sum(p.size for p in self.parts_completed.values())

    @property
    def total_parts(self) -> int:
        if self.total_bytes <= 0:
            return 0
        return math.ceil(self.total_bytes / self.part_size)

    def to_dict(self) -> dict:
        return {
            "upload_id": self.upload_id,
            "bucket": self.bucket,
            "key": self.key,
            "part_size": self.part_size,
            "total_bytes": self.total_bytes,
            "parts_completed": {
                str(k): {"part_number": v.part_number, "etag": v.etag, "size": v.size, "md5": v.md5}
                for k, v in self.parts_completed.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> ResumableUploadState:
        parts = {}
        for k, v in data.get("parts_completed", {}).items():
            parts[int(k)] = PartInfo(
                part_number=v["part_number"], etag=v["etag"], size=v["size"], md5=v.get("md5", "")
            )
        return cls(
            upload_id=data["upload_id"],
            bucket=data["bucket"],
            key=data["key"],
            part_size=data["part_size"],
            total_bytes=data["total_bytes"],
            parts_completed=parts,
        )


class ResumeManager:
    """Manages resumable multipart uploads.

    Persists upload state to a local JSON file so interrupted uploads can be
    continued without re-uploading completed parts.

    Usage::

        mgr = ResumeManager(state_dir="/tmp/s3-resume")
        state = mgr.start(bucket="my-bucket", key="big.bin", file_path="big.bin")
        # ... upload is tracked automatically ...
        # Later, if interrupted:
        mgr.resume(state, file_path="big.bin")
    """

    def __init__(
        self,
        state_dir: str = ".s3resume",
        s3_client=None,
        region_name: str | None = None,
        on_progress: Callable[[UploadProgress], None] | None = None,
    ):
        self.state_dir = state_dir
        self.on_progress = on_progress
        os.makedirs(state_dir, exist_ok=True)

        self._s3 = s3_client or boto3.client(
            "s3",
            region_name=region_name,
            config=BotoConfig(retries={"max_attempts": 5, "mode": "adaptive"}),
        )
        self._lock = threading.Lock()

    def start(
        self,
        bucket: str,
        key: str,
        file_path: str | os.PathLike,
        *,
        part_size: int = DEFAULT_PART_SIZE,
        metadata: dict[str, str] | None = None,
    ) -> ResumableUploadState:
        """Start a new resumable upload. Returns the initial state."""
        file_path = os.fspath(file_path)
        total_bytes = os.path.getsize(file_path)

        if part_size < MIN_PART_SIZE:
            raise ValueError(f"part_size must be >= {MIN_PART_SIZE}")

        # Initiate multipart upload on S3.
        kwargs: dict = {"Bucket": bucket, "Key": key}
        if metadata:
            kwargs["Metadata"] = metadata

        resp = self._s3.create_multipart_upload(**kwargs)
        upload_id = resp["UploadId"]

        state = ResumableUploadState(
            upload_id=upload_id,
            bucket=bucket,
            key=key,
            part_size=part_size,
            total_bytes=total_bytes,
        )
        self._save_state(state)
        logger.info("Started resumable upload %s for s3://%s/%s", upload_id, bucket, key)

        # Upload all parts.
        self._upload_remaining(state, file_path)
        return state

    def resume(self, state: ResumableUploadState, file_path: str | os.PathLike) -> str:
        """Resume an interrupted upload. Returns the ETag on success.

        First reconciles local state with S3 via ListParts, then uploads
        any remaining parts.
        """
        file_path = os.fspath(file_path)

        # Reconcile with S3 — the server knows which parts actually landed.
        self._reconcile_from_s3(state)
        self._save_state(state)

        if state.uploaded_bytes >= state.total_bytes:
            # All parts done — just complete.
            return self._complete(state)

        self._upload_remaining(state, file_path)
        return self._complete(state)

    def list_incomplete(self) -> list[ResumableUploadState]:
        """List all tracked uploads that haven't been completed."""
        results = []
        for fname in os.listdir(self.state_dir):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(self.state_dir, fname)
            try:
                with open(path) as f:
                    import json

                    data = json.load(f)
                results.append(ResumableUploadState.from_dict(data))
            except Exception:
                logger.debug("Skipping corrupt state file %s", path, exc_info=True)
        return results

    def abort(self, state: ResumableUploadState) -> None:
        """Abort a multipart upload and clean up state."""
        try:
            self._s3.abort_multipart_upload(
                Bucket=state.bucket, Key=state.key, UploadId=state.upload_id
            )
        except Exception:
            logger.warning("Failed to abort upload %s", state.upload_id, exc_info=True)
        self._remove_state(state)

    def cleanup(self, state: ResumableUploadState) -> None:
        """Remove local state file (e.g. after successful completion)."""
        self._remove_state(state)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _upload_remaining(self, state: ResumableUploadState, file_path: str) -> None:
        total_parts = state.total_parts
        progress = UploadProgress(
            total_bytes=state.total_bytes,
            uploaded_bytes=state.uploaded_bytes,
            parts_completed=len(state.parts_completed),
            total_parts=total_parts,
            upload_id=state.upload_id,
        )

        with open(file_path, "rb") as f:
            for part_num in range(1, total_parts + 1):
                if part_num in state.parts_completed:
                    # Skip to next part.
                    f.seek(part_num * state.part_size)
                    continue

                f.seek((part_num - 1) * state.part_size)
                data = f.read(state.part_size)
                if not data:
                    break

                md5 = hashlib.md5(data).hexdigest()
                resp = self._s3.upload_part(
                    Bucket=state.bucket,
                    Key=state.key,
                    PartNumber=part_num,
                    UploadId=state.upload_id,
                    Body=data,
                    ContentMD5=md5,
                )
                etag = resp["ETag"]
                part_info = PartInfo(part_number=part_num, etag=etag, size=len(data), md5=md5)

                with self._lock:
                    state.parts_completed[part_num] = part_info
                    progress.uploaded_bytes += len(data)
                    progress.parts_completed += 1
                    self._save_state(state)

                if self.on_progress:
                    try:
                        self.on_progress(progress)
                    except Exception:
                        pass

                logger.debug("Uploaded part %d/%d (%d bytes)", part_num, total_parts, len(data))

    def _complete(self, state: ResumableUploadState) -> str:
        parts_sorted = sorted(state.parts_completed.values(), key=lambda p: p.part_number)
        multipart_parts = [{"PartNumber": p.part_number, "ETag": p.etag} for p in parts_sorted]

        resp = self._s3.complete_multipart_upload(
            Bucket=state.bucket,
            Key=state.key,
            UploadId=state.upload_id,
            MultipartUpload={"Parts": multipart_parts},
        )
        etag = resp["ETag"]
        self._remove_state(state)
        logger.info("Completed resumable upload s3://%s/%s → %s", state.bucket, state.key, etag)
        return etag

    def _reconcile_from_s3(self, state: ResumableUploadState) -> None:
        """Query S3 for parts that actually exist and update local state."""
        try:
            paginator = self._s3.get_paginator("list_parts")
            existing: dict[int, PartInfo] = {}

            for page in paginator.paginate(
                Bucket=state.bucket,
                Key=state.key,
                UploadId=state.upload_id,
            ):
                for part in page.get("Parts", []):
                    pn = part["PartNumber"]
                    existing[pn] = PartInfo(
                        part_number=pn,
                        etag=part["ETag"],
                        size=part["Size"],
                    )

            # Only keep parts that S3 actually has.
            state.parts_completed = {
                k: v for k, v in state.parts_completed.items() if k in existing
            }
            # Add any S3 has that we don't (e.g. state file was corrupted).
            for pn, info in existing.items():
                if pn not in state.parts_completed:
                    state.parts_completed[pn] = info

            logger.info(
                "Reconciled: %d parts confirmed on S3 for %s",
                len(state.parts_completed),
                state.upload_id,
            )
        except Exception as e:
            logger.warning("Could not reconcile from S3: %s", e)

    def _state_path(self, state: ResumableUploadState) -> str:
        safe_key = state.key.replace("/", "__")
        return os.path.join(self.state_dir, f"{state.upload_id}_{safe_key}.json")

    def _save_state(self, state: ResumableUploadState) -> None:
        import json

        path = self._state_path(state)
        with open(path, "w") as f:
            json.dump(state.to_dict(), f, indent=2)

    def _remove_state(self, state: ResumableUploadState) -> None:
        path = self._state_path(state)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
