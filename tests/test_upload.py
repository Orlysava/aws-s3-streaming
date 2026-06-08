"""Tests for streaming upload functionality."""

import io
import pytest
from s3stream.upload import StreamingMultipartUpload


class TestStreamingMultipartUpload:
    def test_chunk_data(self):
        """Test data is split into correct chunk sizes."""
        uploader = StreamingMultipartUpload(
            bucket="test-bucket",
            key="test.bin",
            chunk_size=1024,
        )

        data = b"x" * 3000
        chunks = list(uploader._chunk_data(data))
        assert len(chunks) == 3
        assert len(chunks[0]) == 1024
        assert len(chunks[1]) == 1024
        assert len(chunks[2]) == 952

    def test_chunk_data_small(self):
        """Test data smaller than chunk size."""
        uploader = StreamingMultipartUpload(
            bucket="test-bucket",
            key="test.bin",
            chunk_size=1024,
        )

        data = b"x" * 500
        chunks = list(uploader._chunk_data(data))
        assert len(chunks) == 1
        assert chunks[0] == data

    def test_progress_callback(self):
        """Test progress callback is called."""
        uploader = StreamingMultipartUpload(
            bucket="test-bucket",
            key="test.bin",
            chunk_size=100,
        )

        progress_values = []
        uploader._report_progress(50, 100, progress_values.append)
        assert len(progress_values) == 1
        assert progress_values[0] == 50.0
