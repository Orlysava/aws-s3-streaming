# aws-s3-streaming

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Streaming upload/download library for large S3 objects with multipart handling and resumable transfers.

## Features

- 📤 **Streaming Upload** — multipart upload with configurable chunk size
- 📥 **Streaming Download** — chunked download with progress callbacks
- 🔄 **Resumable Transfers** — checkpoint-based resume after failures
- 📊 **Progress Tracking** — callbacks for upload/download progress
- 🧵 **Concurrent Parts** — parallel multipart uploads

## Installation

```bash
pip install aws-s3-streaming
```

## Usage

### Streaming Upload

```python
from s3stream.upload import StreamingMultipartUpload

uploader = StreamingMultipartUpload(
    bucket="my-bucket",
    key="large-file.bin",
    chunk_size=100 * 1024 * 1024,  # 100MB chunks
)

# Upload from file
uploader.upload_file("large-file.bin", progress_callback=lambda pct: print(f"{pct:.1f}%"))

# Upload from stream
with open("data.bin", "rb") as f:
    uploader.upload_stream(f)
```

### Streaming Download

```python
from s3stream.download import StreamingDownloader

downloader = StreamingDownloader(bucket="my-bucket", key="large-file.bin")

# Download to file
downloader.download_file("output.bin", progress_callback=lambda pct: print(f"{pct:.1f}%"))

# Download to stream
for chunk in downloader.stream():
    process(chunk)
```

### Resumable Upload

```python
from s3stream.upload import StreamingMultipartUpload
from s3stream.resume import ResumeManager

manager = ResumeManager(checkpoint_file="upload.checkpoint")

# Resume a failed upload
uploader = StreamingMultipartUpload(
    bucket="my-bucket",
    key="large-file.bin",
    resume_manager=manager,
)
uploader.upload_file("large-file.bin")  # Automatically resumes from checkpoint
```

## API Reference

### `StreamingMultipartUpload`

| Method | Description |
|--------|-------------|
| `upload_file(path, progress_callback)` | Upload a file with multipart |
| `upload_stream(stream, progress_callback)` | Upload from a readable stream |
| `abort()` | Abort the multipart upload |

### `StreamingDownloader`

| Method | Description |
|--------|-------------|
| `download_file(path, progress_callback)` | Download to a file |
| `stream()` | Generator yielding chunks |

### `ResumeManager`

| Method | Description |
|--------|-------------|
| `save(upload_id, parts)` | Save checkpoint |
| `load(key)` | Load checkpoint |
| `clear(key)` | Remove checkpoint |

## Contributing

Contributions welcome! Please open an issue or PR.

## License

[MIT](LICENSE)
