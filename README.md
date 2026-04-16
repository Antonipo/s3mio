# s3mio

[![Tests](https://github.com/Antonipo/s3mio/actions/workflows/tests.yml/badge.svg)](https://github.com/Antonipo/s3mio/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/s3mio)](https://pypi.org/project/s3mio/)
[![Coverage](https://codecov.io/gh/Antonipo/s3mio/branch/main/graph/badge.svg)](https://codecov.io/gh/Antonipo/s3mio)

Pythonic S3 wrapper for AWS — clean, typed API over boto3.

```python
from s3mio import S3

s3 = S3(region_name="us-east-1")
bucket = s3.bucket("my-bucket")

bucket.put("config.json", {"env": "prod"})
cfg = bucket.get_json("config.json")

folder = bucket / "logs" / "2025"
for obj in folder:
    print(obj.key, obj.size)
```

## Installation

```bash
pip install s3mio
```

Requires Python 3.10+ and `boto3>=1.26`.

## Quick start

```python
from s3mio import S3

# Uses AWS_DEFAULT_REGION / ~/.aws/credentials automatically
s3 = S3(region_name="us-east-1")
bucket = s3.bucket("my-bucket")

# Write
bucket.put("notes/hello.txt", "Hello, world!")
bucket.put("data/config.json", {"version": 2, "env": "prod"})
bucket.put("assets/logo.png", open("logo.png", "rb").read(), content_type="image/png")

# Read
text = bucket.get_text("notes/hello.txt")
cfg  = bucket.get_json("data/config.json")
raw  = bucket.get_bytes("assets/logo.png")

# Delete
bucket.delete("notes/hello.txt")

# Check existence
if bucket.exists("data/config.json"):
    print("found")

# List
for obj in bucket.list(prefix="data/"):
    print(obj.key, obj.size, obj.last_modified)
```

## Feature reference

### Core object operations

| Method | Description |
|--------|-------------|
| `put(key, data, *, content_type, metadata, tags)` | Write bytes / str / dict / list. Type and Content-Type are auto-detected. |
| `get_bytes(key)` | Download raw bytes. |
| `get_text(key, encoding="utf-8")` | Download and decode as text. |
| `get_json(key)` | Download and parse as JSON. |
| `delete(key)` | Delete an object. Idempotent — no error if the key does not exist. |
| `exists(key)` | `True` / `False` — no body download. |

`put()` type detection (when `content_type` is not given):

| Python type | Content-Type |
|-------------|--------------|
| `dict` / `list` | `application/json` |
| `str` | `text/plain; charset=utf-8` |
| `bytes` / `bytearray` | `application/octet-stream` |

### List & iterate

```python
# Materialised, sorted by key
objects = bucket.list(prefix="reports/", delimiter="/")

# Streaming — no RAM buffering; good for millions of objects
for obj in bucket.iter_list(prefix="logs/"):
    process(obj)
```

`ObjectInfo` fields: `key`, `size`, `last_modified`, `etag`, `storage_class`.
`head()` additionally populates `content_type`, `metadata`, and optionally `tags`.

### File upload & download

```python
# Upload — multipart kicked in automatically above 8 MB
bucket.upload("local/video.mp4", "media/video.mp4",
              on_progress=lambda pct: print(f"\r{pct:.0f}%", end=""))

# With explicit metadata and tags
bucket.upload(
    "backup.tar.gz", "backups/nightly.tar.gz",
    content_type="application/gzip",
    metadata={"source": "nightly-job"},
    tags={"env": "prod"},
)

# Download — parent dirs created automatically
bucket.download("reports/q1.pdf", "/tmp/q1.pdf")
```

### Streaming

```python
# Text streaming — line by line, no buffering
with bucket.open("events/2025.jsonl") as lines:
    for line in lines:
        event = json.loads(line)

# Binary streaming — fixed-size chunks
with open("local_copy.bin", "wb") as f:
    for chunk in bucket.stream("backups/large.bin", chunk_size=16 * 1024 * 1024):
        f.write(chunk)
```

`stream()` transparently reconnects on transient network errors using a `Range` header
so it resumes from the last received byte — no restart from scratch.

### Batch operations

```python
# Bulk delete — up to 1000 keys per S3 call, chunked automatically
result = bucket.delete_many(["tmp/a.json", "tmp/b.json", "tmp/c.json"])
print(f"Deleted {len(result)} objects")
if not result:                         # bool(result) is False when failures exist
    for key, code in result.failed:
        print(f"  {key!r}: {code}")

# Copy within the same bucket
bucket.copy("drafts/report.pdf", "published/report.pdf")

# Copy to another bucket
bucket.copy("data/export.csv", "archive/export.csv", dest_bucket="archive-bucket")

# Batch copy with thread pool
result = bucket.copy_many(
    [("raw/jan.csv", "processed/jan.csv"), ("raw/feb.csv", "processed/feb.csv")],
    max_workers=8,
)
if not result:
    bucket.copy_many(result.failed_pairs())  # retry only the failures
```

### Tags

```python
bucket.set_tags("reports/q1.pdf", {"env": "prod", "owner": "data-team"})
tags = bucket.get_tags("reports/q1.pdf")   # {"env": "prod", "owner": "data-team"}

# Remove all tags
bucket.set_tags("reports/q1.pdf", {})
```

### Object metadata & head

```python
info = bucket.head("reports/q1.pdf")
print(info.size)          # 204_800
print(info.content_type)  # "application/pdf"
print(info.metadata)      # {"author": "antonio"}

# Fetch tags in the same call (one extra API request)
info = bucket.head("reports/q1.pdf", with_tags=True)
print(info.tags)          # {"env": "prod"}
```

### Presigned URLs

```python
# Share a file for 24 hours (no credentials required to download)
url = bucket.presign("exports/report.pdf", expires_in=86400)

# Client-side upload URL (browser PUTs directly to S3)
upload_url = bucket.presign("uploads/photo.jpg", method="PUT", expires_in=300)
```

### Virtual folders (Prefix)

The `/` operator returns a `Prefix` that scopes all operations under that path:

```python
folder = bucket / "dev" / "proyecto1"

folder.put("demo.txt", "hello")       # writes dev/proyecto1/demo.txt
folder.get_text("demo.txt")           # reads  dev/proyecto1/demo.txt
folder.exists("demo.txt")             # checks dev/proyecto1/demo.txt

for obj in folder:                    # lists  dev/proyecto1/
    print(obj.key)

result = folder.delete_all()          # bulk-deletes everything under prefix
```

`Prefix` exposes the same API surface as `Bucket`: `put`, `get_*`, `delete`,
`exists`, `list`, `iter_list`, `head`, `presign`, `upload`, `download`, `open`,
`stream`, `set_tags`, `get_tags`, `delete_many`, `copy`, `copy_many`.

## Configuration

```python
s3 = S3(
    region_name="us-east-1",
    aws_access_key_id="...",       # optional — falls back to env / ~/.aws
    aws_secret_access_key="...",
    aws_session_token="...",       # for temporary credentials / IAM roles
    endpoint_url="http://localhost:4566",  # LocalStack / MinIO
    max_retries=3,                 # retry attempts on throttle / network errors
    retry_delay=0.1,               # base back-off in seconds (doubles each attempt)
    max_retry_delay=30.0,          # cap on sleep time between retries
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `region_name` | `AWS_DEFAULT_REGION` env | AWS region |
| `aws_access_key_id` | credential chain | Explicit key ID |
| `aws_secret_access_key` | credential chain | Explicit secret |
| `aws_session_token` | — | Temporary session token |
| `endpoint_url` | — | Custom endpoint (LocalStack, MinIO, …) |
| `max_retries` | `3` | Max retry attempts after the first failure |
| `retry_delay` | `0.1` | Base sleep in seconds before first retry |
| `max_retry_delay` | `30.0` | Upper bound on inter-retry sleep |

`S3` supports use as a context manager to release the connection pool explicitly:

```python
with S3(region_name="us-east-1") as s3:
    bucket = s3.bucket("my-bucket")
    bucket.put("key", b"data")
```

## Error handling

All s3mio exceptions inherit from `S3Error`:

| Exception | When raised |
|-----------|-------------|
| `ObjectNotFoundError` | Key does not exist (HTTP 404) |
| `BucketNotFoundError` | Bucket does not exist |
| `AccessDeniedError` | Caller lacks the required IAM permission (HTTP 403) |
| `ValidationError` | Bad input caught before calling AWS (empty key, unsupported type, …) |
| `S3OperationError` | Any other AWS error not covered above |

```python
from s3mio import S3, ObjectNotFoundError, AccessDeniedError

try:
    data = bucket.get_json("missing.json")
except ObjectNotFoundError:
    print("not found")
except AccessDeniedError as exc:
    print(f"permission denied: {exc}")
```

## Development

```bash
git clone https://github.com/Antonipo/s3mio.git
cd s3mio
uv pip install -e ".[dev]"

# lint
uv run ruff check src/

# type check
uv run mypy src/s3mio/

# tests
uv run pytest --cov=s3mio
```

Tests use [moto](https://github.com/getmoto/moto) to mock AWS — no real credentials needed.

## Requirements

- Python 3.10+
- `boto3 >= 1.26`

## License

Apache 2.0 — see [LICENSE](LICENSE).
