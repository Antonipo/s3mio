# Changelog

All notable changes to s3mio are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [0.1.0] — 2025-04-15

Initial release.

### Added

- **Core CRUD** — `put()`, `get_bytes()`, `get_text()`, `get_json()`, `delete()`,
  `exists()` with auto-detected Content-Type and support for metadata and tags.
- **List & iterate** — `list()` (sorted, materialised) and `iter_list()` (streaming
  generator, no RAM buffering) with prefix and delimiter filters.
- **File I/O** — `upload()` with multipart above 8 MB, progress callback, auto
  Content-Type from file extension, metadata, and tags; `download()` with atomic
  write and automatic parent-dir creation.
- **Streaming** — `open()` context manager for line-by-line text streaming;
  `stream()` for binary chunk streaming with transparent mid-stream reconnect using
  `Range` header and exponential-backoff retry.
- **Batch operations** — `delete_many()` with `DeleteResult` (deleted / failed split);
  `copy()` using high-level TransferManager for objects > 5 GB; `copy_many()` with
  `CopyResult` and optional `ThreadPoolExecutor` parallelism via `max_workers`.
- **Tags** — `set_tags()`, `get_tags()`.
- **Head** — `head()` returning full `ObjectInfo` (size, ETag, Content-Type,
  metadata); optional `with_tags=True` for a second tagging call.
- **Presigned URLs** — `presign()` for `GET` and `PUT` with configurable expiry.
- **Virtual folders** — `Prefix` via the `/` operator (`bucket / "logs" / "2025"`);
  full API parity with `Bucket` including `delete_all()`.
- **Retry** — `call_with_retry()` with exponential backoff, jitter, configurable
  `max_retries` / `retry_delay` / `max_retry_delay`; catches both `ClientError`
  throttle codes and `BotoCoreError` network errors.
- **S3 Express One Zone** — auto-detected from bucket name suffix (`--x-s3`).
- **Typed API** — `ObjectInfo`, `DeleteResult`, `CopyResult` dataclasses;
  `BucketProtocol` / `PrefixProtocol` for structural typing.
- **Exceptions** — `S3Error` hierarchy: `ObjectNotFoundError`, `BucketNotFoundError`,
  `AccessDeniedError`, `ValidationError`, `S3OperationError`.
