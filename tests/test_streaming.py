"""Tests for Fase 2 — open() and stream()."""

import json

import pytest

from s3mio.bucket import Bucket
from s3mio.exceptions import ObjectNotFoundError, ValidationError

# ---------------------------------------------------------------------------
# Bucket.open — text streaming
# ---------------------------------------------------------------------------


class TestBucketOpen:
    def test_open_yields_lines(self, bucket: Bucket) -> None:
        """open() yields one string per line of the object."""
        content = "line one\nline two\nline three"
        bucket.put("text/lines.txt", content)
        with bucket.open("text/lines.txt") as lines:
            result = list(lines)
        assert result == ["line one", "line two", "line three"]

    def test_open_strips_newlines(self, bucket: Bucket) -> None:
        """Trailing \\n and \\r\\n are stripped from each line."""
        bucket.put("text/crlf.txt", b"alpha\r\nbeta\r\ngamma\r\n")
        with bucket.open("text/crlf.txt") as lines:
            result = list(lines)
        assert result == ["alpha", "beta", "gamma"]

    def test_open_single_line_no_newline(self, bucket: Bucket) -> None:
        """A single-line file without a trailing newline yields one element."""
        bucket.put("text/single.txt", "only one line")
        with bucket.open("text/single.txt") as lines:
            result = list(lines)
        assert result == ["only one line"]

    def test_open_jsonl_file(self, bucket: Bucket) -> None:
        """Each line of a JSONL file can be parsed as valid JSON."""
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        content = "\n".join(json.dumps(r) for r in records)
        bucket.put("data/events.jsonl", content)

        parsed: list[dict] = []
        with bucket.open("data/events.jsonl") as lines:
            for line in lines:
                parsed.append(json.loads(line))

        assert parsed == records

    def test_open_utf8_encoding(self, bucket: Bucket) -> None:
        """Non-ASCII characters are decoded correctly with UTF-8."""
        bucket.put("text/unicode.txt", "héllo\nwörld\n日本語")
        with bucket.open("text/unicode.txt") as lines:
            result = list(lines)
        assert result == ["héllo", "wörld", "日本語"]

    def test_open_custom_encoding(self, bucket: Bucket) -> None:
        """open() accepts a custom *encoding* parameter."""
        data = "café".encode("latin-1")
        bucket.put("text/latin.txt", data)
        with bucket.open("text/latin.txt", encoding="latin-1") as lines:
            result = list(lines)
        assert result == ["café"]

    def test_open_empty_object(self, bucket: Bucket) -> None:
        """An empty object yields no lines (empty iterator)."""
        bucket.put("text/empty.txt", b"")
        with bucket.open("text/empty.txt") as lines:
            result = list(lines)
        assert result == []

    def test_open_large_file_does_not_load_into_memory(self, bucket: Bucket) -> None:
        """open() can iterate a large file sequentially without issues."""
        # 10 000 lines — large but not huge for a unit test
        lines_content = "\n".join(f"line {i}" for i in range(10_000))
        bucket.put("text/large.txt", lines_content)
        count = 0
        with bucket.open("text/large.txt") as lines:
            for _ in lines:
                count += 1
        assert count == 10_000

    def test_open_missing_key_raises(self, bucket: Bucket) -> None:
        """open() on a non-existent key raises ObjectNotFoundError."""
        with pytest.raises(ObjectNotFoundError):
            with bucket.open("text/ghost.txt") as _:
                pass  # should not reach here

    def test_open_empty_key_raises(self, bucket: Bucket) -> None:
        """open() with an empty key raises ValidationError immediately."""
        with pytest.raises(ValidationError):
            with bucket.open("") as _:
                pass

    def test_open_context_manager_closes_on_exit(self, bucket: Bucket) -> None:
        """The context manager exits cleanly even if the iterator is not exhausted."""
        content = "\n".join(f"line {i}" for i in range(100))
        bucket.put("text/partial.txt", content)
        with bucket.open("text/partial.txt") as lines:
            first = next(lines)
        # No exception on exit — stream closed properly
        assert first == "line 0"


# ---------------------------------------------------------------------------
# Bucket.stream — binary streaming
# ---------------------------------------------------------------------------


class TestBucketStream:
    def test_stream_yields_bytes(self, bucket: Bucket) -> None:
        """stream() yields bytes chunks that reconstruct the full object."""
        data = b"hello binary world"
        bucket.put("bin/file.bin", data)
        chunks = list(bucket.stream("bin/file.bin"))
        assert b"".join(chunks) == data

    def test_stream_default_chunk_size(self, bucket: Bucket) -> None:
        """A small file is yielded in a single chunk (smaller than threshold)."""
        data = b"small data"
        bucket.put("bin/small.bin", data)
        chunks = list(bucket.stream("bin/small.bin"))
        assert len(chunks) == 1
        assert chunks[0] == data

    def test_stream_custom_chunk_size(self, bucket: Bucket) -> None:
        """stream() splits the object into chunks of at most *chunk_size* bytes."""
        data = b"A" * 100
        bucket.put("bin/chunked.bin", data)
        # chunk_size=30 → expect ceil(100/30) = 4 chunks
        chunks = list(bucket.stream("bin/chunked.bin", chunk_size=30))
        assert len(chunks) == 4
        assert all(len(c) <= 30 for c in chunks)
        assert b"".join(chunks) == data

    def test_stream_reconstructed_data_is_identical(self, bucket: Bucket) -> None:
        """Concatenating all chunks produces the exact original bytes."""
        data = bytes(range(256)) * 50  # 12 800 bytes
        bucket.put("bin/reconstruct.bin", data)
        chunks = list(bucket.stream("bin/reconstruct.bin", chunk_size=1000))
        assert b"".join(chunks) == data

    def test_stream_empty_object(self, bucket: Bucket) -> None:
        """Streaming an empty object yields no chunks."""
        bucket.put("bin/empty.bin", b"")
        chunks = list(bucket.stream("bin/empty.bin"))
        assert chunks == []

    def test_stream_missing_key_raises(self, bucket: Bucket) -> None:
        """stream() on a non-existent key raises ObjectNotFoundError."""
        with pytest.raises(ObjectNotFoundError):
            list(bucket.stream("bin/ghost.bin"))

    def test_stream_empty_key_raises(self, bucket: Bucket) -> None:
        """stream() with an empty key raises ValidationError."""
        with pytest.raises(ValidationError):
            list(bucket.stream(""))

    def test_stream_is_lazy(self, bucket: Bucket) -> None:
        """stream() returns an iterator, not a pre-loaded list."""
        bucket.put("bin/lazy.bin", b"x" * 1000)
        result = bucket.stream("bin/lazy.bin")
        # Should be a generator / iterator, not a list
        assert hasattr(result, "__iter__")
        assert hasattr(result, "__next__")

    def test_stream_closes_connection_after_iteration(self, bucket: Bucket) -> None:
        """Consuming all chunks does not leave dangling connections."""
        bucket.put("bin/full.bin", b"data " * 200)
        chunks = list(bucket.stream("bin/full.bin", chunk_size=100))
        assert b"".join(chunks) == b"data " * 200

    def test_stream_partial_iteration(self, bucket: Bucket) -> None:
        """Stopping iteration early (break) does not raise an error."""
        data = b"chunk" * 100
        bucket.put("bin/partial.bin", data)
        seen: list[bytes] = []
        for chunk in bucket.stream("bin/partial.bin", chunk_size=50):
            seen.append(chunk)
            if len(seen) == 2:
                break
        assert len(seen) == 2


# ---------------------------------------------------------------------------
# Prefix.open / Prefix.stream
# ---------------------------------------------------------------------------


class TestPrefixStreaming:
    def test_prefix_open_uses_full_key(self, bucket: Bucket) -> None:
        """Prefix.open reads from the correct prefixed S3 key."""
        folder = bucket / "logs" / "2025"
        bucket.put("logs/2025/app.log", "INFO start\nINFO end")
        with folder.open("app.log") as lines:
            result = list(lines)
        assert result == ["INFO start", "INFO end"]

    def test_prefix_stream_uses_full_key(self, bucket: Bucket) -> None:
        """Prefix.stream reads from the correct prefixed S3 key."""
        folder = bucket / "media"
        bucket.put("media/clip.bin", b"\xDE\xAD\xBE\xEF")
        chunks = list(folder.stream("clip.bin"))
        assert b"".join(chunks) == b"\xDE\xAD\xBE\xEF"

    def test_prefix_open_missing_key_raises(self, bucket: Bucket) -> None:
        """Prefix.open on a missing key raises ObjectNotFoundError."""
        folder = bucket / "logs"
        with pytest.raises(ObjectNotFoundError):
            with folder.open("ghost.log") as _:
                pass

    def test_prefix_stream_missing_key_raises(self, bucket: Bucket) -> None:
        """Prefix.stream on a missing key raises ObjectNotFoundError."""
        folder = bucket / "bin"
        with pytest.raises(ObjectNotFoundError):
            list(folder.stream("ghost.bin"))

    def test_prefix_open_jsonl(self, bucket: Bucket) -> None:
        """Prefix.open can parse a JSONL file line by line."""
        folder = bucket / "events"
        records = [{"id": i} for i in range(5)]
        content = "\n".join(json.dumps(r) for r in records)
        folder.put("batch.jsonl", content)

        parsed: list[dict] = []
        with folder.open("batch.jsonl") as lines:
            for line in lines:
                parsed.append(json.loads(line))

        assert parsed == records

    def test_prefix_stream_with_chunk_size(self, bucket: Bucket) -> None:
        """Prefix.stream forwards chunk_size correctly."""
        folder = bucket / "chunks"
        data = b"B" * 300
        folder.put("data.bin", data)
        chunks = list(folder.stream("data.bin", chunk_size=100))
        assert len(chunks) == 3
        assert b"".join(chunks) == data
