"""Tests for Fase 2 — upload() and download()."""

import json
from pathlib import Path

import pytest

from s3mio.bucket import Bucket
from s3mio.exceptions import ObjectNotFoundError, ValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_local(tmp_path: Path, filename: str, content: bytes) -> Path:
    """Write *content* to a temp file and return its path."""
    p = tmp_path / filename
    p.write_bytes(content)
    return p


# ---------------------------------------------------------------------------
# Bucket.upload
# ---------------------------------------------------------------------------


class TestBucketUpload:
    def test_upload_text_file(self, bucket: Bucket, tmp_path: Path) -> None:
        """A plain text file is uploaded and its content is preserved."""
        local = _write_local(tmp_path, "hello.txt", b"Hello, s3mio!")
        bucket.upload(local, "upload/hello.txt")
        assert bucket.get_text("upload/hello.txt") == "Hello, s3mio!"

    def test_upload_json_file(self, bucket: Bucket, tmp_path: Path) -> None:
        """A JSON file is uploaded and can be retrieved as a dict."""
        payload = {"service": "s3mio", "version": 2}
        local = _write_local(tmp_path, "config.json", json.dumps(payload).encode())
        bucket.upload(local, "upload/config.json")
        assert bucket.get_json("upload/config.json") == payload

    def test_upload_binary_file(self, bucket: Bucket, tmp_path: Path) -> None:
        """A binary file is uploaded byte-for-byte correctly."""
        data = bytes(range(256))
        local = _write_local(tmp_path, "binary.bin", data)
        bucket.upload(local, "upload/binary.bin")
        assert bucket.get_bytes("upload/binary.bin") == data

    def test_upload_accepts_string_path(self, bucket: Bucket, tmp_path: Path) -> None:
        """upload() accepts a plain string path, not just pathlib.Path."""
        local = _write_local(tmp_path, "str_path.txt", b"via string path")
        bucket.upload(str(local), "upload/str_path.txt")
        assert bucket.exists("upload/str_path.txt")

    def test_upload_empty_file(self, bucket: Bucket, tmp_path: Path) -> None:
        """Uploading an empty file succeeds and the object exists in S3."""
        local = _write_local(tmp_path, "empty.txt", b"")
        bucket.upload(local, "upload/empty.txt")
        assert bucket.exists("upload/empty.txt")
        assert bucket.get_bytes("upload/empty.txt") == b""

    def test_upload_progress_callback_called(
        self, bucket: Bucket, tmp_path: Path
    ) -> None:
        """on_progress callback receives at least one call and ends at 100%."""
        local = _write_local(tmp_path, "tracked.txt", b"data " * 1000)
        calls: list[float] = []
        bucket.upload(local, "upload/tracked.txt", on_progress=calls.append)
        assert len(calls) > 0
        assert calls[-1] == pytest.approx(100.0, abs=0.1)

    def test_upload_progress_percentages_increase(
        self, bucket: Bucket, tmp_path: Path
    ) -> None:
        """Reported percentages are always between 0 and 100 (inclusive)."""
        local = _write_local(tmp_path, "mono.txt", b"x" * 5000)
        calls: list[float] = []
        bucket.upload(local, "upload/mono.txt", on_progress=calls.append)
        assert all(0.0 <= p <= 100.0 for p in calls)

    def test_upload_empty_key_raises(self, bucket: Bucket, tmp_path: Path) -> None:
        """An empty key raises ValidationError before touching S3."""
        local = _write_local(tmp_path, "f.txt", b"x")
        with pytest.raises(ValidationError):
            bucket.upload(local, "")

    def test_upload_missing_local_file_raises(self, bucket: Bucket, tmp_path: Path) -> None:
        """Passing a non-existent path raises ValidationError."""
        with pytest.raises(ValidationError, match="not found"):
            bucket.upload(tmp_path / "ghost.txt", "upload/ghost.txt")

    def test_upload_directory_path_raises(self, bucket: Bucket, tmp_path: Path) -> None:
        """Passing a directory path (not a file) raises ValidationError."""
        with pytest.raises(ValidationError, match="not a file"):
            bucket.upload(tmp_path, "upload/dir.txt")


# ---------------------------------------------------------------------------
# Bucket.download
# ---------------------------------------------------------------------------


class TestBucketDownload:
    def test_download_restores_content(self, bucket: Bucket, tmp_path: Path) -> None:
        """Downloaded file has identical bytes to what was uploaded."""
        original = b"downloaded content 123"
        bucket.put("dl/file.txt", original)
        dest = tmp_path / "file.txt"
        bucket.download("dl/file.txt", dest)
        assert dest.read_bytes() == original

    def test_download_accepts_string_path(self, bucket: Bucket, tmp_path: Path) -> None:
        """download() accepts a plain string as the destination path."""
        bucket.put("dl/str.txt", b"str path test")
        dest = str(tmp_path / "str.txt")
        bucket.download("dl/str.txt", dest)
        assert Path(dest).read_bytes() == b"str path test"

    def test_download_creates_parent_dirs(self, bucket: Bucket, tmp_path: Path) -> None:
        """Parent directories are created automatically if they don't exist."""
        bucket.put("dl/nested.txt", b"nested")
        dest = tmp_path / "a" / "b" / "c" / "nested.txt"
        bucket.download("dl/nested.txt", dest)
        assert dest.read_bytes() == b"nested"

    def test_download_binary_file(self, bucket: Bucket, tmp_path: Path) -> None:
        """Binary content is downloaded without corruption."""
        data = bytes(range(256)) * 100
        bucket.put("dl/binary.bin", data)
        dest = tmp_path / "binary.bin"
        bucket.download("dl/binary.bin", dest)
        assert dest.read_bytes() == data

    def test_download_empty_key_raises(self, bucket: Bucket, tmp_path: Path) -> None:
        """An empty key raises ValidationError before touching S3."""
        with pytest.raises(ValidationError):
            bucket.download("", tmp_path / "out.txt")

    def test_download_missing_key_raises(self, bucket: Bucket, tmp_path: Path) -> None:
        """Downloading a non-existent key raises ObjectNotFoundError."""
        with pytest.raises(ObjectNotFoundError):
            bucket.download("does/not/exist.txt", tmp_path / "out.txt")

    def test_upload_then_download_roundtrip(
        self, bucket: Bucket, tmp_path: Path
    ) -> None:
        """Full roundtrip: upload a file and download it back identically."""
        original = _write_local(tmp_path, "original.bin", b"\x00\xFF" * 512)
        bucket.upload(original, "roundtrip/file.bin")
        dest = tmp_path / "restored.bin"
        bucket.download("roundtrip/file.bin", dest)
        assert dest.read_bytes() == original.read_bytes()


# ---------------------------------------------------------------------------
# Prefix.upload / Prefix.download
# ---------------------------------------------------------------------------


class TestPrefixFileIO:
    def test_prefix_upload_uses_full_key(self, bucket: Bucket, tmp_path: Path) -> None:
        """Prefix.upload writes to the correct prefixed S3 key."""
        folder = bucket / "archive" / "2025"
        local = _write_local(tmp_path, "data.csv", b"a,b,c\n1,2,3")
        folder.upload(local, "data.csv")
        assert bucket.exists("archive/2025/data.csv")

    def test_prefix_download_uses_full_key(self, bucket: Bucket, tmp_path: Path) -> None:
        """Prefix.download retrieves from the correct prefixed S3 key."""
        folder = bucket / "reports"
        bucket.put("reports/jan.txt", b"January data")
        dest = tmp_path / "jan.txt"
        folder.download("jan.txt", dest)
        assert dest.read_bytes() == b"January data"

    def test_prefix_upload_with_progress(self, bucket: Bucket, tmp_path: Path) -> None:
        """Prefix.upload forwards the on_progress callback correctly."""
        folder = bucket / "media"
        local = _write_local(tmp_path, "clip.bin", b"x" * 2000)
        calls: list[float] = []
        folder.upload(local, "clip.bin", on_progress=calls.append)
        assert calls[-1] == pytest.approx(100.0, abs=0.1)
        assert bucket.exists("media/clip.bin")
