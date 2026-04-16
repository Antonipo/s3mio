"""Tests for Fase 1 — Bucket and Prefix core operations."""

import pytest

from s3mio import S3
from s3mio._types import ObjectInfo
from s3mio.bucket import Bucket, Prefix
from s3mio.exceptions import (
    BucketNotFoundError,
    ObjectNotFoundError,
    S3OperationError,
    ValidationError,
)

from .conftest import TEST_BUCKET

# ---------------------------------------------------------------------------
# S3 client
# ---------------------------------------------------------------------------


class TestS3Client:
    def test_bucket_returns_bucket_instance(self, s3: S3) -> None:
        b = s3.bucket(TEST_BUCKET)
        assert isinstance(b, Bucket)
        assert b.name == TEST_BUCKET

    def test_bucket_cached_identity(self, s3: S3) -> None:
        # Each call returns a new Bucket object (lightweight wrapper, not cached)
        b1 = s3.bucket(TEST_BUCKET)
        b2 = s3.bucket(TEST_BUCKET)
        assert b1.name == b2.name

    def test_express_bucket_detected(self, s3: S3) -> None:
        express = s3.bucket("my-data--use1-az4--x-s3")
        assert express.express is True

    def test_regular_bucket_not_express(self, s3: S3) -> None:
        b = s3.bucket(TEST_BUCKET)
        assert b.express is False


# ---------------------------------------------------------------------------
# put — smart type detection
# ---------------------------------------------------------------------------


class TestBucketPut:
    def test_put_dict_stores_json(self, bucket: Bucket) -> None:
        bucket.put("data.json", {"name": "antonio", "age": 30})
        raw = bucket.get_bytes("data.json")
        assert b'"name"' in raw
        assert b'"antonio"' in raw

    def test_put_list_stores_json(self, bucket: Bucket) -> None:
        bucket.put("items.json", [1, 2, 3])
        raw = bucket.get_bytes("items.json")
        assert b"[1, 2, 3]" in raw

    def test_put_str_stores_text(self, bucket: Bucket) -> None:
        bucket.put("note.txt", "hello world")
        assert bucket.get_text("note.txt") == "hello world"

    def test_put_bytes_stores_binary(self, bucket: Bucket) -> None:
        data = b"\x00\x01\x02\x03"
        bucket.put("raw.bin", data)
        assert bucket.get_bytes("raw.bin") == data

    def test_put_bytearray_stores_binary(self, bucket: Bucket) -> None:
        data = bytearray(b"\xde\xad\xbe\xef")
        bucket.put("raw.bin", data)
        assert bucket.get_bytes("raw.bin") == bytes(data)

    def test_put_with_metadata(self, bucket: Bucket) -> None:
        bucket.put("doc.txt", "content", metadata={"author": "antonio"})
        # exists without error — metadata checked in Fase 4 (head)
        assert bucket.exists("doc.txt")

    def test_put_with_tags(self, bucket: Bucket) -> None:
        bucket.put("doc.txt", "content", tags={"env": "prod"})
        assert bucket.exists("doc.txt")

    def test_put_empty_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ValidationError):
            bucket.put("", "data")

    def test_put_unsupported_type_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ValidationError, match="Unsupported data type"):
            bucket.put("bad.bin", 12345)  # type: ignore[arg-type]

    def test_put_unicode_content(self, bucket: Bucket) -> None:
        bucket.put("unicode.json", {"msg": "héllo wörld 🎉"})
        obj = bucket.get_json("unicode.json")
        assert obj["msg"] == "héllo wörld 🎉"


# ---------------------------------------------------------------------------
# get_json / get_text / get_bytes
# ---------------------------------------------------------------------------


class TestBucketGet:
    def test_get_json_returns_dict(self, bucket: Bucket) -> None:
        bucket.put("cfg.json", {"key": "value"})
        obj = bucket.get_json("cfg.json")
        assert obj == {"key": "value"}

    def test_get_json_returns_list(self, bucket: Bucket) -> None:
        bucket.put("arr.json", [1, 2, 3])
        assert bucket.get_json("arr.json") == [1, 2, 3]

    def test_get_text_returns_str(self, bucket: Bucket) -> None:
        bucket.put("hello.txt", "world")
        assert bucket.get_text("hello.txt") == "world"

    def test_get_bytes_returns_bytes(self, bucket: Bucket) -> None:
        bucket.put("bin.bin", b"\xff\xfe")
        assert bucket.get_bytes("bin.bin") == b"\xff\xfe"

    def test_get_json_missing_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ObjectNotFoundError) as exc_info:
            bucket.get_json("nonexistent.json")
        assert "nonexistent.json" in str(exc_info.value)
        assert TEST_BUCKET in str(exc_info.value)

    def test_get_text_missing_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ObjectNotFoundError):
            bucket.get_text("ghost.txt")

    def test_get_bytes_missing_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ObjectNotFoundError):
            bucket.get_bytes("ghost.bin")

    def test_get_json_invalid_json_raises(self, bucket: Bucket) -> None:
        bucket.put("bad.json", b"not-json!!!")
        with pytest.raises(S3OperationError, match="not valid JSON"):
            bucket.get_json("bad.json")

    def test_get_bytes_empty_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ValidationError):
            bucket.get_bytes("")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestBucketDelete:
    def test_delete_removes_object(self, bucket: Bucket) -> None:
        bucket.put("to-delete.txt", "bye")
        assert bucket.exists("to-delete.txt")
        bucket.delete("to-delete.txt")
        assert not bucket.exists("to-delete.txt")

    def test_delete_empty_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ValidationError):
            bucket.delete("")

    def test_delete_nonexistent_is_idempotent(self, bucket: Bucket) -> None:
        # S3 delete on a missing key is a no-op — s3mio should not raise
        bucket.delete("does-not-exist.txt")


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


class TestBucketExists:
    def test_exists_true_for_present_object(self, bucket: Bucket) -> None:
        bucket.put("present.txt", "here")
        assert bucket.exists("present.txt") is True

    def test_exists_false_for_missing_object(self, bucket: Bucket) -> None:
        assert bucket.exists("missing.txt") is False

    def test_exists_empty_key_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ValidationError):
            bucket.exists("")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


class TestBucketList:
    def test_list_returns_object_info(self, bucket: Bucket) -> None:
        bucket.put("a.txt", "a")
        bucket.put("b.txt", "b")
        items = bucket.list()
        assert len(items) == 2
        assert all(isinstance(i, ObjectInfo) for i in items)

    def test_list_with_prefix_filters(self, bucket: Bucket) -> None:
        bucket.put("logs/app.log", "log1")
        bucket.put("logs/error.log", "log2")
        bucket.put("data/file.csv", "csv")
        items = bucket.list(prefix="logs/")
        assert len(items) == 2
        assert all(i.key.startswith("logs/") for i in items)

    def test_list_empty_bucket_returns_empty(self, bucket: Bucket) -> None:
        assert bucket.list() == []

    def test_list_object_info_fields(self, bucket: Bucket) -> None:
        bucket.put("info.txt", "hello")
        items = bucket.list()
        obj = items[0]
        assert obj.key == "info.txt"
        assert obj.size == len(b"hello")
        assert obj.etag != ""
        assert obj.last_modified is not None
        assert obj.storage_class != ""


# ---------------------------------------------------------------------------
# Prefix — / operator and folder navigation
# ---------------------------------------------------------------------------


class TestPrefix:
    def test_slash_operator_returns_prefix(self, bucket: Bucket) -> None:
        folder = bucket / "dev"
        assert isinstance(folder, Prefix)
        assert folder.full_prefix == "dev/"

    def test_chained_slash_builds_nested_prefix(self, bucket: Bucket) -> None:
        folder = bucket / "dev" / "proyecto1"
        assert folder.full_prefix == "dev/proyecto1/"

    def test_prefix_put_uses_full_key(self, bucket: Bucket) -> None:
        folder = bucket / "dev" / "proyecto1"
        folder.put("demo.txt", "hello")
        assert bucket.exists("dev/proyecto1/demo.txt")

    def test_prefix_get_text(self, bucket: Bucket) -> None:
        folder = bucket / "notes"
        folder.put("readme.txt", "hi there")
        assert folder.get_text("readme.txt") == "hi there"

    def test_prefix_get_json(self, bucket: Bucket) -> None:
        folder = bucket / "config"
        folder.put("settings.json", {"debug": True})
        assert folder.get_json("settings.json") == {"debug": True}

    def test_prefix_get_bytes(self, bucket: Bucket) -> None:
        folder = bucket / "assets"
        folder.put("icon.png", b"\x89PNG")
        assert folder.get_bytes("icon.png") == b"\x89PNG"

    def test_prefix_delete(self, bucket: Bucket) -> None:
        folder = bucket / "tmp"
        folder.put("file.txt", "data")
        folder.delete("file.txt")
        assert not folder.exists("file.txt")

    def test_prefix_exists(self, bucket: Bucket) -> None:
        folder = bucket / "assets"
        folder.put("logo.png", b"\x89PNG")
        assert folder.exists("logo.png") is True
        assert folder.exists("missing.png") is False

    def test_prefix_list(self, bucket: Bucket) -> None:
        folder = bucket / "reports"
        folder.put("jan.csv", "data1")
        folder.put("feb.csv", "data2")
        bucket.put("other.txt", "outside")  # should NOT appear
        items = folder.list()
        assert len(items) == 2
        assert all(i.key.startswith("reports/") for i in items)

    def test_prefix_iter(self, bucket: Bucket) -> None:
        folder = bucket / "logs"
        folder.put("a.log", "aaa")
        folder.put("b.log", "bbb")
        keys = [obj.key for obj in folder]
        assert "logs/a.log" in keys
        assert "logs/b.log" in keys

    def test_prefix_delete_all_removes_objects(self, bucket: Bucket) -> None:
        folder = bucket / "tmp"
        folder.put("f1.txt", "a")
        folder.put("f2.txt", "b")
        folder.put("f3.txt", "c")
        bucket.put("keep.txt", "safe")  # outside prefix
        result = folder.delete_all()
        assert len(result) == 3
        assert folder.list() == []
        assert bucket.exists("keep.txt")  # untouched

    def test_prefix_delete_all_empty_returns_empty_result(self, bucket: Bucket) -> None:
        folder = bucket / "empty"
        assert len(folder.delete_all()) == 0

    def test_prefix_method_equivalent_to_slash(self, bucket: Bucket) -> None:
        p1 = bucket / "logs" / "2025"
        p2 = bucket.prefix("logs/2025/")
        assert p1.full_prefix == p2.full_prefix

    def test_prefix_empty_segment_raises(self, bucket: Bucket) -> None:
        with pytest.raises(ValidationError):
            bucket / ""

    def test_prefix_deep_nesting(self, bucket: Bucket) -> None:
        deep = bucket / "a" / "b" / "c" / "d"
        deep.put("file.txt", "deep")
        assert bucket.exists("a/b/c/d/file.txt")

    def test_prefix_repr(self, bucket: Bucket) -> None:
        folder = bucket / "dev"
        assert "dev/" in repr(folder)
        assert TEST_BUCKET in repr(folder)


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


class TestErrorMapping:
    def test_get_from_nonexistent_bucket_raises_bucket_not_found(
        self, s3: S3
    ) -> None:
        ghost = s3.bucket("ghost-bucket-xyz")
        with pytest.raises(BucketNotFoundError):
            ghost.get_bytes("any.txt")

    def test_object_not_found_has_key_and_bucket(self, bucket: Bucket) -> None:
        with pytest.raises(ObjectNotFoundError) as exc_info:
            bucket.get_bytes("nope.txt")
        err = exc_info.value
        assert err.key == "nope.txt"
        assert err.bucket == TEST_BUCKET
