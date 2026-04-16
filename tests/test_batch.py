"""Tests for Fase 3 — delete_many(), copy(), copy_many()."""

import pytest

from s3mio.bucket import Bucket
from s3mio.exceptions import ObjectNotFoundError, ValidationError

# ---------------------------------------------------------------------------
# Bucket.delete_many
# ---------------------------------------------------------------------------


class TestBucketDeleteMany:
    def test_delete_many_removes_all_keys(self, bucket: Bucket) -> None:
        """All specified keys are deleted from the bucket."""
        keys = ["tmp/a.json", "tmp/b.json", "tmp/c.json"]
        for k in keys:
            bucket.put(k, "data")
        deleted = bucket.delete_many(keys)
        assert deleted == 3
        for k in keys:
            assert not bucket.exists(k)

    def test_delete_many_returns_count(self, bucket: Bucket) -> None:
        """Return value equals the number of keys passed."""
        for i in range(5):
            bucket.put(f"item/{i}.txt", "x")
        count = bucket.delete_many([f"item/{i}.txt" for i in range(5)])
        assert count == 5

    def test_delete_many_empty_list_is_noop(self, bucket: Bucket) -> None:
        """Passing an empty list returns 0 and raises no errors."""
        assert bucket.delete_many([]) == 0

    def test_delete_many_preserves_other_objects(self, bucket: Bucket) -> None:
        """Objects not in the key list are untouched."""
        bucket.put("keep/important.txt", "safe")
        bucket.put("tmp/trash.txt", "gone")
        bucket.delete_many(["tmp/trash.txt"])
        assert bucket.exists("keep/important.txt")

    def test_delete_many_nonexistent_keys_is_idempotent(self, bucket: Bucket) -> None:
        """Deleting non-existent keys does not raise an error (S3 is idempotent)."""
        count = bucket.delete_many(["ghost/a.txt", "ghost/b.txt"])
        assert count == 2

    def test_delete_many_mixed_existing_and_missing(self, bucket: Bucket) -> None:
        """A mix of existing and missing keys is handled without error."""
        bucket.put("mix/real.txt", "here")
        count = bucket.delete_many(["mix/real.txt", "mix/ghost.txt"])
        assert count == 2
        assert not bucket.exists("mix/real.txt")

    def test_delete_many_large_batch_chunked(self, bucket: Bucket) -> None:
        """Batches larger than 1000 are split into multiple requests correctly."""
        n = 1100
        keys = [f"bulk/{i}.txt" for i in range(n)]
        for k in keys:
            bucket.put(k, "x")
        deleted = bucket.delete_many(keys)
        assert deleted == n
        assert bucket.list(prefix="bulk/") == []


# ---------------------------------------------------------------------------
# Bucket.copy
# ---------------------------------------------------------------------------


class TestBucketCopy:
    def test_copy_within_same_bucket(self, bucket: Bucket) -> None:
        """An object is copied to a new key within the same bucket."""
        bucket.put("src/file.json", {"value": 42})
        bucket.copy("src/file.json", "dst/file.json")
        assert bucket.get_json("dst/file.json") == {"value": 42}

    def test_copy_source_still_exists(self, bucket: Bucket) -> None:
        """The source object is not deleted after copy."""
        bucket.put("original.txt", "keep me")
        bucket.copy("original.txt", "copy.txt")
        assert bucket.exists("original.txt")
        assert bucket.get_text("original.txt") == "keep me"

    def test_copy_binary_object(self, bucket: Bucket) -> None:
        """Binary objects are copied without data corruption."""
        data = bytes(range(256))
        bucket.put("src/binary.bin", data)
        bucket.copy("src/binary.bin", "dst/binary.bin")
        assert bucket.get_bytes("dst/binary.bin") == data

    def test_copy_overwrites_existing_dest(self, bucket: Bucket) -> None:
        """Copying to an existing key overwrites it."""
        bucket.put("a.txt", "original")
        bucket.put("b.txt", "old value")
        bucket.copy("a.txt", "b.txt")
        assert bucket.get_text("b.txt") == "original"

    def test_copy_empty_src_key_raises(self, bucket: Bucket) -> None:
        """An empty source key raises ValidationError."""
        with pytest.raises(ValidationError, match="Source"):
            bucket.copy("", "dst.txt")

    def test_copy_empty_dest_key_raises(self, bucket: Bucket) -> None:
        """An empty destination key raises ValidationError."""
        bucket.put("src.txt", "data")
        with pytest.raises(ValidationError, match="Destination"):
            bucket.copy("src.txt", "")

    def test_copy_nonexistent_src_raises(self, bucket: Bucket) -> None:
        """Copying a non-existent source raises ObjectNotFoundError."""
        with pytest.raises(ObjectNotFoundError):
            bucket.copy("ghost.txt", "dest.txt")


# ---------------------------------------------------------------------------
# Bucket.copy_many
# ---------------------------------------------------------------------------


class TestBucketCopyMany:
    def test_copy_many_copies_all_pairs(self, bucket: Bucket) -> None:
        """All (src, dest) pairs are copied correctly."""
        pairs = [
            ("raw/jan.csv", "processed/jan.csv"),
            ("raw/feb.csv", "processed/feb.csv"),
            ("raw/mar.csv", "processed/mar.csv"),
        ]
        for src, _ in pairs:
            bucket.put(src, f"content of {src}")

        count = bucket.copy_many(pairs)
        assert count == 3
        for src, dst in pairs:
            assert bucket.get_text(dst) == f"content of {src}"

    def test_copy_many_returns_count(self, bucket: Bucket) -> None:
        """Return value equals the number of pairs."""
        bucket.put("a.txt", "a")
        bucket.put("b.txt", "b")
        count = bucket.copy_many([("a.txt", "a_copy.txt"), ("b.txt", "b_copy.txt")])
        assert count == 2

    def test_copy_many_empty_list_is_noop(self, bucket: Bucket) -> None:
        """Passing an empty list returns 0 and raises no errors."""
        assert bucket.copy_many([]) == 0

    def test_copy_many_sources_still_exist(self, bucket: Bucket) -> None:
        """Source objects are not deleted after copy_many."""
        bucket.put("src/x.txt", "x")
        bucket.copy_many([("src/x.txt", "dst/x.txt")])
        assert bucket.exists("src/x.txt")

    def test_copy_many_stops_on_missing_src(self, bucket: Bucket) -> None:
        """Raises ObjectNotFoundError if any source key does not exist."""
        bucket.put("exists.txt", "data")
        with pytest.raises(ObjectNotFoundError):
            bucket.copy_many([("exists.txt", "exists_copy.txt"), ("ghost.txt", "ghost_copy.txt")])


# ---------------------------------------------------------------------------
# Prefix.delete_many
# ---------------------------------------------------------------------------


class TestPrefixDeleteMany:
    def test_prefix_delete_many_uses_full_key(self, bucket: Bucket) -> None:
        """Prefix.delete_many prepends the prefix to each key."""
        folder = bucket / "cache"
        folder.put("a.json", "a")
        folder.put("b.json", "b")
        deleted = folder.delete_many(["a.json", "b.json"])
        assert deleted == 2
        assert not bucket.exists("cache/a.json")
        assert not bucket.exists("cache/b.json")

    def test_prefix_delete_many_does_not_touch_other_prefix(
        self, bucket: Bucket
    ) -> None:
        """Objects outside the prefix are not affected."""
        bucket.put("keep/safe.txt", "safe")
        folder = bucket / "cache"
        folder.put("del.txt", "bye")
        folder.delete_many(["del.txt"])
        assert bucket.exists("keep/safe.txt")

    def test_prefix_delete_many_empty_list(self, bucket: Bucket) -> None:
        """Empty list returns 0."""
        folder = bucket / "empty"
        assert folder.delete_many([]) == 0


# ---------------------------------------------------------------------------
# Prefix.copy / Prefix.copy_many
# ---------------------------------------------------------------------------


class TestPrefixCopy:
    def test_prefix_copy_within_same_prefix(self, bucket: Bucket) -> None:
        """Prefix.copy copies src → dest both under the same prefix."""
        folder = bucket / "drafts"
        folder.put("v1.txt", "first draft")
        folder.copy("v1.txt", "v2.txt")
        assert bucket.exists("drafts/v1.txt")
        assert bucket.get_text("drafts/v2.txt") == "first draft"

    def test_prefix_copy_source_key_prepended(self, bucket: Bucket) -> None:
        """Source key is automatically prefixed — no manual path needed."""
        folder = bucket / "images"
        bucket.put("images/logo.png", b"\x89PNG")
        folder.copy("logo.png", "logo_backup.png")
        assert bucket.get_bytes("images/logo_backup.png") == b"\x89PNG"

    def test_prefix_copy_many_all_pairs_copied(self, bucket: Bucket) -> None:
        """Prefix.copy_many copies all pairs under the prefix."""
        folder = bucket / "data"
        folder.put("a.csv", "col1,col2")
        folder.put("b.csv", "col3,col4")
        count = folder.copy_many([("a.csv", "a_bak.csv"), ("b.csv", "b_bak.csv")])
        assert count == 2
        assert bucket.exists("data/a_bak.csv")
        assert bucket.exists("data/b_bak.csv")

    def test_prefix_copy_many_empty_list(self, bucket: Bucket) -> None:
        """Empty pairs list returns 0."""
        folder = bucket / "x"
        assert folder.copy_many([]) == 0


# ---------------------------------------------------------------------------
# Prefix.delete_all (refactored to use delete_many)
# ---------------------------------------------------------------------------


class TestPrefixDeleteAll:
    def test_delete_all_still_works_after_refactor(self, bucket: Bucket) -> None:
        """delete_all() correctly removes all objects under the prefix."""
        folder = bucket / "cleanup"
        for i in range(10):
            folder.put(f"file{i}.txt", "data")
        deleted = folder.delete_all()
        assert deleted == 10
        assert folder.list() == []

    def test_delete_all_does_not_touch_sibling_prefix(self, bucket: Bucket) -> None:
        """Objects under a sibling prefix are not deleted."""
        (bucket / "keep").put("safe.txt", "safe")
        folder = bucket / "trash"
        folder.put("a.txt", "gone")
        folder.delete_all()
        assert bucket.exists("keep/safe.txt")

    def test_delete_all_empty_prefix_returns_zero(self, bucket: Bucket) -> None:
        """delete_all() on an empty prefix returns 0 without error."""
        assert (bucket / "empty").delete_all() == 0
