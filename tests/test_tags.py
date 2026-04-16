"""Tests for Fase 3 — set_tags() and get_tags()."""

import pytest

from s3mio.bucket import Bucket
from s3mio.exceptions import ValidationError

# ---------------------------------------------------------------------------
# Bucket.set_tags / Bucket.get_tags
# ---------------------------------------------------------------------------


class TestBucketTags:
    def test_set_and_get_tags(self, bucket: Bucket) -> None:
        """Tags set on an object can be retrieved as an identical dict."""
        bucket.put("file.txt", "content")
        bucket.set_tags("file.txt", {"env": "prod", "owner": "antonio"})
        tags = bucket.get_tags("file.txt")
        assert tags == {"env": "prod", "owner": "antonio"}

    def test_get_tags_empty_when_no_tags(self, bucket: Bucket) -> None:
        """get_tags() returns an empty dict for an object with no tags."""
        bucket.put("untagged.txt", "no tags here")
        tags = bucket.get_tags("untagged.txt")
        assert tags == {}

    def test_set_tags_replaces_existing_tags(self, bucket: Bucket) -> None:
        """Calling set_tags() again replaces all previous tags."""
        bucket.put("file.txt", "data")
        bucket.set_tags("file.txt", {"env": "dev", "team": "backend"})
        bucket.set_tags("file.txt", {"env": "prod"})
        tags = bucket.get_tags("file.txt")
        assert tags == {"env": "prod"}
        assert "team" not in tags

    def test_set_tags_empty_dict_clears_all_tags(self, bucket: Bucket) -> None:
        """Passing an empty dict removes all tags from the object."""
        bucket.put("file.txt", "data")
        bucket.set_tags("file.txt", {"env": "prod"})
        bucket.set_tags("file.txt", {})
        assert bucket.get_tags("file.txt") == {}

    def test_set_tags_single_tag(self, bucket: Bucket) -> None:
        """A single tag is stored and retrieved correctly."""
        bucket.put("solo.txt", "x")
        bucket.set_tags("solo.txt", {"version": "1.0"})
        assert bucket.get_tags("solo.txt") == {"version": "1.0"}

    def test_tags_with_special_characters_in_value(self, bucket: Bucket) -> None:
        """Tag values with spaces and special chars are preserved."""
        bucket.put("doc.pdf", b"%PDF")
        bucket.set_tags("doc.pdf", {"description": "Q1 Report 2025"})
        assert bucket.get_tags("doc.pdf")["description"] == "Q1 Report 2025"

    def test_tags_inline_in_put(self, bucket: Bucket) -> None:
        """Tags passed directly to put() are stored and retrievable."""
        bucket.put("report.pdf", b"%PDF", tags={"env": "staging", "dept": "finance"})
        tags = bucket.get_tags("report.pdf")
        assert tags["env"] == "staging"
        assert tags["dept"] == "finance"

    def test_set_tags_empty_key_raises(self, bucket: Bucket) -> None:
        """set_tags() with an empty key raises ValidationError."""
        with pytest.raises(ValidationError):
            bucket.set_tags("", {"env": "prod"})

    def test_get_tags_empty_key_raises(self, bucket: Bucket) -> None:
        """get_tags() with an empty key raises ValidationError."""
        with pytest.raises(ValidationError):
            bucket.get_tags("")

    def test_multiple_objects_have_independent_tags(self, bucket: Bucket) -> None:
        """Tags are scoped per object — setting one does not affect another."""
        bucket.put("a.txt", "a")
        bucket.put("b.txt", "b")
        bucket.set_tags("a.txt", {"label": "alpha"})
        bucket.set_tags("b.txt", {"label": "beta"})
        assert bucket.get_tags("a.txt") == {"label": "alpha"}
        assert bucket.get_tags("b.txt") == {"label": "beta"}

    def test_get_tags_returns_all_tags(self, bucket: Bucket) -> None:
        """get_tags() returns every tag that was set."""
        bucket.put("multi.txt", "data")
        expected = {f"key{i}": f"val{i}" for i in range(5)}
        bucket.set_tags("multi.txt", expected)
        assert bucket.get_tags("multi.txt") == expected


# ---------------------------------------------------------------------------
# Prefix.set_tags / Prefix.get_tags
# ---------------------------------------------------------------------------


class TestPrefixTags:
    def test_prefix_set_and_get_tags(self, bucket: Bucket) -> None:
        """Prefix.set_tags and get_tags work on the correct prefixed key."""
        folder = bucket / "reports"
        folder.put("q1.pdf", b"%PDF")
        folder.set_tags("q1.pdf", {"quarter": "Q1", "year": "2025"})
        tags = folder.get_tags("q1.pdf")
        assert tags == {"quarter": "Q1", "year": "2025"}

    def test_prefix_tags_use_full_key(self, bucket: Bucket) -> None:
        """Tags operate on the full prefixed key, not the relative one."""
        folder = bucket / "assets"
        bucket.put("assets/logo.png", b"\x89PNG")
        folder.set_tags("logo.png", {"type": "image"})
        # Verify via bucket directly
        assert bucket.get_tags("assets/logo.png") == {"type": "image"}

    def test_prefix_get_tags_empty_when_no_tags(self, bucket: Bucket) -> None:
        """get_tags() via Prefix returns empty dict for untagged object."""
        folder = bucket / "files"
        folder.put("plain.txt", "no tags")
        assert folder.get_tags("plain.txt") == {}

    def test_prefix_set_tags_replaces_via_bucket_get(self, bucket: Bucket) -> None:
        """Tags set via Prefix are visible when reading via Bucket."""
        folder = bucket / "data"
        folder.put("export.csv", "a,b,c")
        folder.set_tags("export.csv", {"env": "prod"})
        assert bucket.get_tags("data/export.csv") == {"env": "prod"}

    def test_prefix_tags_empty_key_raises(self, bucket: Bucket) -> None:
        """Prefix.set_tags with empty key raises ValidationError."""
        folder = bucket / "x"
        with pytest.raises(ValidationError):
            folder.set_tags("", {"k": "v"})

    def test_prefix_get_tags_empty_key_raises(self, bucket: Bucket) -> None:
        """Prefix.get_tags with empty key raises ValidationError."""
        folder = bucket / "x"
        with pytest.raises(ValidationError):
            folder.get_tags("")
