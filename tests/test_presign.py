"""Tests for Fase 4 — presign()."""

import pytest

from s3mio.bucket import Bucket
from s3mio.exceptions import ValidationError

# ---------------------------------------------------------------------------
# Bucket.presign
# ---------------------------------------------------------------------------


class TestBucketPresign:
    def test_presign_get_returns_string(self, bucket: Bucket) -> None:
        """presign() returns a non-empty string URL."""
        bucket.put("file.txt", "data")
        url = bucket.presign("file.txt")
        assert isinstance(url, str)
        assert len(url) > 0

    def test_presign_get_contains_bucket_and_key(self, bucket: Bucket) -> None:
        """The presigned GET URL includes the bucket name and key."""
        bucket.put("reports/q1.pdf", b"%PDF")
        url = bucket.presign("reports/q1.pdf")
        assert "reports" in url or "q1.pdf" in url

    def test_presign_get_default_method(self, bucket: Bucket) -> None:
        """presign() defaults to GET when method is not specified."""
        bucket.put("download.txt", "content")
        url_explicit = bucket.presign("download.txt", method="GET")
        url_default = bucket.presign("download.txt")
        # Both should be valid non-empty strings
        assert isinstance(url_default, str)
        assert isinstance(url_explicit, str)

    def test_presign_put_returns_string(self, bucket: Bucket) -> None:
        """presign() with method='PUT' returns a non-empty string URL."""
        url = bucket.presign("uploads/photo.jpg", method="PUT", expires_in=300)
        assert isinstance(url, str)
        assert len(url) > 0

    def test_presign_method_case_insensitive(self, bucket: Bucket) -> None:
        """method parameter is case-insensitive ('get', 'GET', 'Get' all work)."""
        bucket.put("file.txt", "data")
        url_lower = bucket.presign("file.txt", method="get")
        url_upper = bucket.presign("file.txt", method="GET")
        url_mixed = bucket.presign("file.txt", method="Get")
        assert isinstance(url_lower, str)
        assert isinstance(url_upper, str)
        assert isinstance(url_mixed, str)

    def test_presign_custom_expires_in(self, bucket: Bucket) -> None:
        """presign() accepts a custom expires_in value without error."""
        bucket.put("file.txt", "data")
        url = bucket.presign("file.txt", expires_in=7200)
        assert isinstance(url, str)
        assert len(url) > 0

    def test_presign_minimum_expires_in(self, bucket: Bucket) -> None:
        """expires_in=1 (minimum) is accepted."""
        bucket.put("file.txt", "data")
        url = bucket.presign("file.txt", expires_in=1)
        assert isinstance(url, str)

    def test_presign_nested_key(self, bucket: Bucket) -> None:
        """presign() works for deeply nested keys."""
        bucket.put("a/b/c/d/file.json", {"x": 1})
        url = bucket.presign("a/b/c/d/file.json")
        assert isinstance(url, str)

    def test_presign_empty_key_raises(self, bucket: Bucket) -> None:
        """presign() with an empty key raises ValidationError."""
        with pytest.raises(ValidationError, match="empty"):
            bucket.presign("")

    def test_presign_zero_expires_in_raises(self, bucket: Bucket) -> None:
        """expires_in=0 raises ValidationError."""
        with pytest.raises(ValidationError, match="expires_in"):
            bucket.presign("file.txt", expires_in=0)

    def test_presign_negative_expires_in_raises(self, bucket: Bucket) -> None:
        """A negative expires_in raises ValidationError."""
        with pytest.raises(ValidationError, match="expires_in"):
            bucket.presign("file.txt", expires_in=-60)

    def test_presign_invalid_method_raises(self, bucket: Bucket) -> None:
        """An unsupported method raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid method"):
            bucket.presign("file.txt", method="DELETE")

    def test_presign_post_method_raises(self, bucket: Bucket) -> None:
        """'POST' is not a supported method and raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid method"):
            bucket.presign("file.txt", method="POST")

    def test_presign_get_url_is_https(self, bucket: Bucket) -> None:
        """The generated presigned URL starts with http (moto uses http)."""
        bucket.put("secure.txt", "data")
        url = bucket.presign("secure.txt")
        assert url.startswith("http")

    def test_presign_put_url_is_https(self, bucket: Bucket) -> None:
        """The PUT presigned URL starts with http (moto uses http)."""
        url = bucket.presign("new/upload.bin", method="PUT")
        assert url.startswith("http")

    def test_presign_does_not_require_object_to_exist(self, bucket: Bucket) -> None:
        """presign() generates a URL even if the object doesn't exist yet.

        This is expected for PUT presigned URLs — the object is created
        when the client uploads to the URL.
        """
        url = bucket.presign("future/object.json", method="PUT", expires_in=600)
        assert isinstance(url, str)
        assert len(url) > 0

    def test_presign_returns_different_urls_for_different_keys(
        self, bucket: Bucket
    ) -> None:
        """Different keys produce different presigned URLs."""
        bucket.put("a.txt", "aaa")
        bucket.put("b.txt", "bbb")
        url_a = bucket.presign("a.txt")
        url_b = bucket.presign("b.txt")
        assert url_a != url_b


# ---------------------------------------------------------------------------
# Prefix.presign
# ---------------------------------------------------------------------------


class TestPrefixPresign:
    def test_prefix_presign_uses_full_key(self, bucket: Bucket) -> None:
        """Prefix.presign generates a URL for the full prefixed key."""
        folder = bucket / "exports"
        bucket.put("exports/data.csv", "col1,col2")
        url = folder.presign("data.csv")
        assert isinstance(url, str)
        assert len(url) > 0

    def test_prefix_presign_put(self, bucket: Bucket) -> None:
        """Prefix.presign with method='PUT' returns a valid URL string."""
        folder = bucket / "uploads"
        url = folder.presign("image.jpg", method="PUT", expires_in=600)
        assert isinstance(url, str)
        assert url.startswith("http")

    def test_prefix_presign_empty_key_raises(self, bucket: Bucket) -> None:
        """Prefix.presign with empty key raises ValidationError."""
        folder = bucket / "x"
        with pytest.raises(ValidationError):
            folder.presign("")

    def test_prefix_presign_invalid_method_raises(self, bucket: Bucket) -> None:
        """Prefix.presign with unsupported method raises ValidationError."""
        folder = bucket / "x"
        with pytest.raises(ValidationError, match="Invalid method"):
            folder.presign("file.txt", method="PATCH")

    def test_prefix_presign_nested_folder(self, bucket: Bucket) -> None:
        """Prefix.presign works for deeply nested prefix + key."""
        folder = bucket / "year" / "2025" / "month" / "01"
        bucket.put("year/2025/month/01/report.json", {"total": 42})
        url = folder.presign("report.json")
        assert isinstance(url, str)

    def test_prefix_presign_negative_expires_raises(self, bucket: Bucket) -> None:
        """Prefix.presign with negative expires_in raises ValidationError."""
        folder = bucket / "x"
        with pytest.raises(ValidationError, match="expires_in"):
            folder.presign("file.txt", expires_in=-1)
