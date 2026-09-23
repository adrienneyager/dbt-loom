import json
import sys
from pathlib import Path
from types import SimpleNamespace

from typing import Dict, Generator, Tuple
from urllib.parse import urlparse

import pytest
from dbt_loom.clients import is_gzipped
from dbt_loom.config import (
    FileReferenceConfig,
    ManifestReference,
    ManifestReferenceType,
    LoomConfigurationError,
)
from dbt_loom.cache import ManifestCache
from dbt_loom.clients.dbx import DatabricksClient, DatabricksReferenceConfig
from dbt_loom.clients.s3 import S3ReferenceConfig
from dbt_loom.manifests import ManifestLoader, UnknownManifestPathType

DBX_CONFIG = DatabricksReferenceConfig(path="/Volumes/a/manifest.json")


@pytest.fixture
def example_file() -> Generator[Tuple[Path, Dict], None, None]:
    example_content = {"foo": "bar"}
    path = Path("example.json")
    with open(path, "w") as file:
        json.dump(example_content, file)
    yield path, example_content
    path.unlink()


def test_load_from_local_filesystem_pass(example_file):
    """Test that ManifestLoader can load a local JSON file."""

    path, example_content = example_file

    file_config = FileReferenceConfig(
        path=urlparse("file://" + str(Path(path).absolute()))
    )

    output = ManifestLoader.load_from_local_filesystem(file_config)

    assert output == example_content


def test_load_from_local_filesystem_local_path(example_file):
    """Test that ManifestLoader can load a local JSON file."""

    path, example_content = example_file

    file_config = FileReferenceConfig(path=str(path))  # type: ignore

    output = ManifestLoader.load_from_local_filesystem(file_config)

    assert output == example_content


def test_load_from_path_fails_invalid_scheme(example_file):
    """
    est that ManifestLoader will raise the appropriate exception if an invalid
    scheme is applied.
    """

    file_config = FileReferenceConfig(
        path=urlparse("ftp://example.com/example.json"),
    )  # type: ignore

    with pytest.raises(UnknownManifestPathType):
        ManifestLoader.load_from_path(file_config)


def test_load_from_remote_pass(example_file):
    """Test that ManifestLoader can load a remote JSON file via HTTP(S)."""

    _, example_content = example_file

    file_config = FileReferenceConfig(
        path=urlparse(
            "https://s3.us-east-2.amazonaws.com/com.nicholasyager.dbt-loom/example.json"
        ),
    )

    output = ManifestLoader.load_from_http(file_config)

    assert output == example_content


def test_load_from_remote_gz_pass(example_file):
    """Test that ManifestLoader can load a remote gzipped JSON file via HTTP(S)."""

    _, example_content = example_file

    file_config = FileReferenceConfig(
        path=urlparse(
            "https://s3.us-east-2.amazonaws.com/com.nicholasyager.dbt-loom/manifest.json.gz"
        ),
    )

    output = ManifestLoader.load_from_http(file_config)

    assert output["metadata"]["invocation_id"] == "3557bc11-ce26-4d9c-90ae-28ee866dfc21"


def test_manifest_loader_selection(example_file):
    """Confirm scheme parsing works for picking the manifest loader."""
    _, example_content = example_file
    manifest_loader = ManifestLoader()

    file_config = FileReferenceConfig(
        path=urlparse(
            "https://s3.us-east-2.amazonaws.com/com.nicholasyager.dbt-loom/example.json"
        ),
    )

    manifest_reference = ManifestReference(
        name="example", type=ManifestReferenceType.file, config=file_config
    )

    manifest = manifest_loader.load(manifest_reference)

    assert manifest == example_content


def test_load_from_local_filesystem_optional_missing():
    """If the manifest file does not exist, it should not raise an error if optional=True."""
    file_config = FileReferenceConfig(path="not_exist_manifest.json")  # type: ignore
    manifest_reference = ManifestReference(
        name="missing",
        type=ManifestReferenceType.file,
        config=file_config,
        optional=True,
    )
    manifest_loader = ManifestLoader()
    manifest = manifest_loader.load(manifest_reference)
    assert manifest is None


def test_load_from_local_filesystem_not_optional_missing():
    """If the manifest file does not exist, it should raise an error if optional=False."""
    file_config = FileReferenceConfig(path="not_exist_manifest.json")  # type: ignore
    manifest_reference = ManifestReference(
        name="missing",
        type=ManifestReferenceType.file,
        config=file_config,
        optional=False,
    )
    manifest_loader = ManifestLoader()
    with pytest.raises(LoomConfigurationError):
        manifest_loader.load(manifest_reference)


def test_manifest_reference_resolves_databricks_config():
    """Verify that type=databricks produces DatabricksReferenceConfig, not FileReferenceConfig."""
    ref = ManifestReference(
        name="test_dbx",
        type=ManifestReferenceType.databricks,
        config={"path": "/Volumes/my_catalog/my_schema/my_volume/manifest.json.gz"},  # type: ignore
    )
    assert isinstance(ref.config, DatabricksReferenceConfig)
    assert ref.config.path == "/Volumes/my_catalog/my_schema/my_volume/manifest.json.gz"


def test_manifest_reference_resolves_file_config():
    """Verify that type=file still produces FileReferenceConfig."""
    ref = ManifestReference(
        name="test_file",
        type=ManifestReferenceType.file,
        config={"path": "manifest.json"},  # type: ignore
    )
    assert isinstance(ref.config, FileReferenceConfig)


def test_gzip_detection_works():
    """Confirm that is_gzipped returns true for gzipped magic bytes."""
    assert is_gzipped(b"\x1f\x8b")


def test_manifest_cache_round_trip(tmp_path):
    """The cache returns a manifest only when the token matches."""

    cache = ManifestCache(DBX_CONFIG, directory=tmp_path)
    assert cache.read("abc") is None

    cache.write({"nodes": {}}, "abc")
    assert cache.read("abc") == {"nodes": {}}
    assert cache.read("def") is None


def test_manifest_cache_isolates_references(tmp_path):
    """Each manifest reference gets its own cache directory."""

    def cache(config):
        return ManifestCache(config, directory=tmp_path)

    cache(DatabricksReferenceConfig(path="/a.json")).write({"a": 1}, "token")
    cache(DatabricksReferenceConfig(path="/b.json")).write({"b": 2}, "token")

    assert cache(DatabricksReferenceConfig(path="/a.json")).read("token") == {"a": 1}
    assert cache(DatabricksReferenceConfig(path="/b.json")).read("token") == {"b": 2}
    assert cache(DatabricksReferenceConfig(path="/c.json")).read("token") is None

    # Configs of different source types never collide either.
    s3_config = S3ReferenceConfig(bucket_name="a", object_name="b")
    assert cache(s3_config).read("token") is None


def test_manifest_cache_ttl(tmp_path):
    """Within the TTL the cache is served without a token check."""

    cache = ManifestCache(DBX_CONFIG, directory=tmp_path)
    cache.write({"nodes": {}}, "token")

    assert cache.read_if_fresh(0) is None  # TTL disabled
    assert cache.read_if_fresh(3600) == {"nodes": {}}

    # An expired entry falls back to token validation.
    lock = json.loads(cache.lock_path.read_text())
    lock["cached_at"] = "2020-01-01T00:00:00+00:00"
    cache.lock_path.write_text(json.dumps(lock))

    assert cache.read_if_fresh(3600) is None
    assert cache.read("token") == {"nodes": {}}


@pytest.mark.parametrize("cache_ttl", [0, 3600])
def test_manifest_loader_uses_cache(tmp_path, monkeypatch, cache_ttl):
    """
    A valid cache entry skips the download, whether it was validated by a TTL
    or by a matching token.
    """

    monkeypatch.setattr(
        "dbt_loom.manifests.ManifestCache",
        lambda config: ManifestCache(config, directory=tmp_path),
    )

    def explode(*args, **kwargs):
        raise AssertionError("The manifest should have been loaded from cache.")

    reference = ManifestReference(
        name="example",
        type=ManifestReferenceType.databricks,
        config={"path": DBX_CONFIG.path},
        cache_ttl=cache_ttl,
    )

    ManifestCache(DBX_CONFIG, directory=tmp_path).write({"nodes": {}}, "token-1")

    loader = ManifestLoader()
    loader.loading_functions[ManifestReferenceType.databricks] = explode
    # A fresh TTL cache must not ask the source for a token at all.
    monkeypatch.setattr(
        loader, "get_cache_token", explode if cache_ttl else lambda _: "token-1"
    )

    assert loader.load(reference) == {"nodes": {}}


def test_manifest_loader_refreshes_ttl_on_token_hit(tmp_path, monkeypatch):
    """A token hit restarts the TTL window without rewriting the manifest."""

    monkeypatch.setattr(
        "dbt_loom.manifests.ManifestCache",
        lambda config: ManifestCache(config, directory=tmp_path),
    )

    reference = ManifestReference(
        name="example",
        type=ManifestReferenceType.databricks,
        config={"path": DBX_CONFIG.path},
        cache_ttl=60,
    )

    cache = ManifestCache(DBX_CONFIG, directory=tmp_path)
    cache.write({"nodes": {}}, "token-1")

    # Expire the TTL, so the token check is what serves the manifest.
    lock = json.loads(cache.lock_path.read_text())
    lock["cached_at"] = "2020-01-01T00:00:00+00:00"
    cache.lock_path.write_text(json.dumps(lock))
    manifest_written_at = cache.manifest_path.stat().st_mtime_ns

    loader = ManifestLoader()
    monkeypatch.setattr(loader, "get_cache_token", lambda _: "token-1")

    assert loader.load(reference) == {"nodes": {}}

    # The TTL now covers the manifest again, and it was not rewritten.
    assert cache.read_if_fresh(60) == {"nodes": {}}
    assert cache.manifest_path.stat().st_mtime_ns == manifest_written_at


def test_databricks_cache_token_branches(monkeypatch):
    """Each Databricks path style uses the matching metadata API."""

    workspace = SimpleNamespace(
        workspace=SimpleNamespace(
            get_status=lambda path: SimpleNamespace(modified_at=1, size=2)
        ),
        dbfs=SimpleNamespace(
            get_status=lambda path: SimpleNamespace(modification_time=3, file_size=4)
        ),
        files=SimpleNamespace(
            get_metadata=lambda path: SimpleNamespace(last_modified="Mon, 1 Jan 2026")
        ),
    )
    # The Databricks SDK ships with dbt-databricks, so it may be absent here.
    sdk = SimpleNamespace(WorkspaceClient=lambda: workspace)
    monkeypatch.setitem(sys.modules, "databricks", SimpleNamespace(sdk=sdk))
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)

    token = DatabricksClient(path="/Workspace/Users/a/manifest.json").get_cache_token()
    assert token == "1-2"
    assert DatabricksClient(path="/dbfs/tmp/manifest.json").get_cache_token() == "3-4"
    assert DatabricksClient(path=DBX_CONFIG.path).get_cache_token() == "Mon, 1 Jan 2026"
