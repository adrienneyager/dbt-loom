import json
from pathlib import Path

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
from dbt_loom.clients.dbx import DatabricksReferenceConfig
from dbt_loom.manifests import ManifestLoader, UnknownManifestPathType


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
        path=urlparse(Path(path).absolute().as_uri())
    )

    output = ManifestLoader.load_from_local_filesystem(file_config)

    assert output == example_content


def test_load_from_local_filesystem_utf8(tmp_path):
    """Confirm local manifests with UTF-8 characters load on Windows."""
    path = tmp_path / "manifest.json"
    expected = {"description": "café"}
    path.write_text(json.dumps(expected), encoding="utf-8")
    file_config = FileReferenceConfig(path=str(path))  # type: ignore

    assert ManifestLoader.load_from_local_filesystem(file_config) == expected


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
