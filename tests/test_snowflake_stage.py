import gzip
import json

from dbt_loom.clients.snowflake_stage import _load_downloaded_manifest


def test_load_downloaded_manifest_reads_plain_json(tmp_path):
    manifest = {"metadata": {"dbt_version": "1.12.2"}}
    download_path = tmp_path / "manifest.json"
    download_path.write_text(json.dumps(manifest))

    assert _load_downloaded_manifest(download_path) == manifest


def test_load_downloaded_manifest_reads_gzip_json(tmp_path):
    manifest = {"metadata": {"dbt_version": "1.12.2"}}
    download_path = tmp_path / "manifest.json.gz"
    download_path.write_bytes(gzip.compress(json.dumps(manifest).encode("utf-8")))

    assert _load_downloaded_manifest(download_path) == manifest