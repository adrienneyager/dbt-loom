import gzip
import json
import tempfile
from pathlib import Path, PurePosixPath
from typing import Dict

from dbt.config.runtime import load_profile
from dbt.flags import get_flags
from dbt_loom.clients import is_gzipped
from dbt_loom.logging import fire_event
from pydantic import BaseModel


def _load_downloaded_manifest(download_path: Path) -> Dict:
    """Read a plain or gzip-compressed manifest downloaded from a stage."""
    content = download_path.read_bytes()

    if is_gzipped(content):
        content = gzip.decompress(content)

    return json.loads(content)


class SnowflakeReferenceConfig(BaseModel):
    """Configuration for an reference stored in Snowflake Stage"""

    stage: str
    stage_path: str


class SnowflakeClient:
    """A client for loading manifest files from Snowflake Stage."""

    def __init__(self, stage: str, stage_path: str) -> None:
        self.stage = stage
        self.stage_path = stage_path.lstrip("/")

    def load_manifest(self) -> Dict:
        """Load the manifest.json file from Snowflake stage."""

        try:
            from dbt.adapters.snowflake import SnowflakeAdapter
        except ImportError as exception:
            fire_event(
                msg="dbt-core: Fatal error. Expected to find dbt-snowflake "
                "installed to support loading the manifest from a Snowflake "
                "stage.",
            )
            raise exception

        try:
            from dbt.mp_context import get_mp_context
        except ImportError as exception:
            fire_event(
                msg="dbt-core: Fatal error. Unable to initialize a Snowflake "
                "adapter. Loading from Snowflake stages requires dbt-core "
                "1.8.0 and newer."
            )
            raise exception

        flags = get_flags()
        profile = load_profile(
            project_root=flags.PROJECT_DIR,
            cli_vars=flags.VARS,
            profile_name_override=flags.PROFILE,
            target_override=flags.TARGET,
        )
        adapter = SnowflakeAdapter(profile, get_mp_context())
        file_name = str(PurePosixPath(self.stage_path).name)
        tmp_dir = tempfile.mkdtemp(prefix="dbt_loom_")

        # Snowflake needs '/' path separators
        tmp_dir_sf = tmp_dir.replace("\\", "/")

        with adapter.connection_named("dbt-loom"):
            get_query = f"get @{self.stage}/{self.stage_path} file://{tmp_dir_sf}/"
            response, table = adapter.connections.execute(get_query)
            if response.rows_affected == 0:
                raise Exception(
                    f"Failed to get file {self.stage}/{self.stage_path}: {response}"
                )

        download_path = Path(tmp_dir) / file_name

        return _load_downloaded_manifest(download_path)
