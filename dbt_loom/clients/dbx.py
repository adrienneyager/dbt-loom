import json
import gzip
from io import BytesIO
from typing import Dict, Optional
from dbt_loom.clients import is_gzipped
from dbt_loom.logging import fire_event
from pydantic import BaseModel
from urllib.parse import ParseResult, unquote


class DatabricksReferenceConfig(BaseModel):
    """Configuration for a reference stored in Databricks"""

    path: str


class DatabricksClient:
    """A client for loading manifest files from Databricks."""

    def __init__(self, path: str) -> None:
        self.path = path

    def _get_path_str(self):
        """
        Converts the path attribute to a string representation.
        Handles different types of path inputs (string or ParseResult)
        and decodes URL-encoded paths if necessary.
        """
        if isinstance(self.path, str):
            # If the path is already a string, return it directly.
            return self.path
        elif isinstance(self.path, ParseResult):
            # If the path is a ParseResult object, extract the path component
            # and unquote it to handle URL-encoded characters.
            return unquote(self.path.path)
        else:
            # If the path type is not supported, raise a TypeError.
            raise TypeError(f"Unsupported path type: {type(self.path)}")

    def get_cache_token(self) -> Optional[str]:
        """
        Build a cache-validation token from the file's Databricks metadata
        (modification time and size), or None if it cannot be retrieved.
        """

        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            path_str = self._get_path_str()

            if path_str.startswith("/Workspace/"):
                object_info = w.workspace.get_status(path_str)
                return f"{object_info.modified_at}-{object_info.size}"

            if path_str.startswith("/dbfs/"):
                file_info = w.dbfs.get_status(path_str[5:])
                return f"{file_info.modification_time}-{file_info.file_size}"

            return w.files.get_metadata(path_str).last_modified
        except Exception:
            return None

    def load_manifest(self) -> Dict:
        """Load the manifest.json file from Databricks."""

        # Import the Databricks SDK, which is a dependency of the dbt-databricks adapter
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.workspace import ExportFormat
            import base64
        except ImportError:
            fire_event(msg="dbt-loom expected the Databricks SDK to be installed.")
            raise

        try:
            # Initialize the workspace client; auth is handled via Databricks Unified Authentication model
            w = WorkspaceClient()
            path_str = self._get_path_str()
            downloaded_bytes = None

            # If it's a Databricks Workspace path (e.g., /Workspace/Users/...), use workspace.export.
            # This API returns content that might be base64 encoded.
            if path_str.startswith("/Workspace/"):
                resp = w.workspace.export(path_str, format=ExportFormat.AUTO)
                export_content = resp.content
                # Attempt base64 decode. If it fails, assume it's not base64 encoded and use raw content.
                try:
                    downloaded_bytes = base64.b64decode(export_content)
                except Exception:
                    downloaded_bytes = (
                        export_content
                        if isinstance(export_content, bytes)
                        else export_content.encode("utf-8")
                    )
            # If it's a DBFS path, use w.dbfs.download
            elif path_str.startswith("/dbfs/"):
                # Remove the /dbfs prefix for w.dbfs.download as it expects paths relative to DBFS root
                path_str = path_str[5:]
                resp = w.dbfs.download(path_str)
                downloaded_bytes = resp.read()
            # For other paths (e.g., Unity Catalog volumes or external locations), use files.download.
            else:
                resp = w.files.download(path_str)
                downloaded_bytes = resp.contents.read()
        except Exception:
            fire_event(msg="Unable to retrieve file from Databricks.")
            raise

        # Deserialize the object: handle gzip decompression and then load JSON.
        try:
            content_string = None
            if downloaded_bytes is None:
                raise ValueError("No content downloaded.")

            if is_gzipped(downloaded_bytes):
                with gzip.GzipFile(fileobj=BytesIO(downloaded_bytes)) as gzipfile:
                    content_string = gzipfile.read().decode("utf-8")
            else:
                content_string = downloaded_bytes.decode("utf-8")

            return json.loads(content_string)
        except json.decoder.JSONDecodeError:
            fire_event(msg=f"The object `{path_str}` does not contain valid JSON.")
            raise
        except Exception:
            fire_event(
                msg=f"Unable to read the data contained in the object `{path_str}`"
            )
            raise
