from enum import Enum
from pathlib import Path
import re
from typing import List, Union
from urllib.parse import ParseResult, urlparse

from pydantic import BaseModel, Field, field_validator, model_validator, validator

from dbt_loom.clients.az_blob import AzureReferenceConfig
from dbt_loom.clients.dbt_cloud import DbtCloudReferenceConfig
from dbt_loom.clients.gcs import GCSReferenceConfig
from dbt_loom.clients.paradime import ParadimeReferenceConfig
from dbt_loom.clients.s3 import S3ReferenceConfig
from dbt_loom.clients.snowflake_stage import SnowflakeReferenceConfig
from dbt_loom.clients.dbx import DatabricksReferenceConfig


class ManifestReferenceType(str, Enum):
    """Type of ManifestReference"""

    file = "file"
    dbt_cloud = "dbt_cloud"
    paradime = "paradime"
    gcs = "gcs"
    s3 = "s3"
    azure = "azure"
    snowflake = "snowflake"
    databricks = "databricks"


class FileReferenceConfig(BaseModel):
    """Configuration for a file reference"""

    path: ParseResult

    @validator("path", pre=True, always=True)
    def default_path(cls, v, values) -> ParseResult:
        """
        Check if the provided path is a valid URL. If not, convert it into an
        absolute file path.
        """

        if isinstance(v, ParseResult):
            return v

        if bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", v)):
            return urlparse(v)

        return urlparse(Path(v).absolute().as_uri())


_TYPE_TO_CONFIG = {
    ManifestReferenceType.file: FileReferenceConfig,
    ManifestReferenceType.dbt_cloud: DbtCloudReferenceConfig,
    ManifestReferenceType.paradime: ParadimeReferenceConfig,
    ManifestReferenceType.gcs: GCSReferenceConfig,
    ManifestReferenceType.s3: S3ReferenceConfig,
    ManifestReferenceType.azure: AzureReferenceConfig,
    ManifestReferenceType.snowflake: SnowflakeReferenceConfig,
    ManifestReferenceType.databricks: DatabricksReferenceConfig,
}


class ManifestReference(BaseModel):
    """Reference information for a manifest to be loaded into dbt-loom."""

    name: str
    type: ManifestReferenceType
    config: Union[
        FileReferenceConfig,
        DbtCloudReferenceConfig,
        ParadimeReferenceConfig,
        GCSReferenceConfig,
        S3ReferenceConfig,
        AzureReferenceConfig,
        SnowflakeReferenceConfig,
        DatabricksReferenceConfig,
    ]
    excluded_packages: List[str] = Field(default_factory=list)
    included_packages: List[str] = Field(default_factory=list)
    node_transformers: List[str] = Field(default_factory=list)
    optional: bool = False

    @field_validator("config", mode="before")
    @classmethod
    def resolve_config_type(cls, v, info):
        """Resolve the config to the correct type based on the manifest
        reference type.

        Pydantic v1 Union resolution tries each type in order and returns the
        first match. This can cause configs meant for other types (e.g.,
        DatabricksReferenceConfig) to be incorrectly parsed as
        FileReferenceConfig when both have a `path` field. This validator
        explicitly constructs the correct config class based on `type`.
        """
        ref_type = info.data.get("type")
        if ref_type is not None and isinstance(v, dict):
            config_cls = _TYPE_TO_CONFIG.get(ref_type)
            if config_cls is not None:
                return config_cls(**v)
        return v

    @model_validator(mode="after")
    def mutually_exclusive_package_lists(self):
        """Verify that either excluded_packages OR included_packages is provided."""

        if len(self.included_packages) == 0 or len(self.excluded_packages) == 0:
            return self

        raise ValueError(
            "Cannot specify both 'included_packages' and 'excluded_packages'. "
            "Use one or the other."
        )


class dbtLoomConfig(BaseModel):
    """Configuration for dbt Loom"""

    manifests: List[ManifestReference]
    enable_telemetry: bool = False


class LoomConfigurationError(BaseException):
    """Error raised when dbt-loom has been misconfigured."""
