"""Common types and data structures for the Nekt SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class CloudProvider(StrEnum):
    """Supported cloud providers."""

    AWS = "AWS"
    GCP = "GCP"


class Environment(StrEnum):
    """Supported execution environments."""

    LOCAL = "LOCAL"
    PRODUCTION = "PRODUCTION"


class TokenType(StrEnum):
    """Authentication token types for different contexts."""

    JUPYTER = "jupyter"  # Jupyter notebooks (X-Jupyter-Token)
    REPORT = "report"  # Report kernels (X-Report-Token)
    PIPELINE = "pipeline"  # Production pipelines (X-Pipeline-Run-Token)


class TableFormat(StrEnum):
    """Supported table formats."""

    DELTA = "DELTA"
    ICEBERG = "ICEBERG"


class SaveMode(StrEnum):
    """Supported save modes for tables."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


class SchemaEvolutionStrategy(StrEnum):
    """Strategies for handling schema evolution."""

    MERGE = "merge"
    STRICT = "strict"
    OVERWRITE = "overwrite"


@dataclass(frozen=True, slots=True)
class Expectation:
    """A single data quality expectation."""

    gx_func: str
    config: dict[str, Any] = field(default_factory=dict)
    type_key: str | None = None
    break_pipeline_on_failure: bool = False
    gx_meta: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ColumnExpectations:
    """Expectations for a specific column."""

    column: str
    expectations: list[Expectation] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class IcebergConfig:
    """Apache Iceberg specific configuration."""

    catalog_name: str
    catalog_alias: str
    namespace: str
    table_bucket_arn: str


@dataclass(frozen=True, slots=True)
class DeltaConfig:
    """Delta Lake specific configuration."""

    partitions: list[str] = field(default_factory=list)
    optimize_on_write: bool = False
    z_order_columns: list[str] = field(default_factory=list)
    log_retention_duration: str | None = None


@dataclass(frozen=True, slots=True)
class TableConfig:
    """Configuration for a table, as returned from the Nekt API."""

    layer_name: str
    table_name: str
    provider: CloudProvider

    # Storage paths
    path: str  # S3 path for AWS, or BigQuery table reference for GCP
    database_name: str | None = None  # Glue database for AWS, or BQ dataset for GCP
    database_id: str | None = None

    # AWS Glue region (for catalog operations)
    glue_region_name: str | None = None

    # Table format (AWS only)
    table_format: TableFormat = TableFormat.DELTA

    # Delta-specific config (AWS only)
    delta_config: DeltaConfig | None = None

    # Iceberg-specific config (AWS only)
    iceberg_config: IcebergConfig | None = None

    # Data quality expectations
    expectations: list[ColumnExpectations] = field(default_factory=list)

    @classmethod
    def from_api_response(
        cls,
        layer_name: str,
        table_name: str,
        provider: CloudProvider,
        api_data: dict[str, Any],
    ) -> TableConfig:
        """Create a TableConfig from API response data."""
        # Parse expectations
        expectations = []
        raw_expectations = api_data.get("expectations", [])
        if raw_expectations:
            for col_exp in raw_expectations:
                column_expectations = ColumnExpectations(
                    column=col_exp.get("column", ""),
                    expectations=[
                        Expectation(
                            gx_func=exp.get("gx_func", ""),
                            config=exp.get("config", {}),
                            type_key=exp.get("type_key"),
                            break_pipeline_on_failure=exp.get("break_pipeline_on_failure", False),
                            gx_meta=exp.get("gx_meta"),
                        )
                        for exp in col_exp.get("expectations", [])
                    ],
                )
                expectations.append(column_expectations)

        # Parse table format (AWS only, defaults to DELTA)
        table_format = TableFormat(api_data.get("table_format", "DELTA"))

        # Parse delta config (AWS only)
        delta_config = None
        if provider == CloudProvider.AWS:
            delta_config = DeltaConfig(
                partitions=api_data.get("delta_partitions", []) or [],
                optimize_on_write=api_data.get("delta_optimize_on_write", False) or False,
                z_order_columns=api_data.get("delta_z_order_columns", []) or [],
                log_retention_duration=api_data.get("delta_log_retention_duration"),
            )

        # Parse iceberg config (AWS only, when table_format is ICEBERG)
        iceberg_config = None
        if provider == CloudProvider.AWS and table_format == TableFormat.ICEBERG:
            iceberg_config = IcebergConfig(
                catalog_name=api_data.get("iceberg_catalog_name", ""),
                catalog_alias=api_data.get("iceberg_catalog_alias", ""),
                namespace=api_data.get("iceberg_namespace", ""),
                table_bucket_arn=api_data.get("iceberg_table_bucket_arn", ""),
            )

        # Determine path based on provider
        if provider == CloudProvider.AWS:
            path = api_data.get("s3_path", "")
        else:
            # For GCP, construct BigQuery table reference
            layer_db = api_data.get("layer_database_name", "")
            path = f"{layer_db}.{table_name}" if layer_db else table_name

        return cls(
            layer_name=layer_name,
            table_name=table_name,
            provider=provider,
            path=path,
            database_name=api_data.get("layer_database_name"),
            database_id=api_data.get("database_id"),
            glue_region_name=api_data.get("glue_region_name"),
            table_format=table_format,
            delta_config=delta_config,
            iceberg_config=iceberg_config,
            expectations=expectations,
        )


@dataclass(frozen=True, slots=True)
class VolumeFile:
    """A file in a volume."""

    path: str
    name: str | None = None
    size: int | None = None


@dataclass(frozen=True, slots=True)
class CloudCredentials:
    """Cloud credentials for accessing storage."""

    provider: CloudProvider

    # AWS credentials
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    aws_region: str | None = None

    # GCP credentials
    gcp_access_token: str | None = None
    gcp_project_id: str | None = None

    @classmethod
    def from_aws(
        cls,
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
        region: str | None = None,
    ) -> CloudCredentials:
        """Create AWS credentials."""
        return cls(
            provider=CloudProvider.AWS,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
            aws_region=region,
        )

    @classmethod
    def from_gcp(cls, access_token: str, project_id: str) -> CloudCredentials:
        """Create GCP credentials."""
        return cls(
            provider=CloudProvider.GCP,
            gcp_access_token=access_token,
            gcp_project_id=project_id,
        )

    def to_storage_options(self) -> dict[str, str]:
        """Convert credentials to storage options for delta-rs or other libraries."""
        if self.provider == CloudProvider.AWS:
            options = {}
            if self.aws_access_key_id:
                options["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
            if self.aws_secret_access_key:
                options["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
            if self.aws_session_token:
                options["AWS_SESSION_TOKEN"] = self.aws_session_token
            if self.aws_region:
                options["AWS_REGION"] = self.aws_region
            return options
        elif self.provider == CloudProvider.GCP:
            options = {}
            if self.gcp_access_token:
                options["GOOGLE_SERVICE_ACCOUNT_TOKEN"] = self.gcp_access_token
            return options
        return {}


@dataclass(frozen=True, slots=True)
class ExpectationResult:
    """Result of a single expectation validation."""

    expectation_type: str
    success: bool
    column: str | None = None
    message: str | None = None
    break_pipeline_on_failure: bool = False


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Result of validating data against expectations."""

    success: bool
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def failed_expectations(self) -> list[ExpectationResult]:
        """Get list of failed expectations."""
        return [r for r in self.results if not r.success]

    @property
    def critical_failures(self) -> list[ExpectationResult]:
        """Get list of failed expectations that should break the pipeline."""
        return [r for r in self.results if not r.success and r.break_pipeline_on_failure]
