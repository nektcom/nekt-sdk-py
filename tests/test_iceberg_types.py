"""Tests for Iceberg-related types: TableFormat, IcebergConfig, and TableConfig parsing."""

from nekt.types import (
    CloudProvider,
    IcebergConfig,
    TableConfig,
    TableFormat,
)


class TestTableFormat:
    """Verify TableFormat enum values."""

    def test_delta_value(self):
        assert TableFormat.DELTA == "DELTA"

    def test_iceberg_value(self):
        assert TableFormat.ICEBERG == "ICEBERG"

    def test_delta_is_default_str_enum(self):
        assert TableFormat("DELTA") is TableFormat.DELTA

    def test_iceberg_from_string(self):
        assert TableFormat("ICEBERG") is TableFormat.ICEBERG


class TestIcebergConfig:
    """Verify IcebergConfig dataclass creation."""

    def test_creation(self):
        config = IcebergConfig(
            catalog_name="my_catalog",
            namespace="my_namespace",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
        )
        assert config.catalog_name == "my_catalog"
        assert config.namespace == "my_namespace"
        assert config.table_bucket_arn == "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"

    def test_is_frozen(self):
        config = IcebergConfig(
            catalog_name="cat",
            namespace="ns",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
        )
        import pytest

        with pytest.raises(AttributeError):
            config.catalog_name = "other"  # type: ignore[misc]


class TestTableConfigFromApiResponse:
    """Verify TableConfig.from_api_response handles Iceberg fields."""

    def test_defaults_to_delta_when_no_table_format(self):
        api_data = {
            "s3_path": "s3://bucket/path",
            "layer_database_name": "my_db",
        }
        config = TableConfig.from_api_response("layer", "table", CloudProvider.AWS, api_data)
        assert config.table_format == TableFormat.DELTA
        assert config.iceberg_config is None

    def test_explicit_delta_format(self):
        api_data = {
            "table_format": "DELTA",
            "s3_path": "s3://bucket/path",
            "layer_database_name": "my_db",
        }
        config = TableConfig.from_api_response("layer", "table", CloudProvider.AWS, api_data)
        assert config.table_format == TableFormat.DELTA
        assert config.iceberg_config is None

    def test_iceberg_format_parses_iceberg_config(self):
        api_data = {
            "table_format": "ICEBERG",
            "s3_path": "s3://bucket/path",
            "layer_database_name": "my_db",
            "iceberg_catalog_name": "my_catalog",
            "iceberg_namespace": "my_namespace",
            "iceberg_table_bucket_arn": "arn:aws:s3tables:us-east-1:123:bucket/b",
        }
        config = TableConfig.from_api_response("layer", "table", CloudProvider.AWS, api_data)
        assert config.table_format == TableFormat.ICEBERG
        assert config.iceberg_config is not None
        assert config.iceberg_config.catalog_name == "my_catalog"
        assert config.iceberg_config.namespace == "my_namespace"
        assert config.iceberg_config.table_bucket_arn == "arn:aws:s3tables:us-east-1:123:bucket/b"

    def test_iceberg_format_still_has_delta_config(self):
        """Iceberg tables still get delta_config populated (AWS always does)."""
        api_data = {
            "table_format": "ICEBERG",
            "s3_path": "s3://bucket/path",
            "iceberg_catalog_name": "cat",
            "iceberg_namespace": "ns",
            "iceberg_table_bucket_arn": "arn:aws:s3tables:us-east-1:123:bucket/b",
        }
        config = TableConfig.from_api_response("layer", "table", CloudProvider.AWS, api_data)
        assert config.delta_config is not None

    def test_gcp_ignores_iceberg_config(self):
        """GCP tables should not get iceberg_config even if table_format is present."""
        api_data = {
            "table_format": "ICEBERG",
            "layer_database_name": "my_dataset",
        }
        config = TableConfig.from_api_response("layer", "table", CloudProvider.GCP, api_data)
        assert config.iceberg_config is None
