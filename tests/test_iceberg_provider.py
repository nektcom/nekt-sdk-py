"""Tests for the IcebergProvider (public SDK read-only provider)."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from nekt.exceptions import EngineError, IcebergProviderError
from nekt.provider.iceberg import IcebergProvider
from nekt.types import CloudCredentials, CloudProvider, IcebergConfig


class TestParseRegion:
    """Verify _parse_region extracts AWS region from ARN."""

    def test_standard_arn(self):
        arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
        assert IcebergProvider._parse_region(arn) == "us-east-1"

    def test_eu_region(self):
        arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
        assert IcebergProvider._parse_region(arn) == "eu-west-1"

    def test_invalid_arn_raises(self):
        with pytest.raises(IcebergProviderError, match="Invalid S3 Table Bucket ARN"):
            IcebergProvider._parse_region("not-an-arn")


class TestCreateCatalog:
    """Verify _create_catalog builds the right PyIceberg config."""

    def test_creates_rest_catalog_with_sigv4(self):
        import sys
        from types import ModuleType

        config = IcebergConfig(
            catalog_name="s3tablescatalog/my-bucket",
            catalog_alias="s3tb_001",
            namespace="raw",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
        )
        credentials = CloudCredentials.from_aws(
            access_key_id="AKID",
            secret_access_key="SECRET",
            session_token="TOKEN",
            region="us-east-1",
        )

        provider = IcebergProvider(credentials=credentials, iceberg_config=config)

        # Mock pyiceberg.catalog module to capture load_catalog calls
        mock_load = MagicMock(return_value=MagicMock())
        mock_catalog_mod = ModuleType("pyiceberg.catalog")
        mock_catalog_mod.load_catalog = mock_load
        mock_pyiceberg = ModuleType("pyiceberg")

        with patch.dict(sys.modules, {"pyiceberg": mock_pyiceberg, "pyiceberg.catalog": mock_catalog_mod}):
            provider._create_catalog()

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args
        assert call_kwargs[0][0] == "s3tables"
        kwargs = call_kwargs[1]
        assert kwargs["type"] == "rest"
        assert kwargs["uri"] == "https://s3tables.us-east-1.amazonaws.com/iceberg"
        assert kwargs["warehouse"] == config.table_bucket_arn
        assert kwargs["rest.sigv4-enabled"] == "true"
        assert kwargs["rest.signing-name"] == "s3tables"
        assert kwargs["s3.access-key-id"] == "AKID"
        assert kwargs["s3.secret-access-key"] == "SECRET"
        assert kwargs["s3.session-token"] == "TOKEN"

    def test_raises_when_config_missing(self):
        import sys
        from types import ModuleType

        provider = IcebergProvider(credentials=None, iceberg_config=None)

        mock_catalog_mod = ModuleType("pyiceberg.catalog")
        mock_catalog_mod.load_catalog = MagicMock()
        mock_pyiceberg = ModuleType("pyiceberg")

        with patch.dict(sys.modules, {"pyiceberg": mock_pyiceberg, "pyiceberg.catalog": mock_catalog_mod}):
            with pytest.raises(IcebergProviderError, match="IcebergConfig is required"):
                provider._create_catalog()


class TestLoad:
    """Verify load() returns PyArrow table from PyIceberg."""

    def test_load_returns_arrow_table(self):
        config = IcebergConfig(
            catalog_name="cat",
            catalog_alias="s3tb_001",
            namespace="my_ns",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
        )
        provider = IcebergProvider(credentials=None, iceberg_config=config)

        expected = pa.table({"id": [1, 2, 3]})
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value.to_arrow.return_value = expected

        with patch.object(provider, "_create_catalog") as mock_catalog:
            mock_catalog.return_value.load_table.return_value = mock_iceberg_table
            result = provider.load("s3://unused/path", table_name="events")

        mock_catalog.return_value.load_table.assert_called_once_with("my_ns.events")
        assert result.equals(expected)

    def test_load_requires_table_name(self):
        config = IcebergConfig(
            catalog_name="cat",
            catalog_alias="s3tb_001",
            namespace="ns",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
        )
        provider = IcebergProvider(credentials=None, iceberg_config=config)

        with pytest.raises(IcebergProviderError, match="table_name is required"):
            provider.load("s3://path")

    def test_load_wraps_exceptions(self):
        config = IcebergConfig(
            catalog_name="cat",
            catalog_alias="s3tb_001",
            namespace="ns",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
        )
        provider = IcebergProvider(credentials=None, iceberg_config=config)

        with patch.object(provider, "_create_catalog") as mock_catalog:
            mock_catalog.return_value.load_table.side_effect = Exception("Connection refused")
            with pytest.raises(IcebergProviderError, match="Failed to load Iceberg table"):
                provider.load("s3://path", table_name="events")
