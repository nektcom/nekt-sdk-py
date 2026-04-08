"""Tests for Iceberg table loading via Spark and Python engines."""

from unittest.mock import MagicMock, patch

import pytest

from nekt.types import CloudProvider, Environment, IcebergConfig, TableConfig, TableFormat


class TestSparkEngineIcebergLoading:
    """Verify SparkEngine routes Iceberg tables correctly using catalog_alias from API."""

    def test_load_iceberg_table_calls_spark_table(self):
        from nekt.engine.spark import SparkEngine

        spark_mock = MagicMock()
        api = MagicMock()

        engine = SparkEngine(
            api=api,
            provider=CloudProvider.AWS,
            credentials=None,
            environment=Environment.LOCAL,
        )
        engine._spark = spark_mock

        table_config = TableConfig(
            layer_name="bronze",
            table_name="events",
            provider=CloudProvider.AWS,
            path="s3://bucket/path",
            table_format=TableFormat.ICEBERG,
            iceberg_config=IcebergConfig(
                catalog_name="s3tablescatalog/my-bucket",
                catalog_alias="s3tb_001",
                namespace="my_ns",
                table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
            ),
        )

        result = engine._load_iceberg_table(table_config)

        spark_mock.table.assert_called_once_with("s3tb_001.my_ns.events")
        assert result == spark_mock.table.return_value

    def test_load_iceberg_table_raises_when_alias_missing(self):
        from nekt.engine.spark import SparkEngine
        from nekt.exceptions import EngineError

        spark_mock = MagicMock()
        api = MagicMock()

        engine = SparkEngine(
            api=api,
            provider=CloudProvider.AWS,
            credentials=None,
            environment=Environment.LOCAL,
        )
        engine._spark = spark_mock

        table_config = TableConfig(
            layer_name="bronze",
            table_name="events",
            provider=CloudProvider.AWS,
            path="s3://bucket/path",
            table_format=TableFormat.ICEBERG,
            iceberg_config=IcebergConfig(
                catalog_name="cat",
                catalog_alias="",
                namespace="ns",
                table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
            ),
        )

        with pytest.raises(EngineError, match="catalog_alias missing"):
            engine._load_iceberg_table(table_config)


class TestPythonEngineIcebergLoading:
    """Verify PythonEngine loads Iceberg tables via IcebergProvider."""

    def test_load_table_uses_iceberg_provider(self):
        import pyarrow as pa

        from nekt.engine.python import PythonEngine
        from nekt.provider.iceberg import IcebergProvider

        api = MagicMock()

        iceberg_table = TableConfig(
            layer_name="bronze",
            table_name="events",
            provider=CloudProvider.AWS,
            path="s3://bucket/path",
            table_format=TableFormat.ICEBERG,
            iceberg_config=IcebergConfig(
                catalog_name="cat",
                catalog_alias="s3tb_001",
                namespace="ns",
                table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
            ),
        )
        api.get_table_details.return_value = iceberg_table

        engine = PythonEngine(
            api=api,
            provider=CloudProvider.AWS,
            credentials=None,
            environment=Environment.LOCAL,
        )

        mock_arrow = pa.table({"id": [1, 2], "name": ["a", "b"]})

        with patch.object(IcebergProvider, "load", return_value=mock_arrow) as mock_load:
            df = engine.load_table("bronze", "events")

        mock_load.assert_called_once_with(
            "s3://bucket/path",
            table_name="events",
        )
        assert len(df) == 2
        assert list(df.columns) == ["id", "name"]

    def test_load_table_raises_when_iceberg_config_missing(self):
        from nekt.engine.python import PythonEngine
        from nekt.exceptions import EngineError

        api = MagicMock()

        iceberg_table = TableConfig(
            layer_name="bronze",
            table_name="events",
            provider=CloudProvider.AWS,
            path="s3://bucket/path",
            table_format=TableFormat.ICEBERG,
            iceberg_config=None,
        )
        api.get_table_details.return_value = iceberg_table

        engine = PythonEngine(
            api=api,
            provider=CloudProvider.AWS,
            credentials=None,
            environment=Environment.LOCAL,
        )

        with pytest.raises(EngineError, match="Iceberg config missing"):
            engine.load_table("bronze", "events")
