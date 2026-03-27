"""Tests for Iceberg table loading: catalog alias resolution and engine routing."""

from unittest.mock import MagicMock, patch

import pytest

from nekt.types import CloudProvider, Environment, IcebergConfig, TableConfig, TableFormat


class TestIcebergCatalogAliasResolution:
    """Verify _resolve_iceberg_catalog_alias finds the correct catalog."""

    def _make_spark_engine(self, spark_mock):
        """Create a SparkEngine with a mocked SparkSession."""
        from nekt.engine.spark import SparkEngine

        api = MagicMock()
        engine = SparkEngine(
            api=api,
            provider=CloudProvider.AWS,
            credentials=None,
            environment=Environment.LOCAL,
        )
        engine._spark = spark_mock
        return engine

    def test_finds_matching_catalog(self):
        target_arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"

        spark_mock = MagicMock()

        def conf_get(key):
            if key == "spark.sql.catalog.s3tb_001.warehouse":
                return "arn:aws:s3tables:us-east-1:999999999999:bucket/other-bucket"
            if key == "spark.sql.catalog.s3tb_002.warehouse":
                return target_arn
            raise Exception("not found")

        spark_mock.conf.get = conf_get

        engine = self._make_spark_engine(spark_mock)
        alias = engine._resolve_iceberg_catalog_alias(target_arn)
        assert alias == "s3tb_002"

    def test_finds_first_catalog(self):
        target_arn = "arn:aws:s3tables:us-east-1:123:bucket/first"

        spark_mock = MagicMock()

        def conf_get(key):
            if key == "spark.sql.catalog.s3tb_001.warehouse":
                return target_arn
            raise Exception("not found")

        spark_mock.conf.get = conf_get

        engine = self._make_spark_engine(spark_mock)
        alias = engine._resolve_iceberg_catalog_alias(target_arn)
        assert alias == "s3tb_001"

    def test_raises_when_no_matching_catalog(self):
        spark_mock = MagicMock()
        spark_mock.conf.get.side_effect = Exception("not found")

        engine = self._make_spark_engine(spark_mock)
        with pytest.raises(ValueError, match="No Spark catalog found"):
            engine._resolve_iceberg_catalog_alias("arn:aws:s3tables:us-east-1:123:bucket/missing")

    def test_raises_when_no_arn_matches(self):
        spark_mock = MagicMock()

        def conf_get(key):
            if key == "spark.sql.catalog.s3tb_001.warehouse":
                return "arn:aws:s3tables:us-east-1:111:bucket/a"
            if key == "spark.sql.catalog.s3tb_002.warehouse":
                return "arn:aws:s3tables:us-east-1:222:bucket/b"
            raise Exception("not found")

        spark_mock.conf.get = conf_get

        engine = self._make_spark_engine(spark_mock)
        with pytest.raises(ValueError, match="No Spark catalog found"):
            engine._resolve_iceberg_catalog_alias("arn:aws:s3tables:us-east-1:999:bucket/nomatch")


class TestSparkEngineIcebergLoading:
    """Verify SparkEngine routes Iceberg tables correctly."""

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
                catalog_name="my_catalog",
                namespace="my_ns",
                table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
            ),
        )

        # Mock catalog resolution
        with patch.object(engine, "_resolve_iceberg_catalog_alias", return_value="s3tb_001"):
            result = engine._load_iceberg_table(table_config)

        spark_mock.table.assert_called_once_with("s3tb_001.my_ns.events")
        assert result == spark_mock.table.return_value


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
