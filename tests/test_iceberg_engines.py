"""End-to-end tests for Iceberg table support in both Spark and Python engines.

Uses mocked SparkSession and IcebergProvider — no Java or AWS runtime required.
"""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from nekt.exceptions import EngineError
from nekt.types import CloudProvider, DeltaConfig, Environment, IcebergConfig, TableConfig, TableFormat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_iceberg_table_config(**overrides):
    defaults = dict(
        layer_name="bronze",
        table_name="events",
        provider=CloudProvider.AWS,
        path="s3://bucket/iceberg/path",
        table_format=TableFormat.ICEBERG,
        iceberg_config=IcebergConfig(
            catalog_name="s3tablescatalog/my-bucket",
            catalog_alias="s3tb_001",
            namespace="raw",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
        ),
    )
    defaults.update(overrides)
    return TableConfig(**defaults)


def _make_delta_table_config(**overrides):
    defaults = dict(
        layer_name="bronze",
        table_name="orders",
        provider=CloudProvider.AWS,
        path="s3://bucket/delta/path",
        table_format=TableFormat.DELTA,
        delta_config=DeltaConfig(partitions=[], optimize_on_write=False, z_order_columns=[]),
    )
    defaults.update(overrides)
    return TableConfig(**defaults)


# ===================================================================
# Spark Engine
# ===================================================================

class TestSparkEngineLoadTable:
    """Test the full load_table() dispatch for the Spark engine."""

    def _make_spark_engine(self):
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
        return engine

    def test_load_table_routes_iceberg_to_spark_table(self):
        """load_table() for Iceberg should call spark.table() with the FQN."""
        engine = self._make_spark_engine()
        config = _make_iceberg_table_config()
        engine._api.get_table_details.return_value = config

        result = engine.load_table("bronze", "events")

        engine._spark.table.assert_called_once_with("s3tb_001.raw.events")
        assert result == engine._spark.table.return_value

    def test_load_table_routes_delta_to_load_delta_table(self):
        """load_table() for Delta should call load_delta_table() path."""
        engine = self._make_spark_engine()
        config = _make_delta_table_config()
        engine._api.get_table_details.return_value = config

        with patch.object(engine, "load_delta_table") as mock_load_delta:
            mock_delta_table = MagicMock()
            mock_load_delta.return_value = mock_delta_table
            result = engine.load_table("bronze", "orders")

        mock_load_delta.assert_called_once_with("bronze", "orders")
        mock_delta_table.toDF.assert_called_once()

    def test_load_table_raises_when_iceberg_config_missing(self):
        """load_table() should raise when Iceberg table has no iceberg_config."""
        engine = self._make_spark_engine()
        config = _make_iceberg_table_config(iceberg_config=None)
        engine._api.get_table_details.return_value = config

        with pytest.raises(EngineError, match="Iceberg config missing"):
            engine.load_table("bronze", "events")

    def test_load_table_raises_when_catalog_alias_empty(self):
        """load_table() should raise when catalog_alias is empty."""
        engine = self._make_spark_engine()
        config = _make_iceberg_table_config(
            iceberg_config=IcebergConfig(
                catalog_name="cat",
                catalog_alias="",
                namespace="ns",
                table_bucket_arn="arn:aws:s3tables:us-east-1:123:bucket/b",
            ),
        )
        engine._api.get_table_details.return_value = config

        with pytest.raises(EngineError, match="catalog_alias missing"):
            engine.load_table("bronze", "events")


class TestSparkEngineLoadIcebergTable:
    """Test _load_iceberg_table() directly."""

    def _make_spark_engine(self):
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
        return engine

    def test_builds_correct_fqn(self):
        engine = self._make_spark_engine()
        config = _make_iceberg_table_config(
            table_name="my_table",
            iceberg_config=IcebergConfig(
                catalog_name="cat",
                catalog_alias="s3tb_003",
                namespace="silver",
                table_bucket_arn="arn:aws:s3tables:eu-west-1:999:bucket/b",
            ),
        )

        engine._load_iceberg_table(config)

        engine._spark.table.assert_called_once_with("s3tb_003.silver.my_table")


# ===================================================================
# Python Engine
# ===================================================================

class TestPythonEngineLoadTable:
    """Test the full load_table() dispatch for the Python engine."""

    def _make_python_engine(self):
        from nekt.engine.python import PythonEngine

        api = MagicMock()
        engine = PythonEngine(
            api=api,
            provider=CloudProvider.AWS,
            credentials=None,
            environment=Environment.LOCAL,
        )
        return engine

    def test_load_table_routes_iceberg_to_provider(self):
        """load_table() for Iceberg should use IcebergProvider."""
        from nekt.provider.iceberg import IcebergProvider

        engine = self._make_python_engine()
        config = _make_iceberg_table_config()
        engine._api.get_table_details.return_value = config

        mock_arrow = pa.table({"id": [1, 2, 3]})

        with patch.object(IcebergProvider, "load", return_value=mock_arrow):
            df = engine.load_table("bronze", "events")

        assert len(df) == 3
        assert "id" in df.columns

    def test_load_table_routes_delta_to_data_provider(self):
        """load_table() for Delta should use the DeltaProvider path."""
        engine = self._make_python_engine()
        config = _make_delta_table_config()
        engine._api.get_table_details.return_value = config

        mock_arrow = pa.table({"order_id": [10, 20]})
        mock_provider = MagicMock()
        mock_provider.load.return_value = mock_arrow
        engine._data_provider = mock_provider

        df = engine.load_table("bronze", "orders")

        mock_provider.load.assert_called_once_with("s3://bucket/delta/path")
        assert len(df) == 2
        assert "order_id" in df.columns

    def test_load_table_raises_when_iceberg_config_missing(self):
        """load_table() should raise when Iceberg table has no iceberg_config."""
        engine = self._make_python_engine()
        config = _make_iceberg_table_config(iceberg_config=None)
        engine._api.get_table_details.return_value = config

        with pytest.raises(EngineError, match="Iceberg config missing"):
            engine.load_table("bronze", "events")

    def test_load_table_passes_correct_args_to_provider(self):
        """IcebergProvider.load() receives the table path and table_name."""
        from nekt.provider.iceberg import IcebergProvider

        engine = self._make_python_engine()
        config = _make_iceberg_table_config(path="s3://my-bucket/iceberg/events")
        engine._api.get_table_details.return_value = config

        mock_arrow = pa.table({"x": [1]})

        with patch.object(IcebergProvider, "load", return_value=mock_arrow) as mock_load:
            engine.load_table("bronze", "events")

        mock_load.assert_called_once_with("s3://my-bucket/iceberg/events", table_name="events")
