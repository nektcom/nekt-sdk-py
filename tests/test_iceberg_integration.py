"""
Iceberg integration tests for the public SDK (read-only).

Uses two Iceberg tables in the S3 Table Bucket:
  - raw.titanic          — read-only source (seeded by seed script in internal SDK)
  - raw.titanic_output   — not used here (write tests are in the internal SDK)

Reads the Iceberg source table via both Python and Spark engines to verify
the full PyIceberg / Spark catalog flow works against real AWS infrastructure.

The API response is monkey-patched since the backend isn't deployed with
Iceberg support yet. Post-deploy: remove the monkey-patch.

Requires:
  NEKT_DATA_ACCESS_TOKEN — API token for nekt-dev

Run: pytest -m "integration and iceberg" tests/test_iceberg_integration.py -v
"""

import os
import sys
from unittest.mock import patch

import pytest

from nekt.types import IcebergConfig, TableConfig, TableFormat

NektModule = type(sys.modules["nekt"])

requires_credentials = pytest.mark.skipif(
    not os.environ.get("NEKT_DATA_ACCESS_TOKEN"),
    reason="NEKT_DATA_ACCESS_TOKEN not set",
)

requires_aws = pytest.mark.skipif(
    os.environ.get("NEKT_CLOUD_PROVIDER", "").upper() != "AWS",
    reason="Iceberg via S3Tables is AWS-only",
)

TEST_LAYER = os.environ.get("NEKT_TEST_LAYER", "Testing")
TEST_TABLE = os.environ.get("NEKT_TEST_TABLE", "titanic")

# Real S3 Table Bucket in nekt-dev (CloudFormation aws-2.0.27)
ICEBERG_TABLE_BUCKET_ARN = os.environ.get(
    "NEKT_ICEBERG_TABLE_BUCKET_ARN",
    "arn:aws:s3tables:us-east-1:637423198484:bucket/nekt-target-s3-iceberg-internal-tests",
)
ICEBERG_NAMESPACE = "raw"
ICEBERG_CATALOG_ALIAS = "s3tb_001"
ICEBERG_CATALOG_NAME = "s3tablescatalog/nekt-target-s3-iceberg-internal-tests"

# Read-only source table (seeded by scripts/seed_iceberg_titanic.py)
ICEBERG_SOURCE_TABLE = "titanic"


def _fresh_module():
    """Create a fresh NektModule instance with credentials from env."""
    m = NektModule("nekt_iceberg_integration_test")
    token = os.environ.get("NEKT_DATA_ACCESS_TOKEN")
    if token:
        m.data_access_token = token
    api_url = os.environ.get("NEKT_API_URL")
    if api_url:
        m.api_url = api_url
    return m


def _patch_to_iceberg(table_config: TableConfig) -> TableConfig:
    """Monkey-patch a Delta TableConfig to point at the Iceberg source table."""
    return TableConfig(
        layer_name=table_config.layer_name,
        table_name=ICEBERG_SOURCE_TABLE,
        provider=table_config.provider,
        path=table_config.path,
        database_name=table_config.database_name,
        database_id=table_config.database_id,
        table_format=TableFormat.ICEBERG,
        iceberg_config=IcebergConfig(
            catalog_name=ICEBERG_CATALOG_NAME,
            catalog_alias=ICEBERG_CATALOG_ALIAS,
            namespace=ICEBERG_NAMESPACE,
            table_bucket_arn=ICEBERG_TABLE_BUCKET_ARN,
        ),
    )


# ---------------------------------------------------------------------------
# Python Engine — reads Iceberg via PyIceberg
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.iceberg
@pytest.mark.python_engine
@requires_credentials
@requires_aws
def test_load_iceberg_table_python_engine():
    """Load the Iceberg source table via PyIceberg and verify it returns a pandas DataFrame."""
    import pandas as pd

    m = _fresh_module()
    m.engine = "python"

    engine = m._get_engine()
    original_get_table_config = engine._get_table_config

    def patched_get_table_config(layer_name, table_name):
        config = original_get_table_config(layer_name, table_name)
        return _patch_to_iceberg(config)

    with patch.object(engine, "_get_table_config", side_effect=patched_get_table_config):
        result = engine.load_table(TEST_LAYER, TEST_TABLE)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0, "Iceberg source table (titanic) has no data — run scripts/seed_iceberg_titanic.py"


# ---------------------------------------------------------------------------
# Spark Engine — reads Iceberg via Spark catalog
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.iceberg
@pytest.mark.spark_engine
@requires_credentials
@requires_aws
def test_load_iceberg_table_spark_engine(integration_spark):
    """Load the Iceberg source table via Spark and verify it returns a Spark DataFrame."""
    from pyspark.sql import DataFrame as SparkDataFrame

    m = _fresh_module()
    m.engine = "spark"

    engine = m._get_engine()
    original_get_table_details = engine._api.get_table_details

    def patched_get_table_details(*args, **kwargs):
        config = original_get_table_details(*args, **kwargs)
        return _patch_to_iceberg(config)

    with patch.object(engine._api, "get_table_details", side_effect=patched_get_table_details):
        result = engine.load_table(TEST_LAYER, TEST_TABLE)

    assert isinstance(result, SparkDataFrame)
    assert result.count() > 0, "Iceberg source table (titanic) has no data — run scripts/seed_iceberg_titanic.py"
