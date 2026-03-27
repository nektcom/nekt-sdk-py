"""
Iceberg integration tests for the public SDK.

Loads a real Delta table from the Nekt API, monkey-patches the config to
point at a real Iceberg table on S3 Table Buckets, then reads via PyIceberg.

Post-deploy: remove the monkey-patch once the API serves Iceberg fields natively.

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

TEST_LAYER = os.environ.get("NEKT_TEST_LAYER", "Testing")
TEST_TABLE = os.environ.get("NEKT_TEST_TABLE", "titanic")

# Real S3 Table Bucket in nekt-dev (CloudFormation aws-2.0.27)
ICEBERG_TABLE_BUCKET_ARN = os.environ.get(
    "NEKT_ICEBERG_TABLE_BUCKET_ARN",
    "arn:aws:s3tables:us-east-1:637423198484:bucket/nekt-target-s3-iceberg-internal-tests",
)
ICEBERG_TABLE_NAME = "nscd_t01_ft_pk"
ICEBERG_NAMESPACE = "raw"
ICEBERG_CATALOG_ALIAS = "s3tb_001"
ICEBERG_CATALOG_NAME = "s3tablescatalog/nekt-target-s3-iceberg-internal-tests"


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
    """Monkey-patch a Delta TableConfig to point at the real Iceberg table.

    Preserves the original config's provider/credentials but swaps the table
    identity and format to Iceberg. Post-deploy this function is removed.
    """
    return TableConfig(
        layer_name=table_config.layer_name,
        table_name=ICEBERG_TABLE_NAME,
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
def test_load_iceberg_table_python_engine():
    """Load an Iceberg table via PyIceberg and verify it returns a pandas DataFrame."""
    import pandas as pd

    m = _fresh_module()
    m.engine = "python"

    # Trigger engine initialization, then patch _get_table_config
    engine = m._get_engine()
    original_get_table_config = engine._get_table_config

    def patched_get_table_config(layer_name, table_name):
        config = original_get_table_config(layer_name, table_name)
        return _patch_to_iceberg(config)

    with patch.object(engine, "_get_table_config", side_effect=patched_get_table_config):
        result = engine.load_table(TEST_LAYER, TEST_TABLE)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0, "Iceberg table should have data"
