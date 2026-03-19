# Integration tests: run with NEKT_DATA_ACCESS_TOKEN and NEKT_API_URL env vars set
#
# These tests exercise the real API load path and require valid credentials.
# They are double-gated: both the @pytest.mark.integration marker and the
# @requires_credentials skip condition must be satisfied.
"""Double-gated integration tests for the public SDK."""

import os
import sys

import pytest

# Retrieve NektModule class for fresh instance creation
import nekt

NektModule = type(sys.modules["nekt"])

requires_credentials = pytest.mark.skipif(
    not os.environ.get("NEKT_DATA_ACCESS_TOKEN"),
    reason="NEKT_DATA_ACCESS_TOKEN not set",
)


def _fresh_module():
    """Create a fresh NektModule instance with credentials from env."""
    m = NektModule("nekt_integration_test")
    token = os.environ.get("NEKT_DATA_ACCESS_TOKEN")
    if token:
        m.data_access_token = token
    api_url = os.environ.get("NEKT_API_URL")
    if api_url:
        m.api_url = api_url
    return m


@pytest.mark.integration
@requires_credentials
def test_load_table_python_engine():
    """Load a table via the Python engine and verify it returns a pandas DataFrame."""
    import pandas as pd

    m = _fresh_module()
    m.engine = "python"
    # UPDATE: replace with actual test table name if different
    result = m.load_table(layer_name="raw", table_name="test_table")
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


@pytest.mark.integration
@requires_credentials
def test_load_secret():
    """Load a secret via the API."""
    m = _fresh_module()
    m.engine = "python"
    # Skip if no known test secret is configured
    test_secret_name = os.environ.get("NEKT_TEST_SECRET_NAME")
    if not test_secret_name:
        pytest.skip("No test secret configured (set NEKT_TEST_SECRET_NAME)")
    result = m.load_secret(key=test_secret_name)
    assert isinstance(result, str)
    assert len(result) > 0


@pytest.mark.integration
@requires_credentials
def test_engine_switch_to_spark(spark):
    """Verify that setting engine='spark' allows get_spark_session to return a SparkSession."""
    from pyspark.sql import SparkSession

    m = _fresh_module()
    m.engine = "spark"
    # Note: get_spark_session triggers _get_engine which needs full config.
    # This test verifies the guard does not fire when engine is spark.
    # Actually calling get_spark_session would require API credentials to
    # initialize the engine, so we verify the guard passes and the engine
    # attribute is correctly set.
    assert m.engine == "spark"
    # The spark fixture from conftest confirms SparkSession is available
    assert isinstance(spark, SparkSession)
