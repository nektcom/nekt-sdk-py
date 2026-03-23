# Integration tests: run with NEKT_DATA_ACCESS_TOKEN and NEKT_API_URL env vars set
#
# These tests exercise the real API load path and require valid credentials.
# They are double-gated: both the @pytest.mark.integration marker and the
# @requires_credentials skip condition must be satisfied.
#
# Configurable via env vars:
#   NEKT_TEST_LAYER  — layer to load from (default: Testing)
#   NEKT_TEST_TABLE  — table to load (default: titanic)
#
# Filter by engine: pytest -m "integration and spark_engine"
#                    pytest -m "integration and python_engine"
"""Double-gated integration tests for the public SDK."""

import os
import sys

import pytest

import nekt

NektModule = type(sys.modules["nekt"])

requires_credentials = pytest.mark.skipif(
    not os.environ.get("NEKT_DATA_ACCESS_TOKEN"),
    reason="NEKT_DATA_ACCESS_TOKEN not set",
)

TEST_LAYER = os.environ.get("NEKT_TEST_LAYER", "Testing")
TEST_TABLE = os.environ.get("NEKT_TEST_TABLE", "titanic")
TEST_SECRET_NAME = os.environ.get("NEKT_TEST_SECRET_NAME")
TEST_SECRET_VALUE = os.environ.get("NEKT_TEST_SECRET_VALUE")

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
@pytest.mark.python_engine
@requires_credentials
def test_load_table_python_engine():
    """Load a table via the Python engine and verify it returns a pandas DataFrame."""
    import pandas as pd

    m = _fresh_module()
    m.engine = "python"
    result = m.load_table(layer_name=TEST_LAYER, table_name=TEST_TABLE)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


@pytest.mark.integration
@pytest.mark.spark_engine
@requires_credentials
def test_load_table_spark_engine(integration_spark):
    """Load a table via the Spark engine and verify it returns a Spark DataFrame.

    The integration_spark fixture creates a session with Delta + hadoop-aws + S3 credentials.
    SparkEngine.spark picks it up via getActiveSession().
    """
    from pyspark.sql import DataFrame as SparkDataFrame

    m = _fresh_module()
    m.engine = "spark"
    result = m.load_table(layer_name=TEST_LAYER, table_name=TEST_TABLE)
    assert isinstance(result, SparkDataFrame)
    assert result.count() > 0


@pytest.mark.integration
@pytest.mark.python_engine
@requires_credentials
def test_load_secret():
    """Load a secret via the API."""
    m = _fresh_module()
    m.engine = "python"
    if not TEST_SECRET_NAME:
        pytest.skip("No test secret configured (set NEKT_TEST_SECRET_NAME)")
    result = m.load_secret(key=TEST_SECRET_NAME)
    assert isinstance(result, str)
    assert len(result) > 0
    assert result == TEST_SECRET_VALUE


@pytest.mark.integration
@pytest.mark.spark_engine
@requires_credentials
def test_get_spark_session(integration_spark):
    """Verify the Spark engine initializes and returns a SparkSession."""
    from pyspark.sql import SparkSession

    m = _fresh_module()
    m.engine = "spark"
    session = m.get_spark_session()
    assert isinstance(session, SparkSession)


# ---------------------------------------------------------------------------
# Env-var precedence tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.python_engine
@requires_credentials
def test_env_var_precedence_python_engine():
    """Env var token overrides code-set token (Python engine)."""
    import pandas as pd

    m = _fresh_module()
    m.engine = "python"
    # Set a wrong token via code — env var should override it
    m.data_access_token = "wrong_token_that_would_fail"

    result = m.load_table(layer_name=TEST_LAYER, table_name=TEST_TABLE)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


@pytest.mark.integration
@pytest.mark.spark_engine
@requires_credentials
def test_env_var_precedence_spark_engine(integration_spark):
    """Env var token overrides code-set token (Spark engine)."""
    from pyspark.sql import DataFrame as SparkDataFrame

    m = _fresh_module()
    m.engine = "spark"
    # Set a wrong token via code — env var should override it
    m.data_access_token = "wrong_token_that_would_fail"

    result = m.load_table(layer_name=TEST_LAYER, table_name=TEST_TABLE)
    assert isinstance(result, SparkDataFrame)
    assert result.count() > 0
