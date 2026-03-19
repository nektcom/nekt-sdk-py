"""Tests for engine selection: guard errors, dependency checks, EngineNotSetError."""

import sys
from unittest.mock import patch

import pytest

from nekt.exceptions import EngineError, EngineNotSetError, MissingDependencyError

# NektModule replaces the nekt module in sys.modules, so retrieve the class via type()
import nekt

NektModule = type(sys.modules["nekt"])


def _fresh_module():
    """Create a fresh NektModule instance for test isolation."""
    return NektModule("nekt_test")


class TestEngineNotSet:
    """Verify EngineNotSetError when engine is not configured."""

    def test_load_table_without_engine_raises(self, monkeypatch):
        """Calling load_table without engine or token should raise EngineNotSetError."""
        # Ensure no env vars interfere
        monkeypatch.delenv("NEKT_ENGINE", raising=False)
        monkeypatch.delenv("NEKT_DATA_ACCESS_TOKEN", raising=False)
        m = _fresh_module()
        with pytest.raises(EngineNotSetError, match="Engine not set"):
            m.load_table(layer_name="raw", table_name="test")


class TestEngineGuards:
    """Verify engine-type guards on spark-only methods."""

    def test_get_spark_session_wrong_engine_raises(self):
        m = _fresh_module()
        m.engine = "python"
        with pytest.raises(EngineError, match="get_spark_session\\(\\) requires engine='spark'"):
            m.get_spark_session()

    def test_load_delta_table_wrong_engine_raises(self):
        m = _fresh_module()
        m.engine = "python"
        with pytest.raises(EngineError, match="load_delta_table\\(\\) requires engine='spark'"):
            m.load_delta_table(layer_name="raw", table_name="test")


class TestMissingDependency:
    """Verify MissingDependencyError when pyspark is not importable."""

    def test_spark_engine_missing_pyspark_raises(self):
        m = _fresh_module()
        with patch("importlib.util.find_spec", return_value=None):
            with pytest.raises(MissingDependencyError, match="pyspark"):
                m.engine = "spark"
