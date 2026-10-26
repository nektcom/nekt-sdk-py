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
        from unittest.mock import MagicMock

        m = _fresh_module()
        m.engine = "python"
        # Mock _get_engine to skip API calls — we're testing the guard, not init
        with patch.object(type(m), "_get_engine", return_value=MagicMock()):
            with pytest.raises(EngineError, match="get_spark_session\\(\\) requires engine='spark'"):
                m.get_spark_session()

    def test_load_delta_table_wrong_engine_raises(self):
        from unittest.mock import MagicMock

        m = _fresh_module()
        m.engine = "python"
        with patch.object(type(m), "_get_engine", return_value=MagicMock()):
            with pytest.raises(EngineError, match="load_delta_table\\(\\) requires engine='spark'"):
                m.load_delta_table(layer_name="raw", table_name="test")


class TestMissingDependency:
    """Verify MissingDependencyError when pyspark is not importable."""

    def test_spark_engine_missing_pyspark_raises(self):
        m = _fresh_module()
        with patch("importlib.util.find_spec", return_value=None):
            with pytest.raises(MissingDependencyError, match="pyspark"):
                m.engine = "spark"


class TestEngineReassignment:
    """Setting the engine to the one already in use is a no-op; a different one raises."""

    def test_same_engine_as_env_var_is_noop(self, monkeypatch):
        monkeypatch.setenv("NEKT_ENGINE", "python")
        m = _fresh_module()
        m.engine = "python"
        assert m.engine is None  # untouched: NEKT_ENGINE decides at initialization

    def test_different_engine_from_env_var_raises(self, monkeypatch):
        from nekt.exceptions import ConfigurationLockedError

        monkeypatch.setenv("NEKT_ENGINE", "python")
        m = _fresh_module()
        with pytest.raises(ConfigurationLockedError, match="NEKT_ENGINE"):
            m.engine = "spark"

    def test_same_engine_after_initialization_is_noop(self, monkeypatch):
        monkeypatch.delenv("NEKT_ENGINE", raising=False)
        m = _fresh_module()
        m.engine = "python"
        object.__setattr__(m, "_nekt_locked", True)
        m.engine = "python"
        assert m.engine == "python"

    def test_different_engine_after_initialization_raises(self, monkeypatch):
        from nekt.exceptions import ConfigurationLockedError

        monkeypatch.delenv("NEKT_ENGINE", raising=False)
        m = _fresh_module()
        m.engine = "python"
        object.__setattr__(m, "_nekt_locked", True)
        with pytest.raises(ConfigurationLockedError, match="already 'python'"):
            m.engine = "spark"

    def test_engine_can_change_before_initialization(self, monkeypatch):
        monkeypatch.delenv("NEKT_ENGINE", raising=False)
        m = _fresh_module()
        m.engine = "python"
        with patch("importlib.util.find_spec", return_value=object()):
            m.engine = "spark"
        assert m.engine == "spark"

    def test_initialization_logs_engine_and_source(self, monkeypatch, caplog):
        monkeypatch.setenv("NEKT_ENGINE", "python")
        monkeypatch.setenv("NEKT_DATA_ACCESS_TOKEN", "t")
        m = _fresh_module()
        m.logger.propagate = True
        try:
            with caplog.at_level("INFO", logger="nekt.user"):
                m._resolve_config()
        finally:
            m.logger.propagate = False
        assert "Using the 'python' engine (from NEKT_ENGINE environment variable)" in caplog.text
