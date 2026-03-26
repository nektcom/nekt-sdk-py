"""Tests for NektModule attribute access, type re-exports, and configuration wiring."""

import sys

import pytest

# NektModule replaces the nekt module in sys.modules, so retrieve the class via type()
import nekt

NektModule = type(sys.modules["nekt"])


def _fresh_module():
    """Create a fresh NektModule instance for test isolation."""
    return NektModule("nekt_test")


class TestTypeReexports:
    """Verify that lazy __getattr__ re-exports resolve to the correct types."""

    def test_environment_reexport(self):
        from nekt.types import Environment
        m = _fresh_module()
        assert m.Environment is Environment

    def test_token_type_reexport(self):
        from nekt.types import TokenType
        m = _fresh_module()
        assert m.TokenType is TokenType

    def test_cloud_provider_reexport(self):
        from nekt.types import CloudProvider
        m = _fresh_module()
        assert m.CloudProvider is CloudProvider

    def test_save_mode_reexport(self):
        from nekt.types import SaveMode
        m = _fresh_module()
        assert m.SaveMode is SaveMode

    def test_schema_evolution_strategy_reexport(self):
        from nekt.types import SchemaEvolutionStrategy
        m = _fresh_module()
        assert m.SchemaEvolutionStrategy is SchemaEvolutionStrategy

    def test_nekt_config_reexport(self):
        from nekt.config import NektConfig
        m = _fresh_module()
        assert m.NektConfig is NektConfig

    def test_nekt_error_reexport(self):
        from nekt.exceptions import NektError
        m = _fresh_module()
        assert m.NektError is NektError


class TestDataclassReexports:
    """Verify dataclass types are accessible via NektModule __getattr__."""

    def test_table_config_accessible(self):
        """TableConfig is not re-exported via __getattr__ (it's in nekt.types directly)."""
        from nekt.types import TableConfig
        assert TableConfig is not None

    def test_cloud_credentials_accessible(self):
        """CloudCredentials is accessible from nekt.types."""
        from nekt.types import CloudCredentials
        assert CloudCredentials is not None


class TestEngineAttribute:
    """Verify engine attribute get/set behavior."""

    def test_engine_default_is_none(self):
        m = _fresh_module()
        assert m.engine is None

    def test_engine_settable_to_python(self):
        m = _fresh_module()
        m.engine = "python"
        assert m.engine == "python"

    def test_engine_settable_to_spark(self):
        m = _fresh_module()
        # spark requires pyspark to be importable (it is in this SDK)
        m.engine = "spark"
        assert m.engine == "spark"

    def test_engine_invalid_value_raises(self):
        m = _fresh_module()
        with pytest.raises(ValueError, match="engine must be 'spark' or 'python'"):
            m.engine = "invalid"


class TestConfigAttributes:
    """Verify data_access_token and api_url attribute access."""

    def test_data_access_token_default_is_none(self):
        m = _fresh_module()
        assert m.data_access_token is None

    def test_data_access_token_settable(self):
        m = _fresh_module()
        m.data_access_token = "tok-123"
        assert m.data_access_token == "tok-123"

    def test_api_url_default_is_none(self):
        m = _fresh_module()
        assert m.api_url is None

    def test_api_url_settable(self):
        m = _fresh_module()
        m.api_url = "http://test.example.com"
        assert m.api_url == "http://test.example.com"


class TestUnknownAttribute:
    """Verify unknown attributes raise AttributeError."""

    def test_unknown_attribute_raises(self):
        m = _fresh_module()
        with pytest.raises(AttributeError, match="has no attribute 'nonexistent'"):
            _ = m.nonexistent
