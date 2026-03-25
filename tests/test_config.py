"""Tests for NektConfig: defaults, env var reads, locking, and validation."""

import warnings

import pytest

from nekt.config import NektConfig


class TestConfigDefaults:
    """Verify fresh NektConfig has expected default values."""

    def test_default_engine_is_spark(self):
        config = NektConfig()
        assert config.engine == "spark"

    def test_default_data_access_token_is_none(self):
        config = NektConfig()
        assert config.data_access_token is None

    def test_default_api_url(self):
        config = NektConfig()
        assert config.api_url == "https://api.nekt.ai"

    def test_default_environment_is_none(self):
        config = NektConfig()
        assert config.environment is None

    def test_default_token_type_is_none(self):
        config = NektConfig()
        assert config.token_type is None

    def test_default_is_not_locked(self):
        config = NektConfig()
        assert config.is_locked is False


class TestConfigEnvVars:
    """Verify NektConfig reads environment variables at init time."""

    def test_env_var_engine(self, monkeypatch):
        monkeypatch.setenv("NEKT_ENGINE", "python")
        config = NektConfig()
        assert config.engine == "python"

    def test_env_var_token(self, monkeypatch):
        monkeypatch.setenv("NEKT_DATA_ACCESS_TOKEN", "test-tok-abc")
        config = NektConfig()
        assert config.data_access_token == "test-tok-abc"

    def test_env_var_api_url(self, monkeypatch):
        monkeypatch.setenv("NEKT_API_URL", "http://localhost:8000")
        config = NektConfig()
        assert config.api_url == "http://localhost:8000"

    def test_env_var_invalid_engine_raises(self, monkeypatch):
        from nekt.exceptions import ConfigurationError
        monkeypatch.setenv("NEKT_ENGINE", "invalid")
        with pytest.raises(ConfigurationError, match="Invalid NEKT_ENGINE"):
            NektConfig()

    def test_env_var_production(self, monkeypatch):
        from nekt.types import Environment
        monkeypatch.setenv("NEKT_ENV", "PRODUCTION")
        config = NektConfig()
        assert config.environment == Environment.PRODUCTION

    def test_env_var_local(self, monkeypatch):
        from nekt.types import Environment
        monkeypatch.setenv("NEKT_ENV", "LOCAL")
        config = NektConfig()
        assert config.environment == Environment.LOCAL

    def test_env_var_invalid_environment_raises(self, monkeypatch):
        monkeypatch.setenv("NEKT_ENV", "AWS")
        with pytest.raises(ValueError):
            NektConfig()


class TestConfigLocking:
    """Verify lock behavior: warns and skips set after lock."""

    def test_lock_sets_locked_flag(self):
        config = NektConfig()
        config.lock()
        assert config.is_locked is True

    def test_set_engine_after_lock_warns(self):
        config = NektConfig()
        config.lock()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config.set_engine("python")
            assert len(w) == 1
            assert "locked" in str(w[0].message).lower()
        # Value should not have changed
        assert config.engine == "spark"

    def test_set_token_after_lock_warns(self):
        config = NektConfig()
        config.lock()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config.set_data_access_token("new-tok")
            assert len(w) == 1
            assert "locked" in str(w[0].message).lower()
        assert config.data_access_token is None

    def test_set_api_url_after_lock_warns(self):
        config = NektConfig()
        config.lock()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config.set_api_url("http://new-url")
            assert len(w) == 1
        assert config.api_url == "https://api.nekt.ai"


class TestConfigValidation:
    """Verify NektConfig validates engine values."""

    def test_set_engine_invalid_raises(self):
        from nekt.exceptions import ConfigurationError
        config = NektConfig()
        with pytest.raises(ConfigurationError, match="Invalid engine"):
            config.set_engine("invalid")

    def test_set_engine_valid_values(self):
        config = NektConfig()
        config.set_engine("python")
        assert config.engine == "python"
        config.set_engine("spark")
        assert config.engine == "spark"
