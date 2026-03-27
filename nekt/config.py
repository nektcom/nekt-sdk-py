"""Configuration management for the Nekt public SDK."""

from __future__ import annotations

import logging
import os
import warnings
from typing import Literal

from nekt.exceptions import ConfigurationError
from nekt.types import Environment, TokenType

logger = logging.getLogger(__name__)


class NektConfig:
    """Configuration container for the Nekt public SDK.

    Holds read-operation settings: authentication, API URL, engine selection,
    environment, and token type. Supports locking (warn-on-set when locked)
    and auto-reads environment variables on initialization.

    Usage::

        from nekt.config import NektConfig

        config = NektConfig()
        config.set_engine("python")
        config.lock()
        # Further set calls will warn and be ignored.
    """

    def __init__(self) -> None:
        # Authentication
        self._data_access_token: str | None = None
        self._api_url: str = "https://api.nekt.ai"

        # Engine selection
        self._engine: Literal["spark", "python"] = "spark"

        # Environment and token
        self._environment: Environment | None = None
        self._token_type: TokenType | None = None

        # Lock state
        self._locked: bool = False

        # Auto-read from environment variables
        self._read_env_vars()

    def _read_env_vars(self) -> None:
        """Populate fields from environment variables if set."""
        env_token = os.environ.get("NEKT_DATA_ACCESS_TOKEN")
        if env_token is not None:
            self._data_access_token = env_token

        env_api_url = os.environ.get("NEKT_API_URL")
        if env_api_url is not None:
            self._api_url = env_api_url

        env_engine = os.environ.get("NEKT_ENGINE")
        if env_engine is not None:
            if env_engine not in ("spark", "python"):
                raise ConfigurationError(
                    f"Invalid NEKT_ENGINE environment variable: {env_engine}. "
                    "Must be 'spark' or 'python'."
                )
            self._engine = env_engine  # type: ignore[assignment]

        env_environment = os.environ.get("NEKT_ENV")
        if env_environment is not None:
            self._environment = Environment(env_environment)

        env_token_type = os.environ.get("NEKT_TOKEN_TYPE")
        if env_token_type is not None:
            self._token_type = TokenType(env_token_type)

    def _check_not_locked(self, param_name: str) -> bool:
        """Check if configuration is locked.

        Returns:
            True if locked (caller should skip the set), False if unlocked.
        """
        if self._locked:
            warnings.warn(
                f"Configuration is locked: cannot change '{param_name}'. "
                "Create a new configuration instance.",
                stacklevel=3,
            )
            return True
        return False

    def lock(self) -> None:
        """Lock the configuration, preventing further changes."""
        self._locked = True

    @property
    def is_locked(self) -> bool:
        """Check if configuration is locked."""
        return self._locked

    # --- Authentication properties ---

    @property
    def data_access_token(self) -> str | None:
        """The data access token for API authentication."""
        return self._data_access_token

    def set_data_access_token(self, value: str) -> None:
        """Set the data access token."""
        if self._check_not_locked("data_access_token"):
            return
        self._data_access_token = value

    @property
    def api_url(self) -> str:
        """The Nekt API base URL."""
        return self._api_url

    def set_api_url(self, value: str) -> None:
        """Set the API base URL."""
        if self._check_not_locked("api_url"):
            return
        self._api_url = value

    # --- Engine properties ---

    @property
    def engine(self) -> Literal["spark", "python"]:
        """The execution engine ('spark' or 'python')."""
        return self._engine

    def set_engine(self, value: Literal["spark", "python"]) -> None:
        """Set the execution engine.

        Raises:
            ConfigurationError: If value is not 'spark' or 'python'.
        """
        if self._check_not_locked("engine"):
            return
        if value not in ("spark", "python"):
            raise ConfigurationError(
                f"Invalid engine: {value}. Must be 'spark' or 'python'."
            )
        self._engine = value

    # --- Environment properties ---

    @property
    def environment(self) -> Environment | None:
        """The execution environment (LOCAL, PRODUCTION)."""
        return self._environment

    def set_environment(self, value: Environment) -> None:
        """Set the execution environment."""
        if self._check_not_locked("environment"):
            return
        self._environment = value

    # --- Token type properties ---

    @property
    def token_type(self) -> TokenType | None:
        """The authentication token type."""
        return self._token_type

    def set_token_type(self, value: TokenType | str | None) -> None:
        """Set the token type for authentication.

        Args:
            value: TokenType enum, string ('jupyter', 'report', 'pipeline'),
                   or None for auto-detect.
        """
        if self._check_not_locked("token_type"):
            return
        if value is None:
            self._token_type = None
        elif isinstance(value, str):
            self._token_type = TokenType(value)
        else:
            self._token_type = value

    def get_effective_token_type(self) -> TokenType:
        """Get the effective token type based on explicit setting or environment.

        Returns:
            The token type to use for authentication.
        """
        if self._token_type is not None:
            return self._token_type

        # Auto-detect based on environment
        if self._environment == Environment.LOCAL:
            return TokenType.JUPYTER
        return TokenType.PIPELINE
