"""
Nekt SDK - Public interface for data lake operations.

Usage::

    import nekt

    nekt.engine = "python"  # or "spark"
    nekt.data_access_token = "your-token"

    # API methods auto-initialize the engine on first call
    secret = nekt.get_secret("my-key")
    files = nekt.load_volume("layer", "volume")
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types as module_types

__version__ = "0.7.0"


class NektModule(module_types.ModuleType):
    """Module wrapper that provides attribute-style configuration for the Nekt SDK.

    Replaces the nekt module in sys.modules so that users can do::

        import nekt
        nekt.engine = "spark"
        nekt.data_access_token = "token"
    """

    def __init__(self, name: str, doc: str | None = None):
        super().__init__(name, doc)
        # All internal state uses _nekt_ prefix and object.__setattr__ to avoid recursion
        object.__setattr__(self, "_nekt_data_access_token", None)
        object.__setattr__(self, "_nekt_api_url", None)
        object.__setattr__(self, "_nekt_engine", None)
        object.__setattr__(self, "_nekt_client", None)
        object.__setattr__(self, "_nekt_locked", False)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_dependency(self, name: str, extras_group: str) -> None:
        """Check if a module is importable; raise MissingDependencyError if not."""
        if importlib.util.find_spec(name) is None:
            from nekt.exceptions import MissingDependencyError

            raise MissingDependencyError(name, extras_group)

    def _ensure_not_locked(self, attr_name: str) -> None:
        """Raise ConfigurationLockedError if config is locked after first API call."""
        if self._nekt_locked:
            from nekt.exceptions import ConfigurationLockedError

            raise ConfigurationLockedError(
                f"Cannot change '{attr_name}' after the first API call."
            )

    def _resolve_config(self) -> None:
        """Resolve config from env vars for any attribute still set to None.

        Validates that engine and data_access_token are available before
        engine initialization.
        """
        # Env var fallback for each config attribute
        if self._nekt_data_access_token is None:
            env_token = os.environ.get("NEKT_DATA_ACCESS_TOKEN")
            if env_token is not None:
                object.__setattr__(self, "_nekt_data_access_token", env_token)

        if self._nekt_api_url is None:
            env_url = os.environ.get("NEKT_API_URL")
            if env_url is not None:
                object.__setattr__(self, "_nekt_api_url", env_url)

        if self._nekt_engine is None:
            env_engine = os.environ.get("NEKT_ENGINE")
            if env_engine is not None:
                if env_engine not in ("spark", "python"):
                    raise ValueError(
                        f"Invalid NEKT_ENGINE value: '{env_engine}'. "
                        "engine must be 'spark' or 'python'"
                    )
                # Validate dependency for env-var-sourced engine
                if env_engine == "spark":
                    self._check_dependency("pyspark", "spark")
                object.__setattr__(self, "_nekt_engine", env_engine)

        # After env var resolution, engine must be set
        if self._nekt_engine is None:
            from nekt.exceptions import EngineNotSetError

            raise EngineNotSetError()

        # Token must be available
        if self._nekt_data_access_token is None:
            from nekt.exceptions import ConfigurationError

            raise ConfigurationError(
                "No data access token configured. "
                "Set nekt.data_access_token or NEKT_DATA_ACCESS_TOKEN."
            )

    def _get_engine(self):
        """Lazy engine initialization (placeholder -- wired in plan 06-02)."""
        if self._nekt_client is not None:
            return self._nekt_client

        self._resolve_config()
        object.__setattr__(self, "_nekt_locked", True)

        # Placeholder: actual engine construction added in plan 06-02
        raise NotImplementedError(
            "Engine initialization will be implemented in plan 06-02"
        )

    # ------------------------------------------------------------------
    # Attribute access
    # ------------------------------------------------------------------

    def __getattr__(self, name: str):
        """Handle attribute reads for configuration and re-exports."""
        if name == "data_access_token":
            return self._nekt_data_access_token
        if name == "api_url":
            return self._nekt_api_url
        if name == "engine":
            return self._nekt_engine
        if name == "_engine":
            return self._nekt_client

        # Re-exported types (lazy to avoid import cycles)
        if name == "Environment":
            from nekt.types import Environment

            return Environment
        if name == "TokenType":
            from nekt.types import TokenType

            return TokenType
        if name == "CloudProvider":
            from nekt.types import CloudProvider

            return CloudProvider
        if name == "SaveMode":
            from nekt.types import SaveMode

            return SaveMode
        if name == "SchemaEvolutionStrategy":
            from nekt.types import SchemaEvolutionStrategy

            return SchemaEvolutionStrategy
        if name == "NektConfig":
            from nekt.config import NektConfig

            return NektConfig
        if name == "NektError":
            from nekt.exceptions import NektError

            return NektError

        raise AttributeError(f"module 'nekt' has no attribute '{name}'")

    def __setattr__(self, name: str, value):
        """Handle attribute writes for configuration parameters."""
        # Allow submodule assignment during import system operations
        if isinstance(value, module_types.ModuleType):
            object.__setattr__(self, name, value)
            return

        if name == "data_access_token":
            self._ensure_not_locked(name)
            object.__setattr__(self, "_nekt_data_access_token", value)
        elif name == "api_url":
            self._ensure_not_locked(name)
            object.__setattr__(self, "_nekt_api_url", value)
        elif name == "engine":
            self._ensure_not_locked(name)
            if value not in ("spark", "python"):
                raise ValueError("engine must be 'spark' or 'python'")
            if value == "spark":
                self._check_dependency("pyspark", "spark")
            object.__setattr__(self, "_nekt_engine", value)
        else:
            object.__setattr__(self, name, value)


# ------------------------------------------------------------------
# Module replacement
# ------------------------------------------------------------------
_module = NektModule(__name__, __doc__)
_module.__file__ = __file__
_module.__path__ = __path__
_module.__package__ = __package__
_module.__version__ = __version__
sys.modules[__name__] = _module
