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
        """Lazy engine initialization -- creates the appropriate engine on first call.

        1. Return cached engine if already initialized.
        2. Resolve config from env vars and validate.
        3. Lock configuration to prevent changes.
        4. Create NektAPI, detect cloud provider, get credentials.
        5. Instantiate PythonEngine or SparkEngine based on engine selection.

        Returns:
            The initialized engine instance (PythonEngine or SparkEngine).
        """
        if self._nekt_client is not None:
            return self._nekt_client

        object.__setattr__(self, "_nekt_locked", True)
        self._resolve_config()

        # Build NektAPI instance
        from nekt.api import NektAPI
        from nekt.config import NektConfig
        from nekt.services.cloud import CloudService
        from nekt.types import Environment

        config = NektConfig()
        config.set_data_access_token(self._nekt_data_access_token)
        if self._nekt_api_url:
            config.set_api_url(self._nekt_api_url)

        api = NektAPI(
            self._nekt_data_access_token,
            api_url=config.api_url,
            environment=Environment.LOCAL,
            token_type=config.get_effective_token_type(),
        )

        # Detect cloud provider and get credentials
        cloud = CloudService(api)
        provider_info = cloud.detect_provider()
        credentials = cloud.get_credentials()

        # Create the appropriate engine
        if self._nekt_engine == "python":
            from nekt.engine.python import PythonEngine

            engine_instance = PythonEngine(
                api=api,
                provider=provider_info,
                credentials=credentials,
            )
        else:
            from nekt.engine.spark import SparkEngine

            engine_instance = SparkEngine(
                api=api,
                provider=provider_info,
                credentials=credentials,
            )

        object.__setattr__(self, "_nekt_client", engine_instance)
        return engine_instance

    # ------------------------------------------------------------------
    # Read API methods
    # ------------------------------------------------------------------

    def load_table(self, *, layer_name: str, table_name: str):
        """Load a table as a DataFrame.

        The return type depends on the engine: pandas DataFrame for
        ``engine="python"``, Spark DataFrame for ``engine="spark"``.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            A pandas or PySpark DataFrame depending on the engine.
        """
        return self._get_engine().load_table(layer_name=layer_name, table_name=table_name)

    def load_secret(self, *, key: str) -> str:
        """Load a secret value by key.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value.
        """
        return self._get_engine().load_secret(key=key)

    def load_volume(self, layer_name: str, volume_name: str) -> list[dict[str, str]]:
        """Load volume file listings.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.

        Returns:
            List of file-path dictionaries.
        """
        return self._get_engine().load_volume(layer_name=layer_name, volume_name=volume_name)

    def create_volume(
        self,
        layer_name: str,
        volume_name: str,
        description: str | None = None,
    ) -> dict:
        """Create a new volume in a layer.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume to create.
            description: Optional description.

        Returns:
            Volume metadata (id, name, slug, path, etc.).
        """
        return self._get_engine().create_volume(
            layer_name=layer_name, volume_name=volume_name, description=description
        )

    def save_file(
        self,
        layer_name: str,
        volume_name: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict:
        """Save a file to a volume using multipart upload.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume.
            description: Optional description for the file.

        Returns:
            File metadata (id, name, file_size, file_type, description).
        """
        return self._get_engine().save_file(
            layer_name=layer_name,
            volume_name=volume_name,
            file_path=file_path,
            file_name=file_name,
            description=description,
        )

    def get_spark_session(self):
        """Get the SparkSession from the Spark engine.

        Only available when ``engine="spark"``. Triggers engine
        initialization if not already initialized.

        Returns:
            The active SparkSession.

        Raises:
            EngineError: If engine is not ``"spark"``.
        """
        if self._nekt_engine != "spark":
            from nekt.exceptions import EngineError

            raise EngineError(
                f"get_spark_session() requires engine='spark'. "
                f"Current engine: '{self._nekt_engine}'"
            )
        engine = self._get_engine()
        return engine.spark

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
