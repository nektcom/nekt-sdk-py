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
from typing import Any

__version__ = "0.7.0"

_PUBLIC_ALL = [
    "Environment", "TokenType", "CloudProvider", "SaveMode",
    "SchemaEvolutionStrategy", "NektConfig", "NektError",
    "data_access_token", "api_url", "engine",
    "load_table", "load_delta_table", "load_secret",
    "load_volume", "create_volume", "save_file",
    "get_spark_session", "logger", "get_logger",
]


class NektModule(module_types.ModuleType):
    """Module wrapper that provides attribute-style configuration for the Nekt SDK.

    Replaces the nekt module in sys.modules so that users can do::

        import nekt
        nekt.engine = "spark"
        nekt.data_access_token = "token"
    """

    _nekt_data_access_token: str | None
    _nekt_api_url: str | None
    _nekt_engine: str | None
    _nekt_client: Any
    _nekt_locked: bool

    def __init__(self, name: str, doc: str | None = None):
        super().__init__(name, doc)
        # All internal state uses _nekt_ prefix and object.__setattr__ to avoid recursion
        object.__setattr__(self, "_nekt_data_access_token", None)
        object.__setattr__(self, "_nekt_api_url", None)
        object.__setattr__(self, "_nekt_engine", None)
        object.__setattr__(self, "_nekt_client", None)
        object.__setattr__(self, "_nekt_locked", False)
        object.__setattr__(self, "__all__", _PUBLIC_ALL)

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
        """Resolve config, with env vars taking precedence over code-set values.

        Validates that engine and data_access_token are available before
        engine initialization.
        """
        # Env vars always override code-set values (cloud deployments use correct credentials)
        env_token = os.environ.get("NEKT_DATA_ACCESS_TOKEN")
        if env_token is not None:
            object.__setattr__(self, "_nekt_data_access_token", env_token)

        env_url = os.environ.get("NEKT_API_URL")
        if env_url is not None:
            object.__setattr__(self, "_nekt_api_url", env_url)

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

    def _get_engine(self) -> Any:
        """Lazy engine initialization -- creates the appropriate engine on first call.

        1. Return cached engine if already initialized.
        2. Resolve config from env vars and validate.
        3. Lock configuration to prevent changes.
        4. Determine environment from NEKT_ENV (default: LOCAL).
        5. LOCAL: detect cloud provider and credentials from API.
           AWS/GCP: infer provider from env, get credentials from env vars.
        6. Instantiate PythonEngine or SparkEngine based on engine selection.

        Returns:
            The initialized engine instance (PythonEngine or SparkEngine).
        """
        if self._nekt_client is not None:
            return self._nekt_client

        object.__setattr__(self, "_nekt_locked", True)
        self._resolve_config()

        import os

        from nekt.api import NektAPI
        from nekt.config import NektConfig
        from nekt.services.cloud import CloudService
        from nekt.types import CloudCredentials, CloudProvider, Environment

        env_str = os.environ.get("NEKT_ENV", "LOCAL").upper()
        environment = Environment(env_str)

        token: str = self._nekt_data_access_token  # type: ignore[assignment]
        config = NektConfig()
        config.set_data_access_token(token)
        if self._nekt_api_url:
            config.set_api_url(self._nekt_api_url)

        api = NektAPI(
            token,
            api_url=config.api_url,
            environment=environment,
            token_type=config.get_effective_token_type(),
        )

        if environment == Environment.LOCAL:
            # LOCAL mode: detect provider and credentials from API
            cloud = CloudService(api)
            provider_info = cloud.detect_provider()
            credentials = cloud.get_credentials()
        else:
            # PRODUCTION mode: resolve provider from NEKT_CLOUD_PROVIDER
            cloud_provider = os.environ.get("NEKT_CLOUD_PROVIDER", "AWS").upper()
            if cloud_provider == "AWS":
                provider_info = CloudProvider.AWS
                credentials = CloudCredentials.from_aws(
                    access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
                    secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                    session_token=os.environ.get("AWS_SESSION_TOKEN"),
                    region=os.environ.get("AWS_REGION", "us-east-1"),
                )
            else:
                provider_info = CloudProvider.GCP
                credentials = CloudCredentials.from_gcp(
                    access_token=os.environ.get("GCP_ACCESS_TOKEN", ""),
                    project_id=os.environ.get("GCP_PROJECT_ID", ""),
                )

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

    def load_table(self, *, layer_name: str, table_name: str) -> Any:
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

    def load_volume(self, layer_name: str, volume_name: str) -> Any:
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
    ) -> Any:
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
    ) -> Any:
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

    def load_delta_table(self, *, layer_name: str, table_name: str) -> Any:
        """Load a Delta table (Spark DeltaTable object).

        Only available when engine="spark".

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            DeltaTable object.

        Raises:
            EngineError: If engine is not "spark".
        """
        if self._nekt_engine != "spark":
            from nekt.exceptions import EngineError

            raise EngineError(
                "load_delta_table() requires engine='spark'. "
                f"Current engine: '{self._nekt_engine}'"
            )
        return self._get_engine().load_delta_table(layer_name=layer_name, table_name=table_name)

    def get_spark_session(self) -> Any:
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
    # Write stubs -- require nekt-sdk-internal
    # ------------------------------------------------------------------

    def save_table(self, **kwargs: Any) -> None:
        """Save a table (stub -- requires nekt-sdk-internal)."""
        print("save_table is only available when running on Nekt.")
        return None

    def save_dataframe(self, df: Any, path: str, format: str = "parquet") -> None:
        """Save a DataFrame to cloud storage (stub -- only available on Nekt)."""
        print("save_dataframe is only available when running on Nekt.")
        return None

    # ------------------------------------------------------------------
    # Logger
    # ------------------------------------------------------------------

    @property
    def logger(self):
        """User-facing logger. Returns a standard Python logger.

        When the internal SDK is installed, this is overridden to return
        a FluentBit-backed logger.
        """
        cached = self.__dict__.get('_nekt_user_logger')
        if cached is not None:
            return cached

        import logging
        user_logger = logging.getLogger("nekt.user")
        if not user_logger.handlers:
            user_logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            user_logger.addHandler(handler)
            user_logger.propagate = False

        object.__setattr__(self, '_nekt_user_logger', user_logger)
        return user_logger

    def get_logger(self, name: str | None = None) -> Any:
        """Get a named logger or the default user logger.

        Args:
            name: Optional logger name. Messages will be prefixed with [name].
                  If None, returns the default user logger (same as nekt.logger).

        Returns:
            A logging.Logger instance.
        """
        if name is None:
            return self.logger

        import logging
        named = logging.getLogger(f"nekt.user.{name}")
        if not named.handlers:
            named.parent = self.logger
            named.setLevel(logging.INFO)
        return named

    # ------------------------------------------------------------------
    # Attribute access
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        """Handle attribute reads for configuration and re-exports."""
        if name == "data_access_token":
            return self._nekt_data_access_token
        if name == "api_url":
            return self._nekt_api_url
        if name == "engine":
            return self._nekt_engine
        if name == "_engine":
            return self._nekt_client
        if name == "logger":
            return self.logger  # triggers the @property

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

    def __setattr__(self, name: str, value: Any) -> None:
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

# Detect and upgrade to internal module if nekt_internal is installed
try:
    from nekt_internal._bootstrap import upgrade_module  # type: ignore[import-not-found]
    _module = upgrade_module(_module)
    sys.modules[__name__] = _module
except ImportError:
    pass
