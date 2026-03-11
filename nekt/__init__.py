"""
Nekt SDK - Public interface for data lake operations.

Usage::

    import nekt

    # Explicit initialization
    nekt.init(token="your-token")

    # Or auto-configure from NEKT_DATA_ACCESS_TOKEN env var
    secret = nekt.get_secret("my-key")
    files = nekt.load_volume("layer", "volume")
"""

from __future__ import annotations

from typing import Any

__version__ = "0.7.0"

from nekt.config import NektConfig  # noqa: F401
from nekt.exceptions import NektError  # noqa: F401
from nekt.types import (  # noqa: F401
    CloudProvider,
    Environment,
    SaveMode,
    SchemaEvolutionStrategy,
    TokenType,
)

# Lazy singleton for flat API
_api: Any = None  # typed as Any to avoid import at module level


def _get_api() -> Any:
    """Get or create the lazy NektAPI singleton.

    Auto-configures from environment variables on first call.

    Returns:
        NektAPI instance.

    Raises:
        ConfigurationError: If no data access token is configured.
    """
    global _api
    if _api is None:
        from nekt.api import NektAPI
        from nekt.exceptions import ConfigurationError

        config = NektConfig()
        if config.data_access_token is None:
            raise ConfigurationError(
                "No data access token configured. "
                "Set NEKT_DATA_ACCESS_TOKEN or call nekt.init(token=...)"
            )

        _api = NektAPI(
            config.data_access_token,
            api_url=config.api_url,
            environment=config.environment or Environment.LOCAL,
            token_type=config.token_type,
        )
    return _api


def init(
    token: str | None = None,
    api_url: str | None = None,
    environment: str | None = None,
) -> None:
    """Initialize the Nekt SDK with explicit configuration.

    Creates a new NektAPI instance and caches it as the module singleton.
    Any previously created singleton is replaced.

    Args:
        token: Data access token. If None, reads from NEKT_DATA_ACCESS_TOKEN.
        api_url: API base URL override.
        environment: Environment override (e.g., 'LOCAL', 'AWS', 'GCP').
    """
    global _api
    from nekt.api import NektAPI
    from nekt.exceptions import ConfigurationError

    config = NektConfig()

    if token is not None:
        config.set_data_access_token(token)
    if api_url is not None:
        config.set_api_url(api_url)
    if environment is not None:
        config.set_environment(Environment(environment))

    config.lock()

    if config.data_access_token is None:
        raise ConfigurationError(
            "No data access token configured. "
            "Set NEKT_DATA_ACCESS_TOKEN or pass token= to nekt.init()"
        )

    _api = NektAPI(
        config.data_access_token,
        api_url=config.api_url,
        environment=config.environment or Environment.LOCAL,
        token_type=config.token_type,
    )


def _reset() -> None:
    """Reset the module singleton (for testing)."""
    global _api
    _api = None


def get_secret(key: str) -> str:
    """Load a secret value by key.

    Args:
        key: The secret key to retrieve.

    Returns:
        The secret value.

    Raises:
        SecretNotFoundError: If the secret doesn't exist.
        ConfigurationError: If the SDK is not configured.
    """
    return _get_api().load_secret(key)


def load_volume(layer_name: str, volume_name: str) -> list[dict[str, str]]:
    """Load volume file listings.

    Args:
        layer_name: Name of the layer.
        volume_name: Name of the volume.

    Returns:
        List of file path dictionaries.

    Raises:
        VolumeNotFoundError: If the volume doesn't exist.
        ConfigurationError: If the SDK is not configured.
    """
    return _get_api().load_volume(layer_name, volume_name)


def create_volume(
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
        Dict containing volume metadata.

    Raises:
        APIError: If the layer is not found or creation fails.
        ConfigurationError: If the SDK is not configured.
    """
    return _get_api().create_volume(layer_name, volume_name, description)


def save_file(
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
        Dict with file metadata: id, name, file_size, file_type, description.

    Raises:
        FileUploadError: If the upload fails.
        ConfigurationError: If the SDK is not configured.
    """
    from nekt.services.volumes import VolumeService

    service = VolumeService(_get_api())
    return service.save_file(layer_name, volume_name, file_path, file_name, description)
