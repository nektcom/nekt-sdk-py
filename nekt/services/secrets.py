"""Secret service for loading organization secrets."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nekt.api import NektAPI


class SecretService:
    """Thin wrapper around NektAPI secret methods.

    Provides a service-oriented interface for loading secrets.
    Retry and error handling are delegated to the underlying NektAPI instance.

    Args:
        api: NektAPI instance for HTTP communication.
    """

    def __init__(self, api: NektAPI) -> None:
        self._api = api

    def load_secret(self, key: str) -> str:
        """Load a secret value by key.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret doesn't exist.
            AuthenticationError: If access is denied.
        """
        return self._api.load_secret(key)
