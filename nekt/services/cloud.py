"""Cloud service for provider detection and credential retrieval."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nekt.api import NektAPI
    from nekt.types import CloudCredentials, CloudProvider


class CloudService:
    """Thin wrapper around NektAPI cloud methods.

    Provides a service-oriented interface for cloud provider detection
    and credential retrieval. Retry and error handling are delegated
    to the underlying NektAPI instance.

    Args:
        api: NektAPI instance for HTTP communication.
    """

    def __init__(self, api: NektAPI) -> None:
        self._api = api

    def detect_provider(self) -> CloudProvider:
        """Detect the organization's cloud provider.

        Returns:
            CloudProvider enum value (AWS or GCP).

        Raises:
            APIError: If the provider cannot be determined.
        """
        return self._api.get_cloud_provider()

    def get_credentials(self) -> CloudCredentials:
        """Get cloud credentials for accessing storage.

        Credentials are cached with a 45-minute TTL by the underlying
        NektAPI instance.

        Returns:
            CloudCredentials with provider-specific credentials.

        Raises:
            AuthenticationError: If credential retrieval fails.
        """
        return self._api.get_cloud_credentials()
