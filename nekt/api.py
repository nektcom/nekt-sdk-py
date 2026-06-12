"""Nekt API client for interacting with the Nekt platform."""

from __future__ import annotations

import logging
import mimetypes
import os
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Callable

import requests
from requests.exceptions import ConnectionError, Timeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from nekt.exceptions import (
    APIError,
    AuthenticationError,
    FileUploadError,
    SecretNotFoundError,
    TableNotFoundError,
    VolumeNotFoundError,
)
from nekt.types import CloudCredentials, CloudProvider, Environment, TableConfig, TokenType

logger = logging.getLogger(__name__)

# Default credential cache duration (45 minutes)
# AWS STS credentials expire after 1 hour, so we refresh before that
CREDENTIAL_CACHE_DURATION_MINUTES = 45


class TransientAPIError(Exception):
    """Raised for 5xx server errors to trigger retry."""

    pass


TRANSIENT_EXCEPTIONS = (ConnectionError, Timeout, TransientAPIError)

_api_retry = retry(
    retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.DEBUG),
    reraise=True,
)


class NektAPI:
    """Client for interacting with the Nekt API.

    Handles all HTTP requests to the Nekt platform, including:
    - Table metadata retrieval
    - Secret management
    - Volume access
    - Cloud credential retrieval

    Supports automatic credential caching and refresh for long-running sessions.
    Uses tenacity for retry on transient errors (ConnectionError, Timeout, 5xx).
    4xx errors are NOT retried.
    """

    def __init__(
        self,
        data_access_token: str,
        api_url: str = "https://api.nekt.ai",
        environment: Environment = Environment.LOCAL,
        token_type: TokenType | None = None,
    ) -> None:
        self._token = data_access_token
        self._api_url = api_url.rstrip("/")
        self._environment = environment
        self._token_type = token_type

        # Connection-pooled session
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                **self._get_auth_headers(),
            }
        )

        # Credential cache for automatic refresh
        self._credentials_cache: CloudCredentials | None = None
        self._credentials_expires_at: datetime | None = None

    def _get_effective_token_type(self) -> TokenType:
        """Get the effective token type based on explicit setting or environment.

        Returns:
            The token type to use for authentication.
        """
        if self._token_type is not None:
            return self._token_type

        # Auto-detect based on environment
        if self._environment == Environment.LOCAL:
            return TokenType.JUPYTER
        else:
            return TokenType.PIPELINE

    def _get_auth_headers(self) -> dict[str, str]:
        """Get the appropriate authentication headers based on token type.

        Returns:
            Dictionary with the appropriate auth header.
        """
        token_type = self._get_effective_token_type()

        if token_type == TokenType.REPORT:
            return {"X-Report-Token": self._token}
        elif token_type == TokenType.JUPYTER:
            return {"X-Jupyter-Token": self._token}
        else:  # PIPELINE
            return {"X-Pipeline-Run-Token": self._token}

    def _check_response(self, response: requests.Response, context: str) -> None:
        """Check response status and raise appropriate exceptions.

        Raises TransientAPIError for 5xx (triggering retry), and specific
        SDK exceptions for 4xx errors (no retry).

        Args:
            response: The HTTP response to check.
            context: Description of the request for error messages.

        Raises:
            TransientAPIError: For 5xx server errors (retryable).
            AuthenticationError: For 401/403 responses.
            TableNotFoundError: For 404 on table endpoints.
            SecretNotFoundError: For 404 on secret endpoints.
            VolumeNotFoundError: For 404 on volume endpoints.
            APIError: For other 4xx errors.
        """
        if response.ok:
            return

        status = response.status_code

        # 5xx: transient, trigger retry
        if status >= 500:
            raise TransientAPIError(f"Server error ({status}) for {context}: {response.text}")

        # 401/403: authentication error (no retry)
        if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            raise AuthenticationError(
                f"Authentication failed for {context}",
                status_code=status,
            )

        # 404: resource-specific not-found errors
        if status == HTTPStatus.NOT_FOUND:
            if "table" in context:
                raise TableNotFoundError(
                    f"Table not found: {context}",
                    status_code=status,
                )
            elif "secret" in context:
                raise SecretNotFoundError(
                    f"Secret not found: {context}",
                    status_code=status,
                )
            elif "volume" in context:
                raise VolumeNotFoundError(
                    f"Volume not found: {context}",
                    status_code=status,
                )
            else:
                raise APIError(
                    f"Resource not found: {context}",
                    status_code=status,
                )

        # Other 4xx: generic API error (no retry)
        raise APIError(
            f"API request failed for {context}: {response.text}",
            status_code=status,
        )

    @_api_retry
    def get_table_details(
        self,
        layer_name: str,
        table_name: str,
        provider: CloudProvider,
        include_expectations: bool = False,
        include_delta_fields: bool = False,
        include_layer_database_name: bool = False,
        use_s3a: bool = False,
    ) -> TableConfig:
        """Get table details from the API.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.
            provider: Cloud provider (AWS or GCP).
            include_expectations: Include data quality expectations.
            include_delta_fields: Include Delta-specific fields (AWS only).
            include_layer_database_name: Include the database name for the layer.
            use_s3a: Use s3a:// scheme for paths (AWS only).

        Returns:
            TableConfig with all table metadata.

        Raises:
            TableNotFoundError: If the table does not exist.
            AuthenticationError: If access is denied.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        url = f"{self._api_url}/api/v1/i/layers/{layer_name}/tables/{table_name}/"

        params = {}
        if include_expectations:
            params["include_expectations"] = "true"
        if include_delta_fields:
            params["include_delta_fields"] = "true"
        if include_layer_database_name:
            params["include_layer_database_name"] = "true"
        if provider == CloudProvider.AWS:
            params["include_iceberg_fields"] = "true"
        if use_s3a and provider == CloudProvider.AWS:
            params["use_s3a"] = "true"

        response = self._session.get(url, params=params)
        self._check_response(response, f"table {layer_name}/{table_name}")

        api_data = response.json()
        return TableConfig.from_api_response(layer_name, table_name, provider, api_data)

    @_api_retry
    def get_table_details_raw(
        self,
        layer_name: str,
        table_name: str,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Get raw table details from the API.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.
            params: Optional query parameters.

        Returns:
            Raw API response as dictionary.

        Raises:
            APIError: If the request fails.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        url = f"{self._api_url}/api/v1/i/layers/{layer_name}/tables/{table_name}/"
        response = self._session.get(url, params=params)
        self._check_response(response, f"table {layer_name}/{table_name}")

        return response.json()

    @_api_retry
    def get_cloud_provider(self) -> CloudProvider:
        """Get the organization's cloud provider.

        Returns:
            CloudProvider enum value.

        Raises:
            APIError: If the provider cannot be determined.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        token_type = self._get_effective_token_type()

        # Use different endpoints based on token type
        if token_type == TokenType.REPORT:
            url = f"{self._api_url}/api/v1/report-cloud-provider/"
        else:
            url = f"{self._api_url}/api/v1/organization/notebooks-cloud-provider/"

        response = self._session.get(url)
        self._check_response(response, "cloud provider")

        data = response.json()
        provider_str = data.get("cloud_provider", "").upper()

        if provider_str == "AWS":
            return CloudProvider.AWS
        elif provider_str == "GCP":
            return CloudProvider.GCP
        else:
            raise APIError(f"Unknown cloud provider: {provider_str}")

    def get_cloud_credentials(self, force_refresh: bool = False) -> CloudCredentials:
        """Get cloud credentials for accessing storage.

        Credentials are cached and automatically refreshed before expiration.
        Uses a 45-minute TTL (AWS STS credentials expire after 1 hour).

        Args:
            force_refresh: Force a refresh even if cached credentials are valid.

        Returns:
            CloudCredentials with provider-specific credentials.

        Raises:
            AuthenticationError: If credential retrieval fails.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        # Check cache (unless force refresh)
        if not force_refresh and self._credentials_cache and self._credentials_expires_at:
            now = datetime.now(timezone.utc)
            if now < self._credentials_expires_at:
                logger.debug(
                    "Using cached credentials (expires in %s)",
                    self._credentials_expires_at - now,
                )
                return self._credentials_cache

        # Fetch fresh credentials
        try:
            credentials = self._fetch_cloud_credentials()
        except AuthenticationError:
            # Clear cache and re-raise -- caller may need to re-authenticate
            self.clear_credentials_cache()
            raise

        # Cache with expiration
        self._credentials_cache = credentials
        self._credentials_expires_at = datetime.now(timezone.utc) + timedelta(minutes=CREDENTIAL_CACHE_DURATION_MINUTES)

        logger.debug(
            "Fetched fresh credentials, cached until %s",
            self._credentials_expires_at,
        )
        return credentials

    @_api_retry
    def _fetch_cloud_credentials(self) -> CloudCredentials:
        """Fetch cloud credentials from the API.

        Uses the appropriate endpoint based on token type.

        Returns:
            CloudCredentials with provider-specific credentials.

        Raises:
            AuthenticationError: If access is denied.
            APIError: If the response cannot be parsed.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        token_type = self._get_effective_token_type()

        # Use different endpoints based on token type
        if token_type == TokenType.REPORT:
            url = f"{self._api_url}/api/v1/report-credentials/"
        else:
            url = f"{self._api_url}/api/v1/jupyter-credentials/"

        response = self._session.get(url)
        self._check_response(response, "cloud credentials")

        data = response.json()

        # Determine provider from response
        if "aws_access_key_id" in data:
            return CloudCredentials.from_aws(
                access_key_id=data["aws_access_key_id"],
                secret_access_key=data["aws_secret_access_key"],
                session_token=data.get("aws_session_token"),
                region=data.get("region_name"),
            )
        elif "access_token" in data:
            return CloudCredentials.from_gcp(
                access_token=data["access_token"],
                project_id=data.get("project_id", ""),
            )
        else:
            raise APIError("Unable to parse cloud credentials from API response")

    def clear_credentials_cache(self) -> None:
        """Clear the cached credentials."""
        self._credentials_cache = None
        self._credentials_expires_at = None

    @_api_retry
    def load_secret(self, key: str) -> str:
        """Load a secret value by key from the organization secrets.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret doesn't exist.
            AuthenticationError: If access is denied.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        url = f"{self._api_url}/api/v1/organization/secrets/{key}/"
        response = self._session.get(url)

        if response.status_code == HTTPStatus.OK:
            data = response.json()
            return data.get("value", "")

        self._check_response(response, f"secret {key}")
        return ""  # unreachable, but keeps type checker happy

    @_api_retry
    def load_volume(self, layer_name: str, volume_name: str) -> list[dict[str, str]]:
        """Load volume file paths from the API.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.

        Returns:
            List of file path dictionaries.

        Raises:
            ValueError: If layer_name or volume_name is empty.
            VolumeNotFoundError: If the volume doesn't exist.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        if not layer_name:
            raise ValueError("Layer name is required")
        if not volume_name:
            raise ValueError("Volume name is required")

        url = f"{self._api_url}/api/v1/i/layers/{layer_name}/volumes/{volume_name}/get-file-paths/"
        response = self._session.get(url)
        self._check_response(response, f"volume {layer_name}/{volume_name}")

        return response.json()

    @_api_retry
    def create_volume(
        self,
        layer_name: str,
        volume_name: str,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a new volume in a layer.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume to create.
            description: Optional description.

        Returns:
            Dict containing volume metadata (id, name, slug, s3_path, etc.).

        Raises:
            ValueError: If layer_name or volume_name is empty.
            APIError: If the layer is not found or creation fails.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        if not layer_name:
            raise ValueError("Layer name is required")
        if not volume_name:
            raise ValueError("Volume name is required")

        url = f"{self._api_url}/api/v1/i/layers/{layer_name}/volumes/"

        payload: dict[str, Any] = {
            "name": volume_name,
        }
        if description:
            payload["description"] = description

        response = self._session.post(url, json=payload)
        self._check_response(response, f"create volume {volume_name} in layer {layer_name}")

        return response.json()

    @_api_retry
    def create_volume_file(
        self,
        layer_name: str,
        volume_name: str,
        name: str,
        file_size: int,
        file_type: str,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a volume file and get presigned upload URLs.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            name: Name of the file.
            file_size: Size of the file in bytes.
            file_type: MIME type of the file.
            description: Optional description.

        Returns:
            Dict containing file metadata and presigned_url_list for upload.

        Raises:
            ValueError: If layer_name or volume_name is empty.
            VolumeNotFoundError: If the volume doesn't exist.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        if not layer_name:
            raise ValueError("Layer name is required")
        if not volume_name:
            raise ValueError("Volume name is required")

        url = f"{self._api_url}/api/v1/i/layers/{layer_name}/volumes/{volume_name}/files/"

        payload: dict[str, Any] = {
            "name": name,
            "file_size": file_size,
            "file_type": file_type,
        }
        if description:
            payload["description"] = description

        response = self._session.post(url, json=payload)
        self._check_response(response, f"create volume file in {layer_name}/{volume_name}")

        return response.json()

    @_api_retry
    def complete_volume_file_upload(
        self,
        layer_name: str,
        volume_name: str,
        file_id: str,
        parts: list[dict[str, Any]],
    ) -> None:
        """Complete a multipart upload for a volume file.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_id: ID of the volume file.
            parts: List of dicts with 'etag' and 'part_number' keys.

        Raises:
            ValueError: If layer_name or volume_name is empty.
            VolumeNotFoundError: If the volume file doesn't exist.
            TransientAPIError: On 5xx server errors (will be retried).
        """
        if not layer_name:
            raise ValueError("Layer name is required")
        if not volume_name:
            raise ValueError("Volume name is required")

        url = f"{self._api_url}/api/v1/i/layers/{layer_name}/volumes/{volume_name}/files/{file_id}/complete/"

        payload = {
            "file_upload_parts": parts,
        }

        response = self._session.post(url, json=payload)
        self._check_response(response, f"complete upload for volume {layer_name}/{volume_name}/{file_id}")

    @_api_retry
    def create_volume_file_by_volume_id(
        self,
        volume_identifier: str,
        name: str,
        file_size: int,
        file_type: str,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a volume file by volume id (layer inferred) and get presigned URLs.

        Same as ``create_volume_file`` but targets the layerless endpoint, for
        runs that know only the volume id.

        Args:
            volume_identifier: Id of the volume.
            name: Name of the file.
            file_size: Size of the file in bytes.
            file_type: MIME type of the file.
            description: Optional description.

        Returns:
            Dict containing file metadata and presigned_url_list for upload.
        """
        if not volume_identifier:
            raise ValueError("Volume identifier is required")

        url = f"{self._api_url}/api/v1/i/volumes/{volume_identifier}/files/"

        payload: dict[str, Any] = {
            "name": name,
            "file_size": file_size,
            "file_type": file_type,
        }
        if description:
            payload["description"] = description

        response = self._session.post(url, json=payload)
        self._check_response(response, f"create volume file in volume {volume_identifier}")

        return response.json()

    @_api_retry
    def complete_volume_file_upload_by_volume_id(
        self,
        volume_identifier: str,
        file_id: str,
        parts: list[dict[str, Any]],
    ) -> None:
        """Complete a multipart upload by volume id (layer inferred).

        Same as ``complete_volume_file_upload`` but targets the layerless endpoint.

        Args:
            volume_identifier: ID of the volume.
            file_id: ID of the volume file.
            parts: List of dicts with 'etag' and 'part_number' keys.
        """
        if not volume_identifier:
            raise ValueError("Volume identifier is required")

        url = f"{self._api_url}/api/v1/i/volumes/{volume_identifier}/files/{file_id}/complete/"
        response = self._session.post(url, json={"file_upload_parts": parts})
        self._check_response(response, f"complete upload for volume {volume_identifier}/{file_id}")

    def upload_file(
        self,
        layer_name: str,
        volume_name: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Upload a local file to a volume (by layer + volume name) via multipart.

        Performs the full 3-step upload: create file entry (presigned URLs) ->
        PUT each part -> complete.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume (defaults to the
                local file's basename).
            description: Optional description for the file.

        Returns:
            File metadata: ``id``, ``name``, ``file_size``, ``file_type``,
            ``description``.

        Raises:
            FileUploadError: If the file is missing, or any upload step fails.
        """
        return self._run_file_upload(
            file_path=file_path,
            file_name=file_name,
            description=description,
            target=f"{layer_name}/{volume_name}",
            create=lambda name, size, ftype: self.create_volume_file(
                layer_name=layer_name,
                volume_name=volume_name,
                name=name,
                file_size=size,
                file_type=ftype,
                description=description,
            ),
            complete=lambda file_id, parts: self.complete_volume_file_upload(
                layer_name=layer_name,
                volume_name=volume_name,
                file_id=file_id,
                parts=parts,
            ),
        )

    def upload_file_by_volume_id(
        self,
        volume_identifier: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Upload a local file to a volume by id (layer inferred) via multipart.

        Identical to ``upload_file`` but targets the layerless volume endpoints,
        for runs that know only the volume id.

        Args:
            volume_identifier: Id of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume (defaults to the
                local file's basename).
            description: Optional description for the file.

        Returns:
            File metadata: ``id``, ``name``, ``file_size``, ``file_type``,
            ``description``.

        Raises:
            FileUploadError: If the file is missing, or any upload step fails.
        """
        return self._run_file_upload(
            file_path=file_path,
            file_name=file_name,
            description=description,
            target=volume_identifier,
            create=lambda name, size, ftype: self.create_volume_file_by_volume_id(
                volume_identifier=volume_identifier,
                name=name,
                file_size=size,
                file_type=ftype,
                description=description,
            ),
            complete=lambda file_id, parts: self.complete_volume_file_upload_by_volume_id(
                volume_identifier=volume_identifier,
                file_id=file_id,
                parts=parts,
            ),
        )

    def _run_file_upload(
        self,
        *,
        file_path: str,
        file_name: str | None,
        description: str | None,
        target: str,
        create: Callable[[str, int, str], dict[str, Any]],
        complete: Callable[[str, list[dict[str, Any]]], None],
    ) -> dict[str, Any]:
        """Shared 3-step upload used by both upload_file variants."""
        if not os.path.exists(file_path):
            raise FileUploadError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        name = file_name or os.path.basename(file_path)
        file_type, _ = mimetypes.guess_type(name)
        file_type = file_type or "application/octet-stream"

        logger.info("[%s] Uploading file %s (%d bytes)", target, name, file_size)

        # Step 1: register the file and get presigned URLs.
        try:
            response = create(name, file_size, file_type)
        except Exception as e:
            raise FileUploadError(f"Failed to create volume file: {e}") from e

        file_id: str = response["id"]
        presigned_urls: list = response.get("presigned_url_list", [])
        if not presigned_urls:
            raise FileUploadError("No presigned URLs returned from API")

        # Step 2: upload the file across the presigned URLs.
        parts = self._upload_parts(file_path, file_size, presigned_urls)

        # Step 3: complete the multipart upload.
        try:
            complete(file_id, parts)
        except Exception as e:
            raise FileUploadError(f"Failed to complete upload: {e}") from e

        logger.info("[%s] File %s uploaded (id=%s)", target, name, file_id)
        return {
            "id": file_id,
            "name": name,
            "file_size": file_size,
            "file_type": file_type,
            "description": description,
        }

    def _upload_parts(
        self,
        file_path: str,
        file_size: int,
        presigned_urls: list,
    ) -> list[dict[str, Any]]:
        """PUT the file across the presigned URLs and return the part list.

        The API may return either a flat list of URL strings (one per part) or a
        list of dicts ({part_number, presigned_url}). For the flat list, split the
        file into len(urls) equal parts; for the dict list, use fixed 100 MB chunks.
        """
        is_url_strings = isinstance(presigned_urls[0], str)
        part_size = (
            -(-file_size // len(presigned_urls))  # ceil-divide into N equal parts
            if is_url_strings
            else 100 * 1024 * 1024
        )
        parts: list[dict[str, Any]] = []
        try:
            with open(file_path, "rb") as f:
                for idx, url_info in enumerate(presigned_urls, start=1):
                    if isinstance(url_info, str):
                        presigned_url, part_number = url_info, idx
                    else:
                        presigned_url = url_info.get("presigned_url", "")
                        part_number = url_info.get("part_number", idx)

                    chunk = f.read(part_size)
                    if not chunk:
                        break

                    etag = self._upload_part(presigned_url, chunk, part_number)
                    parts.append({"etag": etag, "part_number": part_number})
        except requests.RequestException as e:
            raise FileUploadError(f"Failed to upload file part: {e}") from e
        except OSError as e:
            raise FileUploadError(f"Failed to read file: {e}") from e
        return parts

    @_api_retry
    def _upload_part(self, presigned_url: str, chunk: bytes, part_number: int) -> str:
        """PUT a single part to its presigned URL, returning its ETag.

        Retries on transient errors (5xx / connection / timeout) like the other
        API calls; 4xx are raised immediately.
        """
        logger.debug("Uploading part %d (%d bytes)", part_number, len(chunk))
        response = requests.put(presigned_url, data=chunk)
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            raise TransientAPIError(f"Server error ({response.status_code}) uploading part {part_number}: {response.text}")
        response.raise_for_status()
        return response.headers.get("ETag", "").strip('"')
