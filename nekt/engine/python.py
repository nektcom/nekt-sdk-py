"""Pure Python engine with read-only operations."""

from __future__ import annotations

import logging
import mimetypes
import os
from typing import TYPE_CHECKING, Any

import requests

from nekt.engine.base import Engine
from nekt.exceptions import EngineError, FileUploadError
from nekt.provider.base import DataProvider
from nekt.types import CloudCredentials, CloudProvider, Environment, TableFormat

if TYPE_CHECKING:
    import pandas as pd

    from nekt.api import NektAPI

logger = logging.getLogger("nekt.engine.python")


class PythonEngine(Engine):
    """Pure Python engine using PyArrow, delta-rs, and BigQuery SDK.

    Provides read-only access to tables (as pandas DataFrames), secrets,
    and volumes.  Write operations (``save_table``, ``save_dataframe``)
    are stubs inherited from :class:`Engine` -- install ``nekt-sdk-internal``
    for full write support.

    This engine delegates storage loading to a :class:`DataProvider` instance
    selected based on the cloud provider:

    - **AWS** -- :class:`DeltaProvider` (delta-rs / deltalake)
    - **GCP** -- :class:`BigQueryProvider` (google-cloud-bigquery)
    """

    def __init__(
        self,
        api: NektAPI,
        provider: CloudProvider,
        credentials: CloudCredentials | None = None,
        environment: Environment = Environment.LOCAL,
    ) -> None:
        """Initialize the Python engine.

        Args:
            api: NektAPI client for interacting with the Nekt platform.
            provider: Cloud provider (AWS or GCP).
            credentials: Cloud credentials for storage access.
            environment: Execution environment.
        """
        self._api = api
        self._cloud_provider = provider
        self._provider = provider  # backward compat for internal SDK subclass
        self._credentials = credentials
        self._environment = environment
        self._data_provider: DataProvider | None = self._create_data_provider(provider, credentials)

    @staticmethod
    def _create_data_provider(
        provider: CloudProvider,
        credentials: CloudCredentials | None,
    ) -> DataProvider | None:
        """Create the appropriate DataProvider for the given cloud provider.

        Returns ``None`` (with a warning) if the required optional dependency
        is not installed.
        """
        if provider == CloudProvider.AWS:
            try:
                from nekt.provider.delta import DeltaProvider

                return DeltaProvider(credentials=credentials)
            except ImportError:
                logger.warning("deltalake is not installed; DeltaProvider unavailable")
                return None
        elif provider == CloudProvider.GCP:
            try:
                from nekt.provider.bigquery import BigQueryProvider

                project = credentials.gcp_project_id if credentials else None
                return BigQueryProvider(credentials=credentials, project=project)
            except ImportError:
                logger.warning("google-cloud-bigquery is not installed; BigQueryProvider unavailable")
                return None
        else:
            logger.warning("Unsupported cloud provider: %s", provider)
            return None

    @property
    def name(self) -> str:
        """Engine identifier."""
        return "python"

    # ------------------------------------------------------------------
    # Read implementations
    # ------------------------------------------------------------------

    def load_table(self, layer_name: str, table_name: str) -> pd.DataFrame:
        """Load a table and return as pandas DataFrame.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            Pandas DataFrame containing the table data.

        Raises:
            EngineError: If the table cannot be loaded.
        """
        logger.info("[%s/%s] Loading table", layer_name, table_name)

        # Get table configuration from API
        table_config = self._get_table_config(layer_name, table_name)

        # Iceberg tables: use PyIceberg provider
        if table_config.table_format == TableFormat.ICEBERG:
            if table_config.iceberg_config is None:
                raise EngineError(
                    f"Iceberg config missing for table {layer_name}/{table_name}. "
                    "Ensure the table has an S3 Table Bucket assigned."
                )
            try:
                from nekt.provider.iceberg import IcebergProvider
            except ImportError:
                raise EngineError(
                    "pyiceberg is required for loading Iceberg tables. "
                    "Install it with: pip install 'pyiceberg[s3fs]'"
                )
            iceberg_provider = IcebergProvider(
                credentials=self._credentials,
                iceberg_config=table_config.iceberg_config,
            )
            arrow_table = iceberg_provider.load(
                table_config.path,
                table_name=table_config.table_name,
            )
            df = arrow_table.to_pandas()
            logger.info("[%s/%s] Loaded %d rows (Iceberg)", layer_name, table_name, len(df))
            return df

        # Load via data provider
        if self._data_provider is None:
            raise EngineError(
                "No storage provider available. Ensure the required library is installed "
                "(deltalake for AWS, google-cloud-bigquery for GCP)."
            )
        arrow_table = self._data_provider.load(table_config.path)

        # Convert to pandas
        df = arrow_table.to_pandas()
        logger.info("[%s/%s] Loaded %d rows", layer_name, table_name, len(df))

        return df

    def load_secret(self, key: str) -> str:
        """Load a secret value by key.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value.
        """
        return self._api.load_secret(key)

    def load_volume(self, layer_name: str, volume_name: str) -> list[dict[str, str]]:
        """Load volume file listings.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.

        Returns:
            List of file-path dictionaries.
        """
        return self._api.load_volume(layer_name, volume_name)

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
            Volume metadata (id, name, slug, path, etc.).
        """
        logger.info("[%s/%s] Creating volume", layer_name, volume_name)
        return self._api.create_volume(
            layer_name=layer_name,
            volume_name=volume_name,
            description=description,
        )

    def save_file(
        self,
        layer_name: str,
        volume_name: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Save a file to a volume using multipart upload.

        Performs a 3-step upload process:
        1. Create volume file entry and get presigned upload URLs.
        2. Upload file in 100 MB chunks to presigned URLs.
        3. Complete the multipart upload with ETags.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume.
            description: Optional description for the file.

        Returns:
            File metadata (id, name, file_size, file_type, description).

        Raises:
            FileUploadError: If the file is not found, upload fails,
                or completion fails.
        """
        # Validate file exists
        if not os.path.exists(file_path):
            raise FileUploadError(f"File not found: {file_path}")

        # Compute metadata
        file_size = os.path.getsize(file_path)
        name = file_name or os.path.basename(file_path)
        file_type, _ = mimetypes.guess_type(file_path)
        file_type = file_type or "application/octet-stream"

        logger.info("[%s/%s] Saving file %s (%d bytes)", layer_name, volume_name, name, file_size)

        # Step 1: Create volume file and get presigned URLs
        try:
            response = self._api.create_volume_file(
                layer_name=layer_name,
                volume_name=volume_name,
                name=name,
                file_size=file_size,
                file_type=file_type,
                description=description,
            )
        except Exception as e:
            raise FileUploadError(f"Failed to create volume file: {e}") from e

        file_id: str = response["id"]
        presigned_urls: list[dict[str, Any]] = response.get("presigned_url_list", [])

        if not presigned_urls:
            raise FileUploadError("No presigned URLs returned from API")

        # Step 2: Upload in 100 MB chunks
        part_size = 100 * 1024 * 1024  # 100 MB
        parts: list[dict[str, Any]] = []

        try:
            with open(file_path, "rb") as f:
                for url_info in presigned_urls:
                    part_number = url_info.get("part_number", len(parts) + 1)
                    presigned_url: str = url_info.get("presigned_url", "")

                    chunk = f.read(part_size)
                    if not chunk:
                        break

                    logger.debug("Uploading part %d (%d bytes)", part_number, len(chunk))

                    upload_response = requests.put(presigned_url, data=chunk)
                    upload_response.raise_for_status()

                    etag = upload_response.headers.get("ETag", "").strip('"')
                    parts.append({"etag": etag, "part_number": part_number})

        except requests.RequestException as e:
            raise FileUploadError(f"Failed to upload file part: {e}") from e
        except OSError as e:
            raise FileUploadError(f"Failed to read file: {e}") from e

        # Step 3: Complete multipart upload
        try:
            self._api.complete_volume_file_upload(
                layer_name=layer_name,
                volume_name=volume_name,
                file_id=file_id,
                parts=parts,
            )
        except Exception as e:
            raise FileUploadError(f"Failed to complete upload: {e}") from e

        logger.info("[%s/%s] File %s saved", layer_name, volume_name, name)

        return {
            "id": file_id,
            "name": name,
            "file_size": file_size,
            "file_type": file_type,
            "description": description,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_table_config(self, layer_name: str, table_name: str) -> Any:
        """Get table configuration from the API.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            TableConfig with table metadata.
        """
        use_s3a = self._environment != Environment.PRODUCTION and self._cloud_provider == CloudProvider.AWS

        return self._api.get_table_details(
            layer_name=layer_name,
            table_name=table_name,
            provider=self._cloud_provider,
            include_expectations=False,
            include_delta_fields=self._cloud_provider == CloudProvider.AWS,
            include_layer_database_name=True,
            use_s3a=use_s3a,
        )

