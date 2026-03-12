"""Pure Python engine with read-only operations."""

from __future__ import annotations

import logging
import mimetypes
import os
from typing import TYPE_CHECKING, Any

import requests

from nekt.engine.base import Engine
from nekt.exceptions import EngineError, FileUploadError
from nekt.types import CloudCredentials, CloudProvider, Environment

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

    This engine uses:
    - PyArrow as the internal data format
    - delta-rs (deltalake) for Delta Lake operations (AWS)
    - google-cloud-bigquery for BigQuery operations (GCP)
    - Pandas as the user-facing data format
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
        self._provider = provider
        self._credentials = credentials
        self._environment = environment

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

        # Load via inline provider
        arrow_table = self._load_via_provider(table_config.path)

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
                    presigned_url = url_info.get("presigned_url")

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
        use_s3a = self._environment == Environment.LOCAL and self._provider == CloudProvider.AWS

        return self._api.get_table_details(
            layer_name=layer_name,
            table_name=table_name,
            provider=self._provider,
            include_expectations=False,
            include_delta_fields=self._provider == CloudProvider.AWS,
            include_layer_database_name=True,
            use_s3a=use_s3a,
        )

    def _load_via_provider(self, path: str) -> Any:
        """Load data from storage using the appropriate provider.

        For AWS: Uses deltalake (delta-rs) to read Delta tables.
        For GCP: Uses google-cloud-bigquery to query BigQuery tables.

        Args:
            path: Storage path (S3/s3a path for AWS, BQ table ref for GCP).

        Returns:
            PyArrow Table.

        Raises:
            EngineError: If loading fails or required library is missing.
        """
        storage_options = self._credentials.to_storage_options() if self._credentials else {}

        if self._provider == CloudProvider.AWS:
            return self._load_delta(path, storage_options)
        elif self._provider == CloudProvider.GCP:
            return self._load_bigquery(path, storage_options)
        else:
            raise EngineError(f"Unsupported provider: {self._provider}")

    def _load_delta(self, path: str, storage_options: dict[str, str]) -> Any:
        """Load a Delta table via deltalake.

        Args:
            path: S3 or s3a path to the Delta table.
            storage_options: AWS credentials as storage options.

        Returns:
            PyArrow Table.

        Raises:
            EngineError: If deltalake is not installed or loading fails.
        """
        try:
            import deltalake
        except ImportError as e:
            raise EngineError(
                "deltalake is required for loading Delta tables on AWS. "
                "Install it with: pip install deltalake"
            ) from e

        try:
            dt = deltalake.DeltaTable(path, storage_options=storage_options)
            return dt.to_pyarrow_table()
        except Exception as e:
            raise EngineError(f"Failed to load Delta table at {path}: {e}") from e

    def _load_bigquery(self, path: str, storage_options: dict[str, str]) -> Any:
        """Load a BigQuery table.

        Args:
            path: Fully-qualified BigQuery table reference (dataset.table).
            storage_options: GCP credentials as storage options.

        Returns:
            PyArrow Table.

        Raises:
            EngineError: If google-cloud-bigquery is not installed or loading fails.
        """
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise EngineError(
                "google-cloud-bigquery is required for loading BigQuery tables on GCP. "
                "Install it with: pip install google-cloud-bigquery"
            ) from e

        try:
            access_token = storage_options.get("GOOGLE_SERVICE_ACCOUNT_TOKEN")
            if access_token:
                from google.oauth2.credentials import Credentials

                credentials = Credentials(token=access_token)
                project_id = self._credentials.gcp_project_id if self._credentials else None
                client = bigquery.Client(project=project_id, credentials=credentials)
            else:
                client = bigquery.Client()

            return client.query(f"SELECT * FROM `{path}`").to_arrow()
        except Exception as e:
            raise EngineError(f"Failed to load BigQuery table at {path}: {e}") from e
