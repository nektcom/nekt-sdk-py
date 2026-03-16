"""Parquet file providers for S3 and GCS storage.

Minimal load-only implementations.  These are rarely used in production
but included for completeness.
"""

from __future__ import annotations

import logging
from typing import Any

from nekt.exceptions import ParquetProviderError
from nekt.provider.base import DataProvider
from nekt.types import CloudCredentials

logger = logging.getLogger(__name__)


class S3ParquetProvider(DataProvider):
    """Parquet provider for AWS S3 using s3fs.

    Handles reading Parquet files from S3 using explicit credentials
    for cross-account access.
    """

    def __init__(self, credentials: CloudCredentials | None = None) -> None:
        """Initialize the S3 Parquet provider.

        Args:
            credentials: AWS credentials for accessing S3.
        """
        self._credentials = credentials

    @property
    def name(self) -> str:
        """Provider name identifier."""
        return "s3-parquet"

    def _get_filesystem(self) -> Any:
        """Get a configured s3fs filesystem."""
        import s3fs

        if self._credentials:
            return s3fs.S3FileSystem(
                anon=False,
                key=self._credentials.aws_access_key_id,
                secret=self._credentials.aws_secret_access_key,
                token=self._credentials.aws_session_token,
                client_kwargs={"region_name": self._credentials.aws_region or "us-east-1"},
            )
        return s3fs.S3FileSystem(anon=False)

    def load(self, path: str, **options: Any) -> Any:
        """Load a Parquet file from S3 as a PyArrow Table.

        Args:
            path: S3 path to the Parquet file
                (e.g. ``s3://bucket/path/to/file.parquet``).
            **options: Additional options passed to ``pq.read_table``.

        Returns:
            PyArrow Table containing the data.

        Raises:
            ParquetProviderError: If loading fails.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as e:
            raise ParquetProviderError(
                "pyarrow is required for loading Parquet files. "
                "Install it with: pip install pyarrow"
            ) from e

        try:
            fs = self._get_filesystem()
            with fs.open(path, "rb") as f:
                return pq.read_table(f, **options)
        except ParquetProviderError:
            raise
        except Exception as e:
            raise ParquetProviderError(f"Failed to load Parquet file from {path}: {e}") from e


class GCSParquetProvider(DataProvider):
    """Parquet provider for GCP GCS using gcsfs.

    Handles reading Parquet files from GCS using OAuth2 credentials
    for cross-project access.
    """

    def __init__(self, credentials: CloudCredentials | None = None) -> None:
        """Initialize the GCS Parquet provider.

        Args:
            credentials: GCP credentials for accessing GCS.
        """
        self._credentials = credentials

    @property
    def name(self) -> str:
        """Provider name identifier."""
        return "gcs-parquet"

    def _get_filesystem(self) -> Any:
        """Get a configured gcsfs filesystem."""
        import gcsfs

        if self._credentials and self._credentials.gcp_access_token:
            from google.oauth2.credentials import Credentials

            gcp_credentials = Credentials(token=self._credentials.gcp_access_token)
            return gcsfs.GCSFileSystem(
                token=gcp_credentials,
                project=self._credentials.gcp_project_id,
            )
        return gcsfs.GCSFileSystem()

    def load(self, path: str, **options: Any) -> Any:
        """Load a Parquet file from GCS as a PyArrow Table.

        Args:
            path: GCS path to the Parquet file
                (e.g. ``gs://bucket/path/to/file.parquet``).
            **options: Additional options passed to ``pq.read_table``.

        Returns:
            PyArrow Table containing the data.

        Raises:
            ParquetProviderError: If loading fails.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as e:
            raise ParquetProviderError(
                "pyarrow is required for loading Parquet files. "
                "Install it with: pip install pyarrow"
            ) from e

        try:
            fs = self._get_filesystem()
            with fs.open(path, "rb") as f:
                return pq.read_table(f, **options)
        except ParquetProviderError:
            raise
        except Exception as e:
            raise ParquetProviderError(f"Failed to load Parquet file from {path}: {e}") from e
