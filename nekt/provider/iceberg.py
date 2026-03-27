"""Apache Iceberg provider using PyIceberg for AWS S3 Table Buckets."""

from __future__ import annotations

import logging
from typing import Any

from nekt.exceptions import EngineError, IcebergProviderError
from nekt.provider.base import DataProvider
from nekt.types import CloudCredentials, IcebergConfig

logger = logging.getLogger(__name__)


class IcebergProvider(DataProvider):
    """Iceberg provider using PyIceberg for AWS S3 Table Buckets.

    Handles reading Iceberg tables via the S3 Tables REST catalog.
    The ``pyiceberg`` package is imported lazily -- it is an optional dependency.
    """

    def __init__(
        self,
        credentials: CloudCredentials | None = None,
        iceberg_config: IcebergConfig | None = None,
    ) -> None:
        self._credentials = credentials
        self._iceberg_config = iceberg_config

    @staticmethod
    def _parse_region(table_bucket_arn: str) -> str:
        """Extract AWS region from an S3 Table Bucket ARN.

        Args:
            table_bucket_arn: ARN like ``arn:aws:s3tables:us-east-1:123456789012:bucket/name``.

        Returns:
            The region string (e.g. ``us-east-1``).
        """
        parts = table_bucket_arn.split(":")
        if len(parts) < 4:
            raise IcebergProviderError(f"Invalid S3 Table Bucket ARN: {table_bucket_arn}")
        return parts[3]

    def _create_catalog(self) -> Any:
        """Create a PyIceberg REST catalog configured for S3 Table Buckets.

        Returns:
            A PyIceberg catalog instance.

        Raises:
            EngineError: If pyiceberg is not installed.
            IcebergProviderError: If iceberg_config is missing.
        """
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as e:
            raise EngineError(
                "pyiceberg is required for Iceberg tables. "
                "Install it with: pip install 'pyiceberg[s3fs]'"
            ) from e

        if self._iceberg_config is None:
            raise IcebergProviderError("IcebergConfig is required to create catalog")

        region = self._parse_region(self._iceberg_config.table_bucket_arn)

        catalog_config: dict[str, str] = {
            "type": "rest",
            "uri": f"https://s3tables.{region}.amazonaws.com/iceberg",
            "warehouse": self._iceberg_config.table_bucket_arn,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": region,
            "rest.signing-name": "s3tables",
            "s3.region": region,
        }

        if self._credentials:
            if self._credentials.aws_access_key_id:
                catalog_config["s3.access-key-id"] = self._credentials.aws_access_key_id
                catalog_config["client.access-key-id"] = self._credentials.aws_access_key_id
            if self._credentials.aws_secret_access_key:
                catalog_config["s3.secret-access-key"] = self._credentials.aws_secret_access_key
                catalog_config["client.secret-access-key"] = self._credentials.aws_secret_access_key
            if self._credentials.aws_session_token:
                catalog_config["s3.session-token"] = self._credentials.aws_session_token
                catalog_config["client.session-token"] = self._credentials.aws_session_token
            if self._credentials.aws_region:
                catalog_config["client.region"] = self._credentials.aws_region

        return load_catalog("s3tables", **catalog_config)

    @property
    def name(self) -> str:
        return "iceberg"

    def load(self, path: str, **options: Any) -> Any:
        """Load an Iceberg table as a PyArrow Table.

        Args:
            path: S3 path (not used for Iceberg -- kept for interface compatibility).
            **options: Must include ``table_name`` (str).

        Returns:
            PyArrow Table containing the data.

        Raises:
            IcebergProviderError: If loading fails.
        """
        table_name = options.get("table_name")
        if not table_name:
            raise IcebergProviderError("table_name is required in options for Iceberg provider")

        if self._iceberg_config is None:
            raise IcebergProviderError("IcebergConfig is required to load Iceberg table")

        catalog = self._create_catalog()
        fqn = f"{self._iceberg_config.namespace}.{table_name}"

        try:
            logger.info("Loading Iceberg table: %s", fqn)
            iceberg_table = catalog.load_table(fqn)
            return iceberg_table.scan().to_arrow()
        except Exception as e:
            raise IcebergProviderError(f"Failed to load Iceberg table {fqn}: {e}") from e
