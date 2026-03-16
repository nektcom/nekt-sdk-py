"""Spark BigQuery provider using the Spark BigQuery connector."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nekt.exceptions import BigQueryProviderError
from nekt.provider.base import DataProvider
from nekt.types import CloudCredentials

if TYPE_CHECKING:
    import pyspark.sql

logger = logging.getLogger(__name__)


class SparkBigQueryProvider(DataProvider):
    """Spark-based BigQuery provider using the Spark BQ connector.

    Loads BigQuery tables as Spark DataFrames via the ``spark-bigquery``
    connector (``com.google.cloud.spark:spark-bigquery``).  Unlike
    :class:`BigQueryProvider` (which uses ``google-cloud-bigquery`` and
    returns PyArrow Tables), this provider works within a Spark context
    and returns a Spark DataFrame.

    Credentials are optional -- only needed in ``LOCAL`` mode where Spark
    does not have ambient GCP credentials.
    """

    def __init__(
        self,
        spark: pyspark.sql.SparkSession,
        credentials: CloudCredentials | None = None,
    ) -> None:
        """Initialize the Spark BigQuery provider.

        Args:
            spark: Active SparkSession instance.
            credentials: Cloud credentials for BigQuery access (needed
                for LOCAL mode to set ``gcpAccessToken``).
        """
        self._spark = spark
        self._credentials = credentials

    @property
    def name(self) -> str:
        """Provider name identifier."""
        return "spark-bigquery"

    def load(self, path: str, **options: Any) -> Any:
        """Load a BigQuery table as a Spark DataFrame.

        Args:
            path: BigQuery table reference
                (e.g. ``project.dataset.table`` or ``dataset.table``).
            **options: Additional options (currently unused).

        Returns:
            Spark DataFrame containing the table data.

        Raises:
            BigQueryProviderError: If loading the table fails.
        """
        try:
            reader = (
                self._spark.read.format("bigquery")
                .option("table", path)
                .option("cacheExpirationTimeInMinutes", "0")
            )

            if self._credentials and self._credentials.gcp_access_token:
                reader = reader.option("gcpAccessToken", self._credentials.gcp_access_token)
                reader = reader.option("parentProject", self._credentials.gcp_project_id)

            return reader.load()
        except Exception as e:
            raise BigQueryProviderError(f"Failed to load BigQuery table {path}: {e}") from e
