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
            **options: ``views_enabled`` and ``materialization_dataset`` for
                a reference that is a BigQuery **view** — which is what a Nekt
                external table is. The connector refuses a view unless views
                are enabled, and with them it materializes the view into a
                real table first, so it needs a dataset to write into. Both
                come from the backend's table details; neither is
                discoverable here.

        Returns:
            Spark DataFrame containing the table data.

        Raises:
            BigQueryProviderError: If loading the table fails, or if views
                are enabled without a materialization dataset.
        """
        import os

        views_enabled = bool(options.get("views_enabled"))
        materialization_dataset = options.get("materialization_dataset") or ""

        if views_enabled and not materialization_dataset:
            # The connector's own error for this names a dataset the user has
            # never heard of and does not mention views. Refusing before the
            # job starts says what is actually wrong.
            raise BigQueryProviderError(
                f"Cannot read view {path}: reading a view requires a materialization dataset, and none was given."
            )

        try:
            reader = (
                self._spark.read.format("bigquery")
                .option("table", path)
                .option("cacheExpirationTimeInMinutes", "0")
            )

            if views_enabled:
                # The materialization dataset MUST be co-located with the
                # view. For a Nekt external table the view lives at the layer
                # address, so the backend sends a dataset in the lakehouse's
                # own location.
                reader = reader.option("viewsEnabled", "true").option(
                    "materializationDataset", materialization_dataset
                )

            # Prefer SA key file for auth (long-lived, works in CI).
            # Fall back to access token for backward compatibility.
            sa_key_file = os.environ.get("GCP_SA_KEY_FILE")
            if sa_key_file:
                reader = reader.option("credentialsFile", sa_key_file)
            elif self._credentials and self._credentials.gcp_access_token:
                reader = reader.option("gcpAccessToken", self._credentials.gcp_access_token)

            if self._credentials and self._credentials.gcp_project_id:
                reader = reader.option("parentProject", self._credentials.gcp_project_id)

            return reader.load()
        except Exception as e:
            raise BigQueryProviderError(f"Failed to load BigQuery table {path}: {e}") from e
