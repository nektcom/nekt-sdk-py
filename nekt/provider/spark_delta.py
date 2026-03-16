"""Spark Delta Lake provider using delta-spark."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nekt.exceptions import DeltaProviderError, EngineError
from nekt.provider.base import DataProvider

if TYPE_CHECKING:
    import pyspark.sql

logger = logging.getLogger(__name__)


class SparkDeltaProvider(DataProvider):
    """Spark-based Delta Lake provider using delta-spark.

    Loads Delta tables as DeltaTable objects via the Spark Delta Lake
    connector.  Unlike :class:`DeltaProvider` (which uses delta-rs and
    returns PyArrow Tables), this provider works within a Spark context
    and returns a ``DeltaTable`` -- call ``.toDF()`` to convert to a
    Spark DataFrame.

    The ``delta-spark`` package is imported lazily -- it is an optional
    dependency.
    """

    def __init__(self, spark: pyspark.sql.SparkSession) -> None:
        """Initialize the Spark Delta provider.

        No credentials parameter is needed -- Spark handles credentials
        via its own Hadoop configuration.

        Args:
            spark: Active SparkSession instance.
        """
        self._spark = spark

    @property
    def name(self) -> str:
        """Provider name identifier."""
        return "spark-delta"

    def load(self, path: str, **options: Any) -> Any:
        """Load a Delta table as a DeltaTable object.

        Args:
            path: S3 path to the Delta table (e.g. ``s3a://bucket/path``).
            **options: Additional options (currently unused).

        Returns:
            DeltaTable object.  Call ``.toDF()`` to get a Spark DataFrame.

        Raises:
            EngineError: If ``delta-spark`` is not installed.
            DeltaProviderError: If loading the table fails.
        """
        try:
            from delta.tables import DeltaTable
        except ImportError as e:
            raise EngineError(
                "delta-spark is required for loading Delta tables on AWS. "
                "Install it with: pip install delta-spark"
            ) from e

        try:
            return DeltaTable.forPath(self._spark, path)
        except Exception as e:
            raise DeltaProviderError(f"Failed to load Delta table from {path}: {e}") from e
