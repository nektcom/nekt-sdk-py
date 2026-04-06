"""BigQuery provider using google-cloud-bigquery."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from nekt.exceptions import BigQueryProviderError
from nekt.provider.base import DataProvider
from nekt.types import CloudCredentials

if TYPE_CHECKING:
    from google.cloud import bigquery

logger = logging.getLogger(__name__)


# Mapping from BigQuery types to PyArrow types.
# pyarrow is acceptable as a module-level import here because
# google-cloud-bigquery (required for this provider) has pyarrow
# as a transitive dependency.
BQ_TO_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "STRING": pa.string(),
    "BYTES": pa.binary(),
    "INTEGER": pa.int64(),
    "INT64": pa.int64(),
    "FLOAT": pa.float64(),
    "FLOAT64": pa.float64(),
    "NUMERIC": pa.decimal128(38, 9),
    "BIGNUMERIC": pa.decimal256(76, 38),
    "BOOLEAN": pa.bool_(),
    "BOOL": pa.bool_(),
    "TIMESTAMP": pa.timestamp("us", tz="UTC"),
    "DATE": pa.date32(),
    "TIME": pa.time64("us"),
    "DATETIME": pa.timestamp("us"),
    "GEOGRAPHY": pa.string(),
    "JSON": pa.string(),
}


class BigQueryProvider(DataProvider):
    """BigQuery provider using google-cloud-bigquery.

    Handles reading BigQuery tables.  The ``google.cloud.bigquery`` package
    is imported lazily -- it is an optional dependency.
    """

    def __init__(
        self,
        credentials: CloudCredentials | None = None,
        project: str | None = None,
    ) -> None:
        """Initialize the BigQuery provider.

        Args:
            credentials: Cloud credentials for accessing BigQuery.
            project: Default GCP project ID.
        """
        self._credentials = credentials
        self._project = project

    def _bq_field_to_arrow(self, field: bigquery.SchemaField) -> pa.Field:
        """Convert a BigQuery SchemaField to a PyArrow Field.

        Handles RECORD/STRUCT types recursively, REPEATED (array) fields,
        and nullable vs required modes.
        """
        if field.field_type in ("RECORD", "STRUCT"):
            nested_fields = [self._bq_field_to_arrow(f) for f in field.fields]
            arrow_type: pa.DataType = pa.struct(nested_fields)
        elif field.field_type in BQ_TO_ARROW_TYPE_MAP:
            arrow_type = BQ_TO_ARROW_TYPE_MAP[field.field_type]
        else:
            logger.warning("Unknown BigQuery type %s, defaulting to string", field.field_type)
            arrow_type = pa.string()

        # Handle repeated (array) fields
        if field.mode == "REPEATED":
            arrow_type = pa.list_(arrow_type)

        # Handle nullable
        nullable = field.mode != "REQUIRED"

        return pa.field(field.name, arrow_type, nullable=nullable)

    @property
    def name(self) -> str:
        """Provider name identifier."""
        return "bigquery"

    def load(self, path: str, **options: Any) -> Any:
        """Load a BigQuery table as a PyArrow Table.

        Args:
            path: Table reference (``project.dataset.table`` or ``dataset.table``).
            **options: Additional options.  Supports ``columns`` (list of column
                names) and ``where`` (SQL WHERE clause without the keyword).

        Returns:
            PyArrow Table containing the data.

        Raises:
            BigQueryProviderError: If loading fails or the library is missing.
        """
        try:
            from google.cloud import bigquery as bq
        except ImportError as e:
            raise BigQueryProviderError(
                "google-cloud-bigquery is required for loading BigQuery tables on GCP. "
                "Install it with: pip install google-cloud-bigquery"
            ) from e

        try:
            # Build credentials for client
            gcp_creds = None
            project = self._project
            if self._credentials:
                if self._credentials.gcp_access_token:
                    from google.oauth2.credentials import Credentials

                    gcp_creds = Credentials(token=self._credentials.gcp_access_token)
                if self._credentials.gcp_project_id:
                    project = self._credentials.gcp_project_id

            client = bq.Client(project=project, credentials=gcp_creds)

            # Build query
            columns = options.get("columns")
            if columns:
                select_clause = ", ".join(f"`{c}`" for c in columns)
            else:
                select_clause = "*"

            query = f"SELECT {select_clause} FROM `{path}`"

            where_clause = options.get("where")
            if where_clause:
                query += f" WHERE {where_clause}"

            logger.info("Executing BigQuery query: %s", query)

            result = client.query(query).to_arrow(create_bqstorage_client=False)
            logger.info("Loaded %d rows from %s", result.num_rows, path)
            return result

        except BigQueryProviderError:
            raise
        except Exception as e:
            raise BigQueryProviderError(f"Failed to load from BigQuery table {path}: {e}") from e
