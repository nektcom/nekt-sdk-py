"""BigQuery provider using google-cloud-bigquery."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import pyarrow as pa

from nekt.exceptions import BigQueryProviderError
from nekt.http import DEFAULT_TIMEOUT, call_with_deadline, deadline_for
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


# (connect, read) timeout for every BigQuery REST call. google-cloud-bigquery's
# own default is ``None``: ``Client._call_api`` hands it unchanged down to
# ``requests``, so a socket severed under the call blocks forever and the
# library's retry never fires -- it only retries a *raised* Timeout. On
# 2026-11-03 a Cloud Run maintenance migration did exactly that to 11 runs,
# which sat "running" for 10h+ inside a single ``query().to_arrow()`` or
# ``get_table()`` (NEKT-6284).
BIGQUERY_TIMEOUT = DEFAULT_TIMEOUT

_session_class: type | None = None


def _timeout_session_class() -> type:
    """Return (building it once) the ``AuthorizedSession`` subclass that enforces timeouts.

    Built lazily because google-auth, like google-cloud-bigquery, is an optional
    dependency of the SDK.
    """
    global _session_class
    if _session_class is not None:
        return _session_class

    from google.auth.transport.requests import AuthorizedSession

    class TimeoutAuthorizedSession(AuthorizedSession):
        """``AuthorizedSession`` in which no request can block indefinitely.

        google-cloud-bigquery sends ``timeout=None`` for every call made without
        an explicit timeout. This substitutes the session default and runs the
        request under the wall-clock deadline from :mod:`nekt.http`, which also
        covers a resolver stall. A timeout the caller does set is honoured as
        is. ``DeadlineExceeded`` and the socket's own ``Timeout`` are both in
        the library's retryable set, so the call is retried on a fresh
        connection instead of hanging the run.
        """

        def __init__(self, credentials: Any, timeout: Any = BIGQUERY_TIMEOUT, **kwargs: Any) -> None:
            super().__init__(credentials, **kwargs)
            self._default_timeout = timeout

        def request(  # type: ignore[override]
            self, method: Any, url: Any, *args: Any, timeout: Any = None, **kwargs: Any
        ) -> Any:
            if timeout is None:
                timeout = self._default_timeout
            method_name = method.decode() if isinstance(method, bytes) else method
            url_text = url.decode() if isinstance(url, bytes) else url
            return call_with_deadline(
                super().request,
                method,
                url,
                *args,
                deadline=deadline_for(timeout),
                description=f"{method_name.upper()} {urlsplit(url_text).netloc}",
                timeout=timeout,
                **kwargs,
            )

    _session_class = TimeoutAuthorizedSession
    return _session_class


def build_bigquery_client(
    project: str | None = None,
    credentials: Any = None,
    timeout: Any = BIGQUERY_TIMEOUT,
) -> bigquery.Client:
    """Return a ``bigquery.Client`` whose every HTTP call carries a timeout.

    Args:
        project: Default project for jobs and unqualified table references.
            Falls back to the credentials' project when they carry one.
        credentials: Any ``google.auth`` credentials object. ``None`` means
            Application Default Credentials, as ``bigquery.Client()`` would use.
        timeout: ``requests`` timeout applied to calls made without one.

    Returns:
        A client identical to ``bigquery.Client(project, credentials)`` except
        for its HTTP session (see ``TimeoutAuthorizedSession``).
    """
    import google.auth
    from google.auth.credentials import with_scopes_if_required
    from google.cloud import bigquery as bq

    if credentials is None:
        credentials, default_project = google.auth.default(scopes=bq.Client.SCOPE)
        project = project or default_project
    # bigquery.Client scopes the credentials it is given; the session we hand it
    # must refresh with the same scopes, so scope them up front.
    credentials = with_scopes_if_required(credentials, bq.Client.SCOPE)
    session = _timeout_session_class()(credentials, timeout=timeout)
    return bq.Client(project=project, credentials=credentials, _http=session)


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
            import google.cloud.bigquery  # noqa: F401 - only checks the optional dependency is installed
        except ImportError as e:
            raise BigQueryProviderError(
                "google-cloud-bigquery is required for loading BigQuery tables on GCP. Install it with: pip install google-cloud-bigquery"
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

            client = build_bigquery_client(project, gcp_creds)

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
