"""Delta Lake provider using delta-rs."""

from __future__ import annotations

import logging
from typing import Any

from nekt.exceptions import DeltaProviderError, EngineError
from nekt.provider.base import DataProvider
from nekt.types import CloudCredentials

logger = logging.getLogger(__name__)


class DeltaProvider(DataProvider):
    """Delta Lake provider using delta-rs (deltalake Python package).

    Handles reading Delta tables from S3 or other supported storage backends.
    The ``deltalake`` package is imported lazily -- it is an optional dependency.
    """

    def __init__(self, credentials: CloudCredentials | None = None) -> None:
        """Initialize the Delta provider.

        Args:
            credentials: Cloud credentials for accessing storage.
        """
        self._credentials = credentials
        self._storage_options = credentials.to_storage_options() if credentials else {}

    def _normalize_path(self, path: str) -> str:
        """Normalize S3 path for delta-rs compatibility.

        Delta-rs uses ``s3://`` protocol, not ``s3a://`` (which is Hadoop/Spark).
        """
        if path.startswith("s3a://"):
            return path.replace("s3a://", "s3://", 1)
        return path

    @property
    def name(self) -> str:
        """Provider name identifier."""
        return "delta"

    def load(self, path: str, **options: Any) -> Any:
        """Load a Delta table as a PyArrow Table.

        Args:
            path: S3 path to the Delta table (e.g. ``s3://bucket/path/to/table``).
            **options: The options ``DeltaTable.to_pyarrow_table`` accepts:
                ``partitions``, ``columns``, ``filesystem``, ``filters``.

        Returns:
            PyArrow Table containing the data.

        Raises:
            DeltaProviderError: If loading fails.
            EngineError: If the ``deltalake`` package is not installed.
        """
        try:
            from deltalake import DeltaTable
        except ImportError as e:
            raise EngineError(
                "deltalake is required for loading Delta tables on AWS. "
                "Install it with: pip install deltalake"
            ) from e

        path = self._normalize_path(path)
        try:
            dt = DeltaTable(path, storage_options=self._storage_options)
            return _read_delta_table(dt, **options)
        except Exception as e:
            raise DeltaProviderError(f"Failed to load Delta table from {path}: {e}") from e


def _read_delta_table(
    dt: Any,
    partitions: Any = None,
    columns: list[str] | None = None,
    filesystem: Any = None,
    filters: Any = None,
) -> Any:
    """``DeltaTable.to_pyarrow_table``, but scanning on the calling thread.

    ``to_pyarrow_table`` scans through a pyarrow ``PyFileSystem`` that calls back
    into delta-rs's Rust object store from pyarrow's worker threads. With
    deltalake 1.x, a process that did this dies at exit: on Linux it aborts with
    "terminate called without an active exception" (exit 134, losing any
    unflushed output), and on macOS it hangs. Reproduced on S3 and on the local
    filesystem, with pyarrow 21-23.

    Scanning with ``use_threads=False`` avoids it and costs nothing measurable:
    the storage handler already serializes reads, so a 57-file table on S3 with
    40ms latency took ~8s either way.
    """
    import pyarrow.dataset as pa_ds
    from pyarrow.parquet import filters_to_expression

    if filters is not None and not isinstance(filters, pa_ds.Expression):
        filters = filters_to_expression(filters)
    dataset = dt.to_pyarrow_dataset(partitions=partitions, filesystem=filesystem)
    return dataset.to_table(columns=columns, filter=filters, use_threads=False)
