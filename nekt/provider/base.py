"""Abstract base class for storage providers.

Providers are engine-internal implementation details, not user-facing.
They handle the low-level loading of data from specific storage backends
(Delta Lake, BigQuery, Parquet on S3/GCS).  Users interact with engines
(e.g. ``PythonEngine.load_table``), which delegate to the appropriate
provider behind the scenes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DataProvider(ABC):
    """Abstract interface for read-only storage providers.

    Concrete providers must implement :pyattr:`name` and :pymeth:`load`.
    Write operations (save, merge, get_schema, exists) live exclusively
    in the internal SDK's provider hierarchy.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name identifier (e.g. ``"delta"``, ``"bigquery"``)."""

    @abstractmethod
    def load(self, path: str, **options: Any) -> Any:
        """Load data from storage.

        Args:
            path: Storage path (S3 path, BigQuery table reference, etc.).
            **options: Provider-specific options.

        Returns:
            Loaded data (typically a PyArrow Table, but the return type
            is ``Any`` so concrete providers can narrow as needed).
        """
