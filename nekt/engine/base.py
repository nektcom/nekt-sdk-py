"""Engine base class and write-stub response type."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    import pyspark.sql

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StubResponse:
    """Response returned by write-method stubs on the public engine.

    Write operations (``save_table``, ``save_dataframe``) are only available
    in the internal SDK (``nekt-sdk-internal``).  The public engine returns
    this object so callers get a clear, inspectable signal instead of an
    opaque ``NotImplementedError``.
    """

    success: bool
    message: str


class Engine(ABC):
    """Abstract base class for data-processing engines.

    Read methods are abstract -- every concrete engine **must** implement them.
    Write methods are concrete stubs that return :class:`StubResponse` and log
    a warning; the internal SDK overrides them with real implementations.
    """

    # ------------------------------------------------------------------
    # Abstract -- subclasses MUST implement
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """Engine identifier (e.g. ``"python"``, ``"spark"``)."""

    @abstractmethod
    def load_table(
        self,
        layer_name: str,
        table_name: str,
    ) -> pd.DataFrame | pyspark.sql.DataFrame:
        """Load a table as a DataFrame.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            A pandas or PySpark DataFrame depending on the engine.
        """

    @abstractmethod
    def load_secret(self, key: str) -> str:
        """Load a secret value by key.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value.
        """

    @abstractmethod
    def load_volume(
        self,
        layer_name: str,
        volume_name: str,
    ) -> list[dict[str, str]]:
        """Load volume file listings.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.

        Returns:
            List of file-path dictionaries.
        """

    @abstractmethod
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

    @abstractmethod
    def save_file(
        self,
        layer_name: str,
        volume_name: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Save a file to a volume.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume.
            description: Optional description for the file.

        Returns:
            File metadata (id, name, size, type, etc.).
        """

    # ------------------------------------------------------------------
    # Concrete stubs -- write operations require nekt-sdk-internal
    # ------------------------------------------------------------------

    def save_table(
        self,
        data: pd.DataFrame,
        layer_name: str,
        table_name: str,
        mode: str = "overwrite",
        merge_keys: list[str] | None = None,
        schema_evolution: str = "merge",
        expectations: list[Any] | None = None,
    ) -> StubResponse:
        """Save a DataFrame to storage (**stub -- requires nekt-sdk-internal**).

        Args:
            data: DataFrame to save.
            layer_name: Target layer name.
            table_name: Target table name.
            mode: Write mode (``"overwrite"``, ``"append"``, ``"merge"``).
            merge_keys: Columns for merge mode.
            schema_evolution: Schema evolution strategy.
            expectations: Data quality expectations.

        Returns:
            :class:`StubResponse` with ``success=False``.
        """
        msg = (
            "save_table requires nekt-sdk-internal. "
            "Install it with: pip install nekt-sdk-internal"
        )
        logger.warning(msg)
        return StubResponse(success=False, message=msg)

    def save_dataframe(
        self,
        df: pd.DataFrame,
        path: str,
        format: str = "parquet",
    ) -> StubResponse:
        """Save a DataFrame to cloud storage (**stub -- requires nekt-sdk-internal**).

        Args:
            df: DataFrame to save.
            path: Cloud storage path (``s3://...`` or ``gs://...``).
            format: Output format (default ``"parquet"``).

        Returns:
            :class:`StubResponse` with ``success=False``.
        """
        msg = (
            "save_dataframe requires nekt-sdk-internal. "
            "Install it with: pip install nekt-sdk-internal"
        )
        logger.warning(msg)
        return StubResponse(success=False, message=msg)
