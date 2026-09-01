"""Engine base class for data-processing operations."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    import pyspark.sql

    from collections.abc import Iterator
    from typing import Any

    from nekt.api import NektAPI

logger = logging.getLogger(__name__)


class Engine(ABC):
    """Abstract base class for data-processing engines.

    Read methods are abstract -- every concrete engine **must** implement them.
    Write methods are concrete stubs that return ``None`` and log a warning;
    the internal SDK overrides them with real implementations.
    """

    # Set by every concrete engine in its ``__init__``; used by the
    # engine-independent download helpers below.
    _api: NektAPI

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
    # Concrete read methods -- engine-independent (delegate to the API)
    # ------------------------------------------------------------------

    def get_file_download_url(
        self,
        layer_name: str,
        volume_name: str,
        file_name: str,
    ) -> str:
        """Get a presigned download URL for a file (by layer + volume + file).

        ``layer_name``, ``volume_name``, and ``file_name`` may each be a name,
        slug, or id (the file segment accepts a name or id).

        Args:
            layer_name: Layer name, slug, or id.
            volume_name: Volume name, slug, or id.
            file_name: File name or id.

        Returns:
            A presigned download URL.
        """
        return self._api.get_file_download_url(
            layer_name=layer_name,
            volume_name=volume_name,
            file_name=file_name,
        )

    def get_file_download_url_by_volume_id(
        self,
        volume_identifier: str,
        file_name: str,
    ) -> str:
        """Get a presigned download URL by volume id/slug (layer inferred).

        Args:
            volume_identifier: Volume id or slug.
            file_name: File name or id.

        Returns:
            A presigned download URL.
        """
        return self._api.get_file_download_url_by_volume_id(
            volume_identifier=volume_identifier,
            file_name=file_name,
        )

    def get_file_download_url_by_file_id(self, file_id: str) -> str:
        """Get a presigned download URL by file id alone.

        Args:
            file_id: Id of the file.

        Returns:
            A presigned download URL.
        """
        return self._api.get_file_download_url_by_file_id(file_id=file_id)

    def iter_volume_files(
        self,
        volume_identifier: str,
        *,
        updated_since: str | None = None,
        page_size: int = 100,
    ) -> "Iterator[dict[str, Any]]":
        """Yield every file in a volume, one page at a time.

        Results arrive ordered by ``updated_at`` ascending, so ``updated_since``
        works as an incremental bookmark. Filtering happens server-side, so a
        run that finds nothing new costs a single request.

        Args:
            volume_identifier: Volume id or slug.
            updated_since: ISO-8601 timestamp; only files modified at or after it.
            page_size: Files per request (the API caps this at 100).

        Yields:
            One dict per file: ``id``, ``name``, ``description``, ``file_size``,
            ``file_type``, ``created_at``, ``updated_at``.
        """
        return self._api.iter_volume_files(
            volume_identifier=volume_identifier,
            updated_since=updated_since,
            page_size=page_size,
        )

    def get_download_url(
        self,
        *,
        file_id: str | None = None,
        volume: str | None = None,
        layer: str | None = None,
        file_name: str | None = None,
    ) -> str:
        """Get a presigned download URL from whichever identifiers you have.

        Args:
            file_id: Id of the file (takes precedence when given).
            volume: Volume id or slug (a name, when ``layer`` is also given).
            layer: Layer name, slug, or id.
            file_name: File name or id, resolved within the volume.

        Returns:
            A presigned download URL.
        """
        return self._api.get_download_url(
            file_id=file_id,
            volume=volume,
            layer=layer,
            file_name=file_name,
        )

    def download_file(
        self,
        destination: str,
        *,
        file_id: str | None = None,
        volume: str | None = None,
        layer: str | None = None,
        file_name: str | None = None,
    ) -> str:
        """Download a volume file's contents to a local path.

        Args:
            destination: Local file path to write to.
            file_id: Id of the file (takes precedence when given).
            volume: Volume id or slug (a name, when ``layer`` is also given).
            layer: Layer name, slug, or id.
            file_name: File name or id, resolved within the volume.

        Returns:
            The path the file was written to.
        """
        return self._api.download_file(
            destination,
            file_id=file_id,
            volume=volume,
            layer=layer,
            file_name=file_name,
        )

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
    ) -> None:
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
            None. Install nekt-sdk-internal for write support.
        """
        print("save_table is only available when running on Nekt.")
        return None

    def save_dataframe(
        self,
        df: pd.DataFrame,
        path: str,
        format: str = "parquet",
    ) -> None:
        """Save a DataFrame to cloud storage (**stub -- only available on Nekt**).

        Args:
            df: DataFrame to save.
            path: Cloud storage path (``s3://...`` or ``gs://...``).
            format: Output format (default ``"parquet"``).

        Returns:
            None. Only available when running on Nekt.
        """
        print("save_dataframe is only available when running on Nekt.")
        return None
