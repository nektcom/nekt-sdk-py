"""Spark engine with read-only operations."""

from __future__ import annotations

import logging
import mimetypes
import os
from typing import TYPE_CHECKING, Any

import requests

from nekt.engine.base import Engine
from nekt.exceptions import EngineError, FileUploadError
from nekt.types import CloudCredentials, CloudProvider, Environment, TableFormat

if TYPE_CHECKING:
    import pyspark.sql
    from delta.tables import DeltaTable as _DeltaTable

    from nekt.api import NektAPI
    from nekt.provider.base import DataProvider

logger = logging.getLogger("nekt.engine.spark")


class SparkEngine(Engine):
    """Spark engine using PySpark and Delta Lake.

    Provides read-only access to tables (as Spark DataFrames), secrets,
    and volumes.  Write operations (``save_table``, ``save_dataframe``)
    are stubs inherited from :class:`Engine` -- install ``nekt-sdk-internal``
    for full write support.

    This engine delegates storage loading to a :class:`DataProvider`
    instance selected based on the cloud provider:

    - **AWS** -- :class:`SparkDeltaProvider` (delta-spark)
    - **GCP** -- :class:`SparkBigQueryProvider` (spark-bigquery connector)

    The provider is created lazily (on first use) because it requires
    the SparkSession, which is itself lazy-initialized.
    """

    def __init__(
        self,
        api: NektAPI,
        provider: CloudProvider,
        credentials: CloudCredentials | None = None,
        environment: Environment = Environment.LOCAL,
    ) -> None:
        """Initialize the Spark engine.

        No ``spark`` parameter is required -- the SparkSession is
        auto-discovered lazily on first access via :attr:`spark`.

        Args:
            api: NektAPI client for interacting with the Nekt platform.
            provider: Cloud provider (AWS or GCP).
            credentials: Cloud credentials for storage access.
            environment: Execution environment.
        """
        self._api = api
        self._cloud_provider = provider
        self._provider = provider  # backward compat for internal SDK subclass
        self._credentials = credentials
        self._environment = environment
        self._spark: pyspark.sql.SparkSession | None = None
        self._data_provider: DataProvider | None = None

    # ------------------------------------------------------------------
    # Provider delegation
    # ------------------------------------------------------------------

    def _get_data_provider(self) -> DataProvider:
        """Get or create the data provider, lazily initialized.

        The provider is created on first call because it requires the
        SparkSession, which itself is lazy.

        Returns:
            The appropriate DataProvider for the configured cloud provider.

        Raises:
            EngineError: If the provider is not supported.
        """
        if self._data_provider is not None:
            return self._data_provider

        if self._cloud_provider == CloudProvider.AWS:
            from nekt.provider.spark_delta import SparkDeltaProvider

            self._data_provider = SparkDeltaProvider(spark=self.spark)
        elif self._cloud_provider == CloudProvider.GCP:
            from nekt.provider.spark_bigquery import SparkBigQueryProvider

            self._data_provider = SparkBigQueryProvider(spark=self.spark, credentials=self._credentials)
        else:
            raise EngineError(f"Unsupported provider: {self._cloud_provider}")

        return self._data_provider

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Engine identifier."""
        return "spark"

    @property
    def spark(self) -> pyspark.sql.SparkSession:
        """Lazy-initialized SparkSession.

        On first access, tries ``SparkSession.getActiveSession()`` first,
        then falls back to ``SparkSession.builder.getOrCreate()``.  The
        result is cached for subsequent access.

        Returns:
            The active or newly-created SparkSession.

        Raises:
            EngineError: If no SparkSession can be obtained.
        """
        if self._spark is not None:
            return self._spark

        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise EngineError(
                "pyspark is required for the Spark engine. "
                "Install it with: pip install pyspark"
            ) from e

        # Try active session first
        session = SparkSession.getActiveSession()
        if session is not None:
            self._spark = session
            self._configure_credentials()
            return self._spark

        # Fall back to builder with Delta Lake and cloud storage packages
        logger.info("No active SparkSession found — creating one with Delta Lake support")

        packages = ["io.delta:delta-spark_2.12:3.3.0"]
        if self._cloud_provider == CloudProvider.AWS:
            packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        elif self._cloud_provider == CloudProvider.GCP:
            packages.append(
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.43.1"
            )

        try:
            session = (
                SparkSession.builder  # type: ignore[union-attr]
                .appName("nekt")
                .master("local[*]")
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.jars.packages", ",".join(packages))
                .getOrCreate()
            )
        except Exception as exc:
            raise EngineError(
                "No active SparkSession found and could not create one"
            ) from exc

        if session is None:
            raise EngineError(
                "No active SparkSession found and could not create one"
            )

        self._spark = session
        self._configure_credentials()
        return session

    def _configure_credentials(self) -> None:
        """Set cloud credentials on the Hadoop configuration.

        For AWS, configures the S3A filesystem with temporary credentials
        obtained from the Nekt API.  For GCP, credentials are handled by
        the BigQuery provider at query time.
        """
        if (
            self._spark is None
            or self._credentials is None
            or self._environment != Environment.LOCAL
        ):
            return

        if self._cloud_provider == CloudProvider.AWS and self._credentials.aws_access_key_id:
            hadoop_conf = self._spark._jsc.hadoopConfiguration()
            hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            hadoop_conf.set("fs.s3a.access.key", self._credentials.aws_access_key_id)
            hadoop_conf.set("fs.s3a.secret.key", self._credentials.aws_secret_access_key or "")
            hadoop_conf.set("fs.s3a.session.token", self._credentials.aws_session_token or "")
            hadoop_conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            logger.info("AWS credentials configured on Hadoop S3A filesystem")

    # ------------------------------------------------------------------
    # Read implementations
    # ------------------------------------------------------------------

    def load_table(self, layer_name: str, table_name: str) -> pyspark.sql.DataFrame:
        """Load a table as a Spark DataFrame.

        Delegates to :meth:`load_spark_dataframe`.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            Spark DataFrame containing the table data.
        """
        return self.load_spark_dataframe(layer_name, table_name)

    def load_spark_dataframe(self, layer_name: str, table_name: str) -> pyspark.sql.DataFrame:
        """Load a table as a Spark DataFrame.

        Extra method beyond the base :class:`Engine` contract.

        For AWS: loads via :meth:`load_delta_table` and converts to
        DataFrame.
        For GCP: loads via the BigQuery Spark connector.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            Spark DataFrame.

        Raises:
            EngineError: If the provider is not supported.
        """
        logger.info("[%s/%s] Loading table as Spark DataFrame", layer_name, table_name)

        if self._cloud_provider == CloudProvider.AWS:
            # Check if this is an Iceberg table
            table_config = self._api.get_table_details(
                layer_name=layer_name,
                table_name=table_name,
                provider=self._cloud_provider,
                include_delta_fields=True,
                include_layer_database_name=True,
                use_s3a=self._environment == Environment.LOCAL,
            )
            if table_config.table_format == TableFormat.ICEBERG:
                return self._load_iceberg_table(table_config)

            delta_table = self.load_delta_table(layer_name, table_name)
            return delta_table.toDF()
        elif self._cloud_provider == CloudProvider.GCP:
            table_details = self._api.get_table_details_raw(
                layer_name, table_name, params={"include_layer_database_name": "true"}
            )
            table_reference = f"{table_details['layer_database_name']}.{table_name}"
            logger.info("[%s/%s] Loading from BigQuery: %s", layer_name, table_name, table_reference)
            provider = self._get_data_provider()
            return provider.load(table_reference)
        else:
            raise EngineError(f"Unsupported provider: {self._cloud_provider}")

    def load_delta_table(self, layer_name: str, table_name: str) -> _DeltaTable:
        """Load a Delta table object.

        Extra method beyond the base :class:`Engine` contract.
        Only supported for AWS provider.

        Args:
            layer_name: Name of the layer.
            table_name: Name of the table.

        Returns:
            DeltaTable object.

        Raises:
            EngineError: If the provider is not AWS or delta-spark is
                not installed.
        """
        if self._cloud_provider != CloudProvider.AWS:
            raise EngineError("Delta tables are only supported for AWS provider")

        # Get table path from raw API (need s3_path field)
        params: dict[str, str] = {}
        if self._environment != Environment.PRODUCTION:
            params["use_s3a"] = "true"

        table_details = self._api.get_table_details_raw(layer_name, table_name, params)
        s3_path = table_details.get("s3_path")

        if not s3_path:
            raise EngineError(f"No S3 path returned for table {layer_name}/{table_name}")

        logger.info("[%s/%s] Loading Delta table from %s", layer_name, table_name, s3_path)
        provider = self._get_data_provider()
        return provider.load(s3_path)

    def _load_iceberg_table(self, table_config: Any) -> pyspark.sql.DataFrame:
        """Load an Iceberg table as a Spark DataFrame.

        Uses the ``catalog_alias`` from the backend API (e.g. ``s3tb_001``)
        which matches the Spark catalog configured in EMR spark-submit parameters.

        Args:
            table_config: TableConfig with iceberg_config populated.

        Returns:
            Spark DataFrame.

        Raises:
            EngineError: If iceberg_config is missing or catalog_alias is empty.
        """
        if table_config.iceberg_config is None:
            raise EngineError(
                f"Iceberg config missing for table {table_config.layer_name}/{table_config.table_name}"
            )

        iceberg = table_config.iceberg_config
        if not iceberg.catalog_alias:
            raise EngineError(
                f"Iceberg catalog_alias missing for table {table_config.layer_name}/{table_config.table_name}"
            )

        fqn = f"{iceberg.catalog_alias}.{iceberg.namespace}.{table_config.table_name}"

        logger.info(
            "[%s/%s] Loading Iceberg table: %s",
            table_config.layer_name,
            table_config.table_name,
            fqn,
        )
        return self.spark.table(fqn)

    def load_secret(self, key: str) -> str:
        """Load a secret value by key.

        Args:
            key: The secret key to retrieve.

        Returns:
            The secret value.
        """
        return self._api.load_secret(key)

    def load_volume(self, layer_name: str, volume_name: str) -> list[dict[str, str]]:
        """Load volume file listings.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.

        Returns:
            List of file-path dictionaries.
        """
        return self._api.load_volume(layer_name, volume_name)

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
        logger.info("[%s/%s] Creating volume", layer_name, volume_name)
        return self._api.create_volume(
            layer_name=layer_name,
            volume_name=volume_name,
            description=description,
        )

    def save_file(
        self,
        layer_name: str,
        volume_name: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Save a file to a volume using multipart upload.

        Performs a 3-step upload process:
        1. Create volume file entry and get presigned upload URLs.
        2. Upload file in 100 MB chunks to presigned URLs.
        3. Complete the multipart upload with ETags.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume.
            description: Optional description for the file.

        Returns:
            File metadata (id, name, file_size, file_type, description).

        Raises:
            FileUploadError: If the file is not found, upload fails,
                or completion fails.
        """
        # Validate file exists
        if not os.path.exists(file_path):
            raise FileUploadError(f"File not found: {file_path}")

        # Compute metadata
        file_size = os.path.getsize(file_path)
        name = file_name or os.path.basename(file_path)
        file_type, _ = mimetypes.guess_type(file_path)
        file_type = file_type or "application/octet-stream"

        logger.info("[%s/%s] Saving file %s (%d bytes)", layer_name, volume_name, name, file_size)

        # Step 1: Create volume file and get presigned URLs
        try:
            response = self._api.create_volume_file(
                layer_name=layer_name,
                volume_name=volume_name,
                name=name,
                file_size=file_size,
                file_type=file_type,
                description=description,
            )
        except Exception as e:
            raise FileUploadError(f"Failed to create volume file: {e}") from e

        file_id: str = response["id"]
        presigned_urls: list[dict[str, Any]] = response.get("presigned_url_list", [])

        if not presigned_urls:
            raise FileUploadError("No presigned URLs returned from API")

        # Step 2: Upload in 100 MB chunks
        part_size = 100 * 1024 * 1024  # 100 MB
        parts: list[dict[str, Any]] = []

        try:
            with open(file_path, "rb") as f:
                for url_info in presigned_urls:
                    part_number = url_info.get("part_number", len(parts) + 1)
                    presigned_url: str = url_info.get("presigned_url", "")

                    chunk = f.read(part_size)
                    if not chunk:
                        break

                    logger.debug("Uploading part %d (%d bytes)", part_number, len(chunk))

                    upload_response = requests.put(presigned_url, data=chunk)
                    upload_response.raise_for_status()

                    etag = upload_response.headers.get("ETag", "").strip('"')
                    parts.append({"etag": etag, "part_number": part_number})

        except requests.RequestException as e:
            raise FileUploadError(f"Failed to upload file part: {e}") from e
        except OSError as e:
            raise FileUploadError(f"Failed to read file: {e}") from e

        # Step 3: Complete multipart upload
        try:
            self._api.complete_volume_file_upload(
                layer_name=layer_name,
                volume_name=volume_name,
                file_id=file_id,
                parts=parts,
            )
        except Exception as e:
            raise FileUploadError(f"Failed to complete upload: {e}") from e

        logger.info("[%s/%s] File %s saved", layer_name, volume_name, name)

        return {
            "id": file_id,
            "name": name,
            "file_size": file_size,
            "file_type": file_type,
            "description": description,
        }
