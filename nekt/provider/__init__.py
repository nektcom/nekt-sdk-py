"""Storage providers for the Nekt SDK."""

from nekt.provider.base import DataProvider

# DeltaProvider is optional (requires deltalake)
try:
    from nekt.provider.delta import DeltaProvider
except ImportError:
    DeltaProvider = None  # type: ignore[assignment,misc]

# BigQueryProvider is optional (requires google-cloud-bigquery, pyarrow)
try:
    from nekt.provider.bigquery import BigQueryProvider
except ImportError:
    BigQueryProvider = None  # type: ignore[assignment,misc]

# IcebergProvider is optional (requires pyiceberg)
try:
    from nekt.provider.iceberg import IcebergProvider
except ImportError:
    IcebergProvider = None  # type: ignore[assignment,misc]

# Parquet providers are optional (require s3fs/gcsfs)
try:
    from nekt.provider.parquet import S3ParquetProvider
except ImportError:
    S3ParquetProvider = None  # type: ignore[assignment,misc]

try:
    from nekt.provider.parquet import GCSParquetProvider
except ImportError:
    GCSParquetProvider = None  # type: ignore[assignment,misc]

# Spark-based providers are optional (require pyspark, delta-spark / spark-bigquery)
try:
    from nekt.provider.spark_delta import SparkDeltaProvider
except ImportError:
    SparkDeltaProvider = None  # type: ignore[assignment,misc]

try:
    from nekt.provider.spark_bigquery import SparkBigQueryProvider
except ImportError:
    SparkBigQueryProvider = None  # type: ignore[assignment,misc]

__all__ = [
    "DataProvider",
    "DeltaProvider",
    "BigQueryProvider",
    "IcebergProvider",
    "S3ParquetProvider",
    "GCSParquetProvider",
    "SparkDeltaProvider",
    "SparkBigQueryProvider",
]
