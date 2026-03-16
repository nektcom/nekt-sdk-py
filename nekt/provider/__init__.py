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

# Parquet providers are optional (require s3fs/gcsfs)
try:
    from nekt.provider.parquet import S3ParquetProvider
except ImportError:
    S3ParquetProvider = None  # type: ignore[assignment,misc]

try:
    from nekt.provider.parquet import GCSParquetProvider
except ImportError:
    GCSParquetProvider = None  # type: ignore[assignment,misc]

__all__ = [
    "DataProvider",
    "DeltaProvider",
    "BigQueryProvider",
    "S3ParquetProvider",
    "GCSParquetProvider",
]
