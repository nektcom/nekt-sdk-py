"""Custom exceptions for the Nekt SDK."""

from __future__ import annotations


class NektError(Exception):
    """Base exception for all Nekt SDK errors."""

    pass


class ConfigurationError(NektError):
    """Raised when there's a configuration issue."""

    pass


class ClientNotInitializedError(NektError):
    """Raised when the client is accessed before initialization."""

    pass


class ConfigurationLockedError(NektError):
    """Raised when trying to modify configuration after client initialization."""

    pass


class APIError(NektError):
    """Raised when an API call fails."""

    def __init__(self, message: str, status_code: int | None = None, response: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class TableNotFoundError(APIError):
    """Raised when a table is not found in the API."""

    pass


class SecretNotFoundError(APIError):
    """Raised when a secret is not found."""

    pass


class VolumeNotFoundError(APIError):
    """Raised when a volume is not found."""

    pass


class AuthenticationError(APIError):
    """Raised when authentication fails."""

    pass


class SchemaEvolutionError(NektError):
    """Raised when schema evolution fails."""

    pass


class SchemaValidationError(SchemaEvolutionError):
    """Raised when schema validation fails in strict mode."""

    pass


class TypeCompatibilityError(SchemaEvolutionError):
    """Raised when types are not compatible for evolution."""

    pass


class ProviderError(NektError):
    """Base exception for storage provider errors."""

    pass


class DeltaProviderError(ProviderError):
    """Raised when Delta Lake operations fail."""

    pass


class BigQueryProviderError(ProviderError):
    """Raised when BigQuery operations fail."""

    pass


class IcebergProviderError(ProviderError):
    """Raised when Iceberg operations fail."""

    pass


class ParquetProviderError(ProviderError):
    """Raised when Parquet file operations fail."""

    pass


class EngineError(NektError):
    """Base exception for engine errors."""

    pass


class UnsupportedOperationError(EngineError):
    """Raised when an operation is not supported by the current engine."""

    pass


class DataValidationError(NektError):
    """Raised when data validation (Great Expectations) fails."""

    def __init__(self, message: str, failed_expectations: list | None = None):
        super().__init__(message)
        self.failed_expectations = failed_expectations or []


class FileUploadError(NektError):
    """Raised when file upload fails."""

    pass


class FileDownloadError(NektError):
    """Raised when file download fails."""

    pass


class MissingDependencyError(NektError):
    """Raised when a required optional dependency is not installed."""

    def __init__(
        self,
        dependency_name: str,
        extras_group: str,
        message: str | None = None,
    ):
        self.dependency_name = dependency_name
        self.extras_group = extras_group
        if message is None:
            message = (
                f"'{dependency_name}' is required but not installed. "
                f"Install it with: pip install nekt-sdk[{extras_group}]"
            )
        super().__init__(message)


class EngineNotSetError(ConfigurationError):
    """Raised when an API method is called without setting engine."""

    def __init__(self, message: str | None = None):
        if message is None:
            message = (
                "Engine not set. Set nekt.engine = 'spark' or 'python', "
                "or set the NEKT_ENGINE environment variable."
            )
        super().__init__(message)
