import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from nekt.sdk.transformation import TransformationClient

# Global variables to store the data access token, URL, and client instance (all prefixed with _nekt_)
_nekt_data_access_token: Optional[str] = None
_nekt_api_url: str = "https://api.nekt.ai"
_nekt_client: Optional[TransformationClient] = None


def _get_client() -> TransformationClient:
    """
    Get the global client instance, creating it if necessary.
    """
    global _nekt_client, _nekt_data_access_token, _nekt_api_url

    if _nekt_data_access_token is None:
        raise ValueError(
            "data_access_token must be set. Set nekt.data_access_token = 'your_token' before using the SDK."
        )

    if _nekt_client is None:
        _nekt_client = TransformationClient(data_access_token=_nekt_data_access_token, api_url=_nekt_api_url)

    return _nekt_client


def _reset_client():
    """
    Reset the global client instance. This will force a new client to be created
    on the next method call, ensuring the new data_access_token is used.
    """
    global _nekt_client
    if _nekt_client is not None:
        # Clean up the existing Spark session if needed
        if hasattr(_nekt_client, "_spark") and _nekt_client._spark is not None:
            _nekt_client._spark.stop()
    _nekt_client = None


def set_data_access_token(token: str):
    """
    Set the data access token and reset the client to use the new token.
    """
    global _nekt_data_access_token
    if _nekt_data_access_token != token:
        _nekt_data_access_token = token
        _reset_client()


def set_api_url(url: str):
    """
    Set the API URL and reset the client to use the new URL.
    """
    global _nekt_api_url
    if _nekt_api_url != url:
        _nekt_api_url = url
        _reset_client()


def load_table(*, layer_name: str, table_name: str) -> DataFrame:
    """
    Load a table into the transformation.

    Args:
        layer_name: The name of the layer
        table_name: The name of the table

    Returns:
        DataFrame: The loaded table as a Spark DataFrame
    """
    client = _get_client()
    return client.load_table(layer_name=layer_name, table_name=table_name)


def load_delta_table(*, layer_name: str, table_name: str) -> DeltaTable:
    """
    Load a Delta table into the transformation.

    Args:
        layer_name: The name of the layer
        table_name: The name of the table

    Returns:
        DeltaTable: The loaded Delta table
    """
    client = _get_client()
    return client.load_delta_table(layer_name=layer_name, table_name=table_name)


def save_table(*, df: DataFrame, layer_name: str, table_name: str) -> bool:
    """
    Save a table into the layer.

    Args:
        df: The DataFrame to save
        layer_name: The name of the layer
        table_name: The name of the table

    Returns:
        bool: Success status
    """
    client = _get_client()
    return client.save_table(df=df, layer_name=layer_name, table_name=table_name)


def get_spark_session() -> SparkSession:
    """
    Get the shared Spark session instance.

    Returns:
        SparkSession: The shared Spark session
    """
    client = _get_client()
    return client.spark


def __getattr__(name: str):
    """Handle dynamic attribute access for data_access_token and api_url."""
    if name == "data_access_token":
        return _nekt_data_access_token
    elif name == "api_url":
        return _nekt_api_url
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __setattr__(name: str, value):
    """Handle dynamic attribute setting for data_access_token and api_url."""
    if name == "data_access_token":
        global _nekt_data_access_token
        if _nekt_data_access_token != value:
            _nekt_data_access_token = value
            _reset_client()
    elif name == "api_url":
        global _nekt_api_url
        if _nekt_api_url != value:
            _nekt_api_url = value
            _reset_client()
    else:
        # For other attributes, use the standard behavior
        sys.modules[__name__].__dict__[name] = value


__all__ = [
    "data_access_token",
    "api_url",
    "set_data_access_token",
    "set_api_url",
    "load_table",
    "load_delta_table",
    "save_table",
    "get_spark_session",
]
