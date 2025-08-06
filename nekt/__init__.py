import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from nekt.sdk.transformation import TransformationClient


class NektModule:
    """Module wrapper class to handle dynamic attributes properly."""

    def __init__(self):
        # Global variables to store the data access token, URL, and client instance (all prefixed with _nekt_)
        object.__setattr__(self, "_nekt_data_access_token", None)
        object.__setattr__(self, "_nekt_api_url", "https://api.nekt.ai")
        object.__setattr__(self, "_nekt_client", None)

    def _get_client(self) -> TransformationClient:
        """
        Get the global client instance, creating it if necessary.
        """
        if self._nekt_data_access_token is None:
            raise ValueError(
                "data_access_token must be set. Set nekt.data_access_token = 'your_token' before using the SDK."
            )

        if self._nekt_client is None:
            object.__setattr__(
                self,
                "_nekt_client",
                TransformationClient(data_access_token=self._nekt_data_access_token, api_url=self._nekt_api_url),
            )

        return self._nekt_client

    def _reset_client(self):
        """
        Reset the global client instance. This will force a new client to be created
        on the next method call, ensuring the new data_access_token is used.
        """
        if self._nekt_client is not None:
            # Clean up the existing Spark session if needed
            if hasattr(self._nekt_client, "_spark") and self._nekt_client._spark is not None:
                self._nekt_client._spark.stop()
        object.__setattr__(self, "_nekt_client", None)

    def set_data_access_token(self, token: str):
        """
        Set the data access token and reset the client to use the new token.
        """
        if self._nekt_data_access_token != token:
            object.__setattr__(self, "_nekt_data_access_token", token)
            self._reset_client()

    def set_api_url(self, url: str):
        """
        Set the API URL and reset the client to use the new URL.
        """
        if self._nekt_api_url != url:
            object.__setattr__(self, "_nekt_api_url", url)
            self._reset_client()

    def load_table(self, *, layer_name: str, table_name: str) -> DataFrame:
        """
        Load a table into the transformation.

        Args:
            layer_name: The name of the layer
            table_name: The name of the table

        Returns:
            DataFrame: The loaded table as a Spark DataFrame
        """
        client = self._get_client()
        return client.load_table(layer_name=layer_name, table_name=table_name)

    def load_delta_table(self, *, layer_name: str, table_name: str) -> DeltaTable:
        """
        Load a Delta table into the transformation.

        Args:
            layer_name: The name of the layer
            table_name: The name of the table

        Returns:
            DeltaTable: The loaded Delta table
        """
        client = self._get_client()
        return client.load_delta_table(layer_name=layer_name, table_name=table_name)

    def save_table(self, *, df: DataFrame, layer_name: str, table_name: str) -> bool:
        """
        Save a table into the layer.

        Args:
            df: The DataFrame to save
            layer_name: The name of the layer
            table_name: The name of the table

        Returns:
            bool: Success status
        """
        client = self._get_client()
        return client.save_table(df=df, layer_name=layer_name, table_name=table_name)

    def get_spark_session(self) -> SparkSession:
        """
        Get the shared Spark session instance.

        Returns:
            SparkSession: The shared Spark session
        """
        client = self._get_client()
        return client.spark

    def __getattr__(self, name: str):
        """Handle dynamic attribute access for data_access_token and api_url."""
        if name == "data_access_token":
            return self._nekt_data_access_token
        elif name == "api_url":
            return self._nekt_api_url
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    def __setattr__(self, name: str, value):
        """Handle dynamic attribute setting for data_access_token and api_url."""
        if name == "data_access_token":
            if getattr(self, "_nekt_data_access_token", None) != value:
                object.__setattr__(self, "_nekt_data_access_token", value)
                self._reset_client()
        elif name == "api_url":
            if getattr(self, "_nekt_api_url", None) != value:
                object.__setattr__(self, "_nekt_api_url", value)
                self._reset_client()
        else:
            # For other attributes, use the standard behavior
            object.__setattr__(self, name, value)


# Replace this module with an instance of our wrapper class
sys.modules[__name__] = NektModule()


# Re-export functions at module level for convenience (this will be ignored since we replaced the module)
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
