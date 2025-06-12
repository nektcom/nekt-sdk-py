from typing import Dict

import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def _get_table_details(layer_identifier: str, table_identifier: str) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    # url: str = f"https://api.nekt.ai/api/v1/i/layers/{layer_identifier}/tables/{table_identifier}"
    url: str = f"http://host.docker.internal:8000/api/v1/i/layers/{layer_identifier}/tables/{table_identifier}"
    if jupyter_token := _get_data_access_token_from_caller():
        headers = {"X-Jupyter-Token": jupyter_token}
        url = url + "?use_s3a=true"
    elif pipeline_run_token := _get_pipeline_run_token_from_caller():
        headers = {"X-Pipeline-Run-Token": pipeline_run_token}
    else:
        raise Exception("Missing token to get table details")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def load_table(*, layer_name: str, table_name: str, spark: SparkSession = None) -> DeltaTable:
    """
    Load a table into the transformation.
    If a spark session is not provided, it will try to find a `spark` variable
    in the caller's scope.
    """
    if not layer_name:
        raise Exception("Layer name is required")
    if not table_name:
        raise Exception("Table name is required")

    spark = spark or _get_spark_from_caller_or_raise()

    table_details = _get_table_details(layer_name, table_name)
    s3_path = table_details.get("s3_path")
    return DeltaTable.forPath(spark, s3_path)


def save_table(layer_name: str, table_name: str) -> bool:
    """
    Save a table into the layer.
    """
    print("Table can only be saved in Nekt Production environment")
