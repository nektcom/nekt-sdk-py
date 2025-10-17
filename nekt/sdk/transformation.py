from typing import Dict, List, Optional

import requests
from delta.tables import DeltaTable
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession

from nekt.sdk.service.auth import get_cloud_credentials
from nekt.sdk.service.secrets import get_secret


class TransformationClient:
    data_access_token: str

    def __init__(self, data_access_token: str, api_url: str = "https://api.nekt.ai", spark: SparkSession = None):
        self.data_access_token = data_access_token
        self.api_url = api_url
        self._spark = spark or self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        credentials = get_cloud_credentials(self.data_access_token)

        conf = (
            SparkConf()
            .setAppName("Nekt-Transformation")  # replace with your desired name
            .set("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.hadoop.fs.s3a.access.key", credentials["aws_access_key_id"])
            .set("spark.hadoop.fs.s3a.secret.key", credentials["aws_secret_access_key"])
            .set("spark.hadoop.fs.s3a.session.token", credentials["aws_session_token"])
            # default is 200 partitions which is too many for local
            .set("spark.sql.shuffle.partitions", "4")
            # replace the * with your desired number of cores. * for use all.
            .setMaster("local[*]")
        )

        return SparkSession.builder.config(conf=conf).getOrCreate()

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def _get_table_details(self, layer_identifier: str, table_identifier: str) -> Dict[str, str]:
        url: str = f"{self.api_url}/api/v1/i/layers/{layer_identifier}/tables/{table_identifier}?use_s3a=true"
        headers = {"X-Jupyter-Token": self.data_access_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def load_volume(self, layer_name: str, volume_name: str) -> List[Dict[str, str]]:
        """
        Load a volume into the transformation.
        """
        if not layer_name:
            raise Exception("Layer name is required")
        if not volume_name:
            raise Exception("Volume name is required")

        url: str = f"{self.api_url}/api/v1/i/layers/{layer_name}/volumes/{volume_name}/get-file-paths/"
        headers = {"X-Jupyter-Token": self.data_access_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data

    def load_table(self, *, layer_name: str, table_name: str) -> DataFrame:
        """
        Load a table into the transformation.
        """
        delta_table = self.load_delta_table(layer_name=layer_name, table_name=table_name)
        return delta_table.toDF()

    def load_delta_table(self, *, layer_name: str, table_name: str) -> DeltaTable:
        """
        Load a table into the transformation.
        """
        if not layer_name:
            raise Exception("Layer name is required")
        if not table_name:
            raise Exception("Table name is required")

        table_details = self._get_table_details(layer_name, table_name)
        s3_path = table_details.get("s3_path")
        return DeltaTable.forPath(self.spark, s3_path)

    def save_table(self, *, df: DataFrame, layer_name: str, table_name: str, folder_name: Optional[str] = None) -> bool:
        """
        Save a table into the layer.
        """
        print(
            f"WARNING: Table saving is not available in the local development environment. "
            f'To save dataframe as table "{table_name}" in folder "{folder_name}" '
            f'of layer "{layer_name}", use the Nekt Production environment.'
        )
        return False

    def load_secret(self, *, key: str) -> str:
        """
        Load a secret value by key from the organization secrets.
        """
        if not key:
            raise Exception("Secret key is required")

        return get_secret(self.data_access_token, key, self.api_url)
