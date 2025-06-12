from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from nekt.sdk.auth import get_cloud_credentials


class TransformationClient:

    def __init__(self, data_access_token: str):
        self.data_access_token = data_access_token
        self._spark = self._create_spark_session(data_access_token)

    data_access_token: str

    def _create_spark_session(self, data_access_token: str) -> SparkSession:
        """
        Create a Spark session.
        """

        credentials = get_cloud_credentials(data_access_token)

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

        self._spark = SparkSession.builder.config(conf=conf).getOrCreate()

    @property
    def spark(self) -> SparkSession:
        return self._spark
