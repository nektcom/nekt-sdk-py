import os

import pytest


def pytest_report_header():
    suite = os.environ.get("PYTEST_SUITE_NAME")
    if suite:
        return [f"Suite: {suite}"]


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: tests requiring real API credentials (NEKT_DATA_ACCESS_TOKEN)",
    )

    # Set GOOGLE_APPLICATION_CREDENTIALS from GCP_SA_KEY_FILE early so all tests
    # (including non-Spark Python engine tests) use the correct SA credentials.
    sa_key_file = os.environ.get("GCP_SA_KEY_FILE", "")
    if sa_key_file and os.environ.get("NEKT_CLOUD_PROVIDER", "").upper() == "GCP":
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_key_file


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing with Delta Lake support.

    Auto-detects Java home and downloads Delta Lake jars from Maven Central.
    """
    import os
    import platform

    from pyspark.sql import SparkSession

    # Set Java home if not already set
    if "JAVA_HOME" not in os.environ:
        import shutil
        import subprocess

        system = platform.system()

        if system == "Darwin":
            possible_java_homes = [
                "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home",
                "/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home",
                "/opt/homebrew/opt/openjdk@17",
                "/usr/local/opt/openjdk@17",
            ]
        elif system == "Linux":
            possible_java_homes = [
                "/usr/lib/jvm/java-17-openjdk-amd64",
                "/usr/lib/jvm/java-11-openjdk-amd64",
                "/usr/lib/jvm/default-java",
                "/usr/lib/jvm/java-17",
                "/usr/lib/jvm/java-11",
            ]
        else:
            possible_java_homes = [
                "C:\\Program Files\\Java\\jdk-17",
                "C:\\Program Files\\Java\\jdk-11",
            ]

        for java_home in possible_java_homes:
            if os.path.exists(java_home):
                os.environ["JAVA_HOME"] = java_home
                break
        else:
            # Fallback: find a working java binary on PATH.
            # Iterate all PATH entries because macOS has a /usr/bin/java stub
            # that fails when no JDK is installed, and it may shadow a real
            # JDK later in PATH (e.g. Nix-managed).
            path_dirs = os.environ.get("PATH", "").split(os.pathsep)
            for d in path_dirs:
                java_bin = os.path.join(d, "java")
                if not os.path.isfile(java_bin):
                    continue
                try:
                    result = subprocess.run([java_bin, "-version"], capture_output=True, timeout=5)
                    if result.returncode != 0:
                        continue
                except (subprocess.TimeoutExpired, OSError):
                    continue
                java_real = os.path.realpath(java_bin)
                bin_dir = os.path.dirname(java_real)
                home_candidate = os.path.dirname(bin_dir)
                if os.path.basename(home_candidate) == "Home":
                    os.environ["JAVA_HOME"] = home_candidate
                else:
                    os.environ["JAVA_HOME"] = home_candidate
                break

    # Set GOOGLE_APPLICATION_CREDENTIALS for GCP before Spark starts.
    cloud_provider = os.environ.get("NEKT_CLOUD_PROVIDER", "").upper()
    if cloud_provider == "GCP":
        sa_key_file = os.environ.get("GCP_SA_KEY_FILE", "")
        if sa_key_file:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_key_file

    builder = (
        SparkSession.builder.appName("nekt-test")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.12:3.3.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.43.1",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
                "org.apache.iceberg:iceberg-aws-bundle:1.7.1",
            ]),
        )
    )

    return builder.getOrCreate()


@pytest.fixture(scope="session")
def integration_spark(spark):
    """Spark session with cloud credentials for integration tests.

    LOCAL mode: fetches credentials from API (Jupyter token).
    AWS mode: reads credentials from env vars.
    """
    from nekt.types import CloudCredentials, CloudProvider, Environment

    token = os.environ.get("NEKT_DATA_ACCESS_TOKEN")
    if not token:
        pytest.skip("NEKT_DATA_ACCESS_TOKEN not set")

    env_str = os.environ.get("NEKT_ENV", "LOCAL").upper()

    if env_str == "LOCAL":
        from nekt.api import NektAPI

        api = NektAPI(
            data_access_token=token,
            api_url=os.environ.get("NEKT_API_URL", "https://api.nekt.ai"),
            environment=Environment.LOCAL,
        )
        credentials = api.get_cloud_credentials()
    elif env_str == "AWS":
        credentials = CloudCredentials.from_aws(
            access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
            secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            session_token=os.environ.get("AWS_SESSION_TOKEN"),
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )
    else:
        credentials = CloudCredentials.from_gcp(
            access_token=os.environ.get("GCP_ACCESS_TOKEN", ""),
            project_id=os.environ.get("GCP_PROJECT_ID", ""),
        )

    # Configure Spark with cloud credentials for S3 access
    if credentials.aws_access_key_id:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", credentials.aws_access_key_id)
        hadoop_conf.set("fs.s3a.secret.key", credentials.aws_secret_access_key or "")
        hadoop_conf.set("fs.s3a.session.token", credentials.aws_session_token or "")
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )

    return spark
