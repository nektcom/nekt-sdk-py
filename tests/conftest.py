import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: tests requiring real API credentials (NEKT_DATA_ACCESS_TOKEN)",
    )


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

    builder = (
        SparkSession.builder.appName("nekt-test")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
    )

    return builder.getOrCreate()
