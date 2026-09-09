"""Reading an external table from Spark (NEKT-5114).

An external table in the Nekt catalog is a BigQuery **view** over a table in the customer's own
project. The spark-bigquery connector refuses a view outright unless ``viewsEnabled=true``, and
with it the connector **materializes** the view into a real table before reading — which needs a
dataset to write into, co-located with the view.

Neither fact is discoverable here: the SDK asks the backend for table details, so the backend
sends ``views_enabled`` and ``materialization_dataset`` and these tests pin that the loader passes
them through. Without them a PySpark transformation over an external table fails mid-run with a
BigQuery error that never mentions views.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nekt.provider.spark_bigquery import SparkBigQueryProvider


class _Reader:
    """Records the connector options the provider sets."""

    def __init__(self) -> None:
        self.options: dict[str, str] = {}
        self.loaded = False

    def format(self, _name: str) -> "_Reader":
        return self

    def option(self, key: str, value: str) -> "_Reader":
        self.options[key] = value
        return self

    def load(self) -> str:
        self.loaded = True
        return "dataframe"


@pytest.fixture
def spark() -> MagicMock:
    session = MagicMock()
    session.read = _Reader()
    return session


def test_a_view_is_read_with_views_enabled_and_a_materialization_dataset(spark):
    provider = SparkBigQueryProvider(spark=spark)

    provider.load("silver.pedidos", views_enabled=True, materialization_dataset="nekt_ext_materialization")

    assert spark.read.options["viewsEnabled"] == "true"
    assert spark.read.options["materializationDataset"] == "nekt_ext_materialization"
    assert spark.read.loaded


def test_a_managed_table_is_read_exactly_as_before(spark):
    """Every PySpark transformation in the product goes through this call. A managed table must
    not start materializing anything."""
    provider = SparkBigQueryProvider(spark=spark)

    provider.load("silver.clientes")

    assert "viewsEnabled" not in spark.read.options
    assert "materializationDataset" not in spark.read.options


def test_enabling_views_without_a_dataset_is_refused_here(spark):
    """The connector's own failure for this is a BigQuery permission error naming a dataset the
    user never heard of. Refusing before the job starts says what is actually wrong."""
    from nekt.exceptions import BigQueryProviderError

    provider = SparkBigQueryProvider(spark=spark)

    with pytest.raises(BigQueryProviderError, match="materialization"):
        provider.load("silver.pedidos", views_enabled=True)
