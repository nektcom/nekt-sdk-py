# Nekt SDK for Python

The Nekt SDK for Python provides a simple and powerful interface for data transformations and analytics workflows. It enables seamless data loading, processing, and saving across different data layers.

## Installation

Install the Nekt SDK using pip:

```bash
pip install nekt-sdk
```

## Requirements

- Python >= 3.9
- Apache Spark 3.5.5
- Delta Lake 3.3.0

## Quick Start

### Authentication

Before using the SDK, you need to set your data access token:

```python
import nekt

# Set your authentication token
nekt.data_access_token = "ADD_YOUR_TOKEN_HERE"
```

### Basic Usage

```python
import nekt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Set your authentication token
nekt.data_access_token = "ADD_YOUR_TOKEN_HERE"

# Load your source tables (you can load multiple tables)
deals_df: DataFrame = nekt.load_table(layer_name="Extraction", table_name="pipedrive_deals")
persons_df: DataFrame = nekt.load_table(layer_name="Extraction", table_name="pipedrive_persons")

# Apply your data logic, modeling, or AI workflow here
deals_persons_df: DataFrame = (
    deals_df.join(
        persons_df.withColumnRenamed("id", "person_id"),
        on="person_id",
    )
)
high_value_deals_df: DataFrame = deals_persons_df.filter(F.col("value") > 1000)

# Save your results to one or more output tables
nekt.save_table(
    df=deals_persons_df, 
    layer_name="Trusted", 
    table_name="pipedrive_deals_persons",
)
nekt.save_table(
    df=high_value_deals_df, 
    layer_name="Service", 
    table_name="pipedrive_high_value_deals", 
    folder_name="Reports",
)
```

## API Reference

### Data Loading

#### `nekt.load_table(layer_name: str, table_name: str) -> DataFrame`
Load a table as a Spark DataFrame.

**Parameters:**
- `layer_name`: The name of the data layer (e.g., "Extraction", "Trusted", "Service")
- `table_name`: The name of the table to load

**Returns:** PySpark DataFrame

```python
df = nekt.load_table(layer_name="Extraction", table_name="sales_data")
```

#### `nekt.load_delta_table(layer_name: str, table_name: str) -> DeltaTable`
Load a table as a Delta Lake table for advanced operations.

**Parameters:**
- `layer_name`: The name of the data layer
- `table_name`: The name of the table to load

**Returns:** DeltaTable object

```python
delta_table = nekt.load_delta_table(layer_name="Trusted", table_name="customer_data")
```

#### `nekt.load_volume(layer_name: str, volume_name: str) -> List[Dict[str, str]]`
Load file paths from a data volume.

**Parameters:**
- `layer_name`: The name of the data layer
- `volume_name`: The name of the volume to load

**Returns:** List of file path dictionaries

```python
file_paths = nekt.load_volume(layer_name="Raw", volume_name="documents")
```

### Data Saving

#### `nekt.save_table(df: DataFrame, layer_name: str, table_name: str, folder_name: Optional[str] = None) -> bool`
Save a DataFrame to a specified layer and table.

**Parameters:**
- `df`: The PySpark DataFrame to save
- `layer_name`: The target data layer
- `table_name`: The target table name
- `folder_name`: Optional folder within the layer

**Returns:** Success status (boolean)

**Note:** Table saving is only available in the Nekt Production environment. In local development, this function will display a warning and return `False`.

```python
success = nekt.save_table(
    df=processed_data,
    layer_name="Service",
    table_name="analytics_results",
    folder_name="monthly_reports"
)
```

### Spark Session Management

#### `nekt.get_spark_session() -> SparkSession`
Get the shared Spark session instance.

**Returns:** SparkSession object

```python
spark = nekt.get_spark_session()
spark.createDataFrame([{'name': 'Alice', 'age': 1}]).show()
+---+-----+
|age| name|
+---+-----+
|  1|Alice|
+---+-----+
```
## Advanced Usage

### Custom Spark Configuration

The SDK automatically configures Spark with optimized settings for Delta Lake and cloud storage. The default configuration includes:

- Delta Lake extensions and catalog
- AWS S3 connectivity
- Optimized partition settings for local development
- Multi-core processing (local[*])

## Examples

### Data Transformation Pipeline

```python
import nekt
from pyspark.sql import functions as F

# Authentication
nekt.data_access_token = "your_token_here"

# Load multiple source tables
orders = nekt.load_table("Extraction", "ecommerce_orders")
customers = nekt.load_table("Extraction", "ecommerce_customers")
products = nekt.load_table("Extraction", "ecommerce_products")

# Join and transform data
enriched_orders = (
    orders
    .join(customers, "customer_id")
    .join(products, "product_id")
    .withColumn("order_value", F.col("quantity") * F.col("unit_price"))
    .withColumn("customer_segment", 
        F.when(F.col("total_spent") > 1000, "Premium")
        .when(F.col("total_spent") > 500, "Standard")
        .otherwise("Basic")
    )
)

# Create aggregated views
daily_sales = (
    enriched_orders
    .groupBy(F.date_trunc("day", "order_date").alias("date"))
    .agg(
        F.sum("order_value").alias("total_sales"),
        F.count("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
)

# Save results
nekt.save_table(enriched_orders, "Trusted", "enriched_orders")
nekt.save_table(daily_sales, "Service", "daily_sales_summary")
```


## Support

For questions, issues, or feature requests, please contact the Nekt support team.
