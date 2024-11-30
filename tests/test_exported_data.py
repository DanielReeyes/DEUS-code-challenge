"""
Test suite for the dataframe_helper module using Chispa for DataFrame validation.
"""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number
from pyspark.sql.functions import sum as spark_sum

from src.utils.dataframe_helpers import standardize_date_type_columns
from src.utils.schemas import ProductSchema, SalesSchema, StoresSchema

spark = SparkSession.builder.appName("DEUS-code-challenge").getOrCreate()


def test_exported_enriched_dataset():
    """
    Test the exported enriched dataset function.
    """
    exported_enriched_dataframe = spark.read.parquet("../src/export/enriched_dataset/")

    products_dataframe = spark.read.csv(
        "../src/data/products_uuid.csv", header=True, schema=ProductSchema().schema
    )

    sales_dataframe = spark.read.csv(
        "../src/data/sales_uuid.csv", header=True, schema=SalesSchema().schema
    )

    stores_dataframe = spark.read.csv(
        "../src/data/stores_uuid.csv", header=True, schema=StoresSchema().schema
    )

    sales_formatted = standardize_date_type_columns(
        sales_dataframe, ["transaction_date"]
    )

    enriched_dataframe = (
        products_dataframe.join(
            sales_formatted, products_dataframe.product_id == sales_formatted.product_id
        )
        .join(stores_dataframe, sales_formatted.store_id == stores_dataframe.store_id)
        .select(
            products_dataframe.product_name,
            products_dataframe.category,
            sales_formatted.transaction_id,
            sales_formatted.quantity,
            sales_formatted.transaction_date,
            sales_formatted.price,
            stores_dataframe.store_id,
        )
    )

    assert_df_equality(
        exported_enriched_dataframe,
        enriched_dataframe,
        ignore_column_order=True,
        ignore_row_order=True,
    )


def test_exported_sales_dataset():
    """
    Test the exported sales dataset function.
    """
    exported_sales_dataframe = spark.read.csv(
        "../src/export/sales_dataset/", header=True
    )

    products_dataframe = spark.read.csv(
        "../src/data/products_uuid.csv", header=True, schema=ProductSchema().schema
    )

    sales_dataframe = spark.read.csv(
        "../src/data/sales_uuid.csv", header=True, schema=SalesSchema().schema
    )

    stores_dataframe = spark.read.csv(
        "../src/data/stores_uuid.csv", header=True, schema=StoresSchema().schema
    )

    sales_formatted = standardize_date_type_columns(
        sales_dataframe, ["transaction_date"]
    )

    enriched_dataframe = (
        products_dataframe.join(
            sales_formatted, products_dataframe.product_id == sales_formatted.product_id
        )
        .join(stores_dataframe, sales_formatted.store_id == stores_dataframe.store_id)
        .select(
            products_dataframe.product_name,
            products_dataframe.category,
            sales_formatted.transaction_id,
            sales_formatted.quantity,
            sales_formatted.transaction_date,
            sales_formatted.price,
            stores_dataframe.store_id,
        )
    )

    total_revenue_dataframe = (
        enriched_dataframe.withColumn("total_revenue", col("quantity") * col("price"))
        .groupBy("store_id", "category")
        .agg(spark_sum("total_revenue").alias("total_revenue"))
        .withColumn("total_revenue", format_number("total_revenue", 2))
    )

    assert_df_equality(
        exported_sales_dataframe,
        total_revenue_dataframe,
        ignore_column_order=True,
        ignore_row_order=True,
    )
