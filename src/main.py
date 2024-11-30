"""This is the main python file, it contains the script to generate the insight reports """

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number, month
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import udf, year
from pyspark.sql.types import StringType

from utils.dataframe_helpers import (
    categorize_price,
    check_duplicate_data,
    check_missing_data,
    standardize_date_type_columns,
    write_report,
)
from utils.schemas import ProductSchema, SalesSchema, StoresSchema

if __name__ == "__main__":

    spark = SparkSession.builder.appName("DEUS-code-challenge").getOrCreate()

    products_dataframe = spark.read.csv(
        "data/products_uuid.csv", header=True, schema=ProductSchema().schema
    )

    sales_dataframe = spark.read.csv(
        "data/sales_uuid.csv", header=True, schema=SalesSchema().schema
    )

    stores_dataframe = spark.read.csv(
        "data/stores_uuid.csv", header=True, schema=StoresSchema().schema
    )

    products_dataframe.show(5)
    products_dataframe.printSchema()

    sales_dataframe.show(5)
    sales_dataframe.printSchema()

    stores_dataframe.show(5)
    stores_dataframe.printSchema()

    check_missing_data(products_dataframe)
    check_missing_data(sales_dataframe)
    check_missing_data(stores_dataframe)

    check_duplicate_data(products_dataframe)
    check_duplicate_data(sales_dataframe)
    check_duplicate_data(stores_dataframe)

    sales_formatted = standardize_date_type_columns(
        sales_dataframe, ["transaction_date"]
    )
    sales_formatted.show(5)
    sales_formatted.printSchema()

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

    enriched_dataframe.show(5)

    total_revenue_dataframe = (
        enriched_dataframe.withColumn("total_revenue", col("quantity") * col("price"))
        .groupBy("store_id", "category")
        .agg(spark_sum("total_revenue").alias("total_revenue"))
        .withColumn("total_revenue", format_number("total_revenue", 2))
    )

    total_revenue_dataframe.show(5)
    total_revenue_dataframe.printSchema()

    monthly_sales_dataframe = (
        enriched_dataframe.withColumn("year", year(col("transaction_date")))
        .withColumn("month", month(col("transaction_date")))
        .groupBy("year", "month", "category")
        .agg(spark_sum("quantity").alias("total_quantity_sold"))
        .orderBy("year", "month", "category")
    )

    monthly_sales_dataframe.show(5)

    categorize_price_udf = udf(categorize_price, StringType())

    enriched_dataframe = enriched_dataframe.withColumn(
        "price_category", categorize_price_udf("price")
    )

    enriched_dataframe.show(5)

    write_report(
        enriched_dataframe.drop("price_category"), "PARQUET", "export/enriched_dataset"
    )
    write_report(total_revenue_dataframe, "CSV", "export/sales_dataset")

    spark.stop()
