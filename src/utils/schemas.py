"""
This module contains the definition of schemas for the DataFrames in the project.
The schemas are organized into classes for better structure and reusability.

The classes define the types of each column in the CSV files that are read by Spark,
ensuring consistent data loading without the need for schema inference.

Each class can be used to define the schema when loading the respective CSV files.

"""

import dataclasses

from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@dataclasses.dataclass
class ProductSchema:
    """
    Class defining the schema for the 'products_uuid.csv' file.
    Expected columns are 'product_id', 'product_name', and 'category'.
    """

    def __init__(self):
        self.schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
            ]
        )


@dataclasses.dataclass
class SalesSchema:
    """
    Class defining the schema for the 'sales_uuid.csv' file.
    Expected columns are 'transaction_id', 'store_id', 'product_id', 'quantity',
    'transaction_date', and 'price'.
    """

    def __init__(self):
        self.schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("store_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("transaction_date", StringType(), True),
                StructField("price", FloatType(), True),
            ]
        )


@dataclasses.dataclass
class StoresSchema:
    """
    Class defining the schema for the 'stores_uuid.csv' file.
    Expected columns are 'store_id', 'store_name', and 'location'.
    """

    def __init__(self):
        self.schema = StructType(
            [
                StructField("store_id", StringType(), True),
                StructField("store_name", StringType(), True),
                StructField("location", StringType(), True),
            ]
        )
