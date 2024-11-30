"""
Test suite for the dataframe_helper module using Chispa for DataFrame validation.
"""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

from src.utils.dataframe_helpers import (
    categorize_price,
    check_duplicate_data,
    check_missing_data,
    standardize_date_type_columns,
)

spark = SparkSession.builder.appName("DEUS-code-challenge").getOrCreate()


def test_check_missing_data():
    """
    Test the check_missing_data function.
    """
    input_data = [(1, "John", None), (2, "Jane", 30), (3, None, 25)]
    input_columns = ["id", "name", "age"]
    df = spark.createDataFrame(input_data, input_columns)

    # Expect logger to indicate missing data in specific columns
    check_missing_data(df, columns=["name", "age"])


def test_check_duplicate_data():
    """
    Test the check_duplicate_data function.
    """
    input_data = [(1, "John", 25), (2, "Jane", 30), (2, "Jane", 30)]
    input_columns = ["id", "name", "age"]
    df = spark.createDataFrame(input_data, input_columns)

    result = check_duplicate_data(df, columns=["id", "name", "age"])
    assert result is True

    unique_data = [(1, "John", 25), (2, "Jane", 30)]
    unique_df = spark.createDataFrame(unique_data, input_columns)
    result_unique = check_duplicate_data(unique_df, columns=["id", "name", "age"])
    assert result_unique is False


def test_standardize_date_type_columns():
    """
    Test the standardize_date_type_columns function.
    """
    input_data = [("1", "January 01, 2022"), ("2", "2022-01-02"), ("3", "01/03/2022")]
    input_columns = ["id", "date_column"]
    df = spark.createDataFrame(input_data, input_columns)

    expected_data = [("1", "2022-01-01"), ("2", "2022-01-02"), ("3", "2022-01-03")]
    expected_df = spark.createDataFrame(expected_data, ["id", "date_column"]).withColumn(
        "date_column", to_date(col("date_column"))
    )

    result_df = standardize_date_type_columns(df, date_type_columns=["date_column"])

    # Compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_categorize_price():
    """
    Test the categorize_price function.
    """
    assert categorize_price(10) == "Low"
    assert categorize_price(50) == "Medium"
    assert categorize_price(150) == "High"

    with pytest.raises(ValueError):
        categorize_price(-10)

    with pytest.raises(ValueError):
        categorize_price("invalid")
