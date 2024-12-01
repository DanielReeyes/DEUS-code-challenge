"""This module is to centralize the dataframe transform functions. """

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, when

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REPORT_ALLOWED_TYPES = ["CSV", "PARQUET", "JSON"]


def check_missing_data(dataframe_to_check: DataFrame, columns: list = None):
    """
    Checks for missing or null values in specified columns of a PySpark DataFrame.

    Parameters:
        dataframe_to_check (DataFrame): The PySpark DataFrame to be analyzed.
        columns (list, optional): A list of column names to check for missing values.
                                  If None, all columns in the DataFrame will be analyzed.

    Returns:
        None

    Raises:
        ValueError: If `dataframe_to_check` is not a valid PySpark DataFrame.
        ValueError: If `columns` contains names not present in the DataFrame.
    """
    if columns is None:
        columns = dataframe_to_check.columns
    else:
        invalid_columns = [column for column in columns if column not in dataframe_to_check.columns]
        if invalid_columns:
            raise ValueError(f"The following columns are not in the DataFrame: {invalid_columns}")

    logger.info(">>> Columns to be analyzed: %s", columns)

    for column in columns:
        if dataframe_to_check.filter(col(column).isNull()).count() > 0:
            logger.info(">>> Column %s contains Null", column)


def check_duplicate_data(dataframe_to_check: DataFrame, columns: list = None):
    """
    Checks for duplicate entries in specified columns of a PySpark DataFrame.

    Parameters:
        dataframe_to_check (DataFrame): The PySpark DataFrame to be analyzed.
        columns (list, optional): A list of column names to check for duplicates.
                                  If None, all columns in the DataFrame will be analyzed.

    Returns:
        bool: True if duplicates are found, False otherwise.

    Raises:
        ValueError: If `dataframe_to_check` is not a valid PySpark DataFrame.
        ValueError: If `columns` contains names not present in the DataFrame.
    """

    if columns is None:
        columns = dataframe_to_check.columns

    if not isinstance(dataframe_to_check, DataFrame):
        raise ValueError("The provided 'dataframe_to_check' is not a valid PySpark DataFrame.")

    invalid_columns = [col for col in columns if col not in dataframe_to_check.columns]
    if invalid_columns:
        raise ValueError(f"The following columns are not in the DataFrame: {invalid_columns}")

    logger.info(">>> Columns to be analyzed: %s", columns)

    if dataframe_to_check.select(columns).count() > dataframe_to_check.select(columns).distinct().count():
        logger.info("DataFrame has duplicates entries for columns: %s", columns)
        return True

    logger.info("No duplicates entries found for columns: %s", columns)
    return False


def standardize_date_type_columns(dataframe_to_standardize: DataFrame, date_type_columns: list = None):
    """
    Standardizes columns with date values into a specified format.

    Parameters:
        dataframe_to_standardize (DataFrame): The PySpark DataFrame to be standardized.
        date_type_columns (list, optional): List of column names to standardize.
            If not provided, the function will log an error.

    Returns:
        DataFrame: A new DataFrame with the standardized date columns.
    """
    if date_type_columns is None:
        logger.error("DateType columns not defined")
        return dataframe_to_standardize

    standardized_df = dataframe_to_standardize

    for column in date_type_columns:
        if column in standardized_df.columns:
            logger.info("Standardizing column: %s", column)

            standardized_df = standardized_df.withColumn(
                column,
                when(
                    to_date(col(column), "MMMM dd, yyyy").isNotNull(),
                    to_date(col(column), "MMMM dd, yyyy"),
                )
                .when(
                    to_date(col(column), "yyyy-MM-dd").isNotNull(),
                    to_date(col(column), "yyyy-MM-dd"),
                )
                .when(
                    to_date(col(column), "yyyy/MM/dd").isNotNull(),
                    to_date(col(column), "yyyy/MM/dd"),
                )
                .when(
                    to_date(col(column), "MM/dd/yyyy").isNotNull(),
                    to_date(col(column), "MM/dd/yyyy"),
                )
                .when(
                    to_date(col(column), "dd-MM-yyyy").isNotNull(),
                    to_date(col(column), "dd-MM-yyyy"),
                )
                .when(
                    to_date(col(column), "MM-dd-yyyy").isNotNull(),
                    to_date(col(column), "MM-dd-yyyy"),
                )
                .otherwise(lit(None)),
            )
        else:
            logger.warning("Column %s not found in DataFrame", column)

    return standardized_df


def categorize_price(price: float) -> str:
    """
    Categorizes a price into one of three categories: 'Low', 'Medium', or 'High'.

    Parameters:
        price (float): The price to be categorized.

    Returns:
        str: The price category. Possible values are:
            - 'Low' if the price is less than 20.
            - 'Medium' if the price is between 20 and 100, inclusive.
            - 'High' if the price is greater than 100.

    Raises:
        ValueError: If the provided price is not a valid float or is negative.
    """

    if not isinstance(price, (int, float)):
        raise ValueError(f"Invalid type for price: {type(price)}. Expected int or float.")
    if price < 0:
        raise ValueError(f"Price cannot be negative: {price}")

    if price < 20:
        return "Low"
    if 20 <= price <= 100:
        return "Medium"
    return "High"


def write_report(dataframe_to_write: DataFrame, write_type: str = "CSV", output_path: str = "export/"):
    """
    Writes a PySpark DataFrame to a specified file format.

    Parameters:
        dataframe_to_write (DataFrame): The PySpark DataFrame to be written.
        write_type (str, optional): The type of file to write.
                                    Supported types are "CSV", "PARQUET", and "JSON".
                                    Default is "CSV".
        output_path (str, optional): The path where the file will be written. Default is "export/".

    Returns:
        None

    Raises:
        ValueError: If the specified write_type is not supported or the output_path is invalid.
        Exception: If writing the file fails for any reason.
    """

    if write_type.upper() not in REPORT_ALLOWED_TYPES:
        raise ValueError(f"Unsupported write_type '{write_type}'. Supported types are {REPORT_ALLOWED_TYPES}.")

    if not isinstance(dataframe_to_write, DataFrame):
        raise ValueError("Invalid DataFrame. Please provide a valid PySpark DataFrame.")

    if not output_path or not isinstance(output_path, str):
        raise ValueError("Invalid output path. Please provide a valid string path.")

    try:
        if write_type.upper() == "CSV":
            dataframe_to_write.write.mode("overwrite").option("header", "true").csv(output_path)
            logger.info("Dataset successfully saved as CSV at %s", output_path)

        elif write_type.upper() == "PARQUET":
            dataframe_to_write.write.partitionBy("category", "transaction_date").mode("overwrite").parquet(output_path)
            logger.info("Dataset successfully saved as PARQUET at %s", output_path)

        elif write_type.upper() == "JSON":
            dataframe_to_write.write.mode("overwrite").json(output_path)
            logger.info("Dataset successfully saved as JSON at %s", output_path)

    except Exception as exception:
        logger.error("Failed to write dataset: %s", exception)
        raise
