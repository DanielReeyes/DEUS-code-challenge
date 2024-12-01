# DEUS-code-challenge

## Part 1: Data Preparation

Perform basic data validation:
The first step is to load the three datasets into three different PySpark Dataframes.  

### Check for missing or null values and inconsistencies in the data format.  
    - I created a generic function (check_missing_data) to receive a dataframe and a list of columns.  
        With that, the user can check only a few columns. In case of no pass columns, the function checks all columns in the dataframe.  
    - The function will log if there is some null value.  
    - To test, you can manually add the row: "dd98c054-b910-4d10-991d-82adca90c819,Raspberrie," in products_uuid.csv  

### Identify and handle duplicates, if any.
    - I created a generic function (check_duplicate_data) to receive a dataframe and a list of columns  
        With that, the user can check only a few columns. If columns are not passed, the function checks all columns in the dataframe.  
    - The function will log if there is a duplicate entry.  
    - Also, it will return a boolean value (True | False)  
    - To test,t you can manually add the row: "dd98c054-b910-4d10-991d-82adca90c819,Raspberries,Fruit" in products_uuid.csv  
    Ps.: We can add a step to remove the duplicates here  

### Enforce the appropriate data types for all columns. 
    - I created schema.py with the data types to read the CSV.  
    - Additional: I noticed that the date column in the sales CSV doesn't have a standard.
        - Created a function to standardize it with some date formats found.

## Part 2: Data Transformation

To perform the report tasks, I created a general dataframe (enriched_dataframe) combining the three dataframes

### Sales Aggregation:
Calculate the total revenue for each store (store_id) and each product category.
Output: DataFrame with store_id, category, and total_revenue.

    - With this general dataframe I just aggregated it using the asked columns (store_id, category)

### Monthly Sales Insights:
Calculate the total quantity sold for each product category, grouped by month.
Output: DataFrame with year, month, category, and total_quantity_sold. 

    As the date was already standardized, it was easy to call the function month and aggregate the values as asked.

### Enrich Data:
Combine the sales, products, and stores datasets into a single enriched dataset
with the following columns: transaction_id , store_name , location ,
product_name , category , quantity , transaction_date , and price .

    - I used the dataframe created early and removed the unnecessary columns.

### PySpark UDF:
Implement a PySpark UDF to categorize products based on the following price ranges:

    I created the function on dataframe_helpers
    Registered the function and used it.
        - Saved the result dataframe as "aditional_enriched_dataset.csv" 


## Part 3: Data Export
Save the enriched dataset from Part 2, Task 3, in Parquet format, partitioned by
category and transaction_date.
Save the store_id-level revenue insights (from Part 2, Task 1) in CSV format.

    - Created a generic function "write_report" that receives the output_path and the format type.
        - Call it as many times as you want.
    - By default, I use the overwrite, but it is possible to make the function store split by date_time and, with that, just append the data.

## Tests
Use the following package for PySpark tests - https://github.com/MrPowers/chispa -
the application needs to have tests. 

    - As requested, I used chispa suite to perform the unit tests for the functions created.

## Extra: Validate the exported data
    - Created a test class to validate the CSV and Parquet exported data.
    - Is possible, in the future, to change the test function to use SQL with the business logic and check if the exported data is the right one.

# To run:
    You can run the command `pip install -r requirements.txt` to install all dependencies.
    After that, you should be able to run the main script `src/main.py`

# TODOs
    I want to create a file to store all config values, such as the output path, the type file, etc.
    