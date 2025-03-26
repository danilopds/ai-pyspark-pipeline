from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

def check_data_quality(df_products, df_transactions):
    # Check if required columns exist in both dataframes
    missing_columns_products = set(['Product_ID']) - set(df_products.columns)
    missing_columns_transactions = set(['Product_ID', 'Transaction_Date', 'Quantity']) - set(df_transactions.columns)

    if missing_columns_products:
        raise ValueError(f"Missing columns in df_products: {missing_columns_products}")

    if missing_columns_transactions:
        raise ValueError(f"Missing columns in df_transactions: {missing_columns_transactions}")

    # Check data types of key columns
    if not (df_transactions.select(F.col("Transaction_Date")).dtypes[0][1] == 'date' or 
            df_transactions.select(F.col("Transaction_Date")).dtypes[0][1] == 'timestamp'):
        raise ValueError("Transaction_Date should be a date or timestamp")

    # Check for missing values
    null_columns_products = [col for col in df_products.columns if df_products.filter(F.col(col).isNull()).count() > 0]
    null_columns_transactions = [col for col in df_transactions.columns if df_transactions.filter(F.col(col).isNull()).count() > 0]

    if null_columns_products:
        raise ValueError(f"Missing values found in df_products: {null_columns_products}")

    if null_columns_transactions:
        raise ValueError(f"Missing values found in df_transactions: {null_columns_transactions}")
    
    return True