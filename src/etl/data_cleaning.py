import pyspark.sql.functions as F
import pyspark.sql.types as T

def clean_products_data(df_products):
    return df_products.withColumn("Price", F.col("Price").cast(T.DecimalType(10, 2)))

def clean_transactions_data(df_transactions):
    return (
        df_transactions
        .withColumn("Transaction_Date", F.to_date(F.col("Transaction_Date"), "yyyy-MM-dd"))
        .withColumn("Quantity", F.col("Quantity").cast(T.IntegerType()))
    )