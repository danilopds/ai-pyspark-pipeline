import pyspark.sql.functions as F

def calculate_total_price_by_date(df_products, df_transactions):
    # Join the products and transactions DataFrames on Product_ID
    df_joined = df_transactions.join(df_products, df_transactions.Product_ID == df_products.Product_ID)
    
    # Calculate the total price for each transaction
    df_total_price = df_joined.withColumn("Total_Price", F.col("Quantity") * F.col("Price"))
    
    # Group by Transaction_Date and calculate the total values
    df_total_by_date = df_total_price.groupBy("Transaction_Date").agg(
        F.sum("Quantity").alias("Total_Quantity"),
        F.sum("Total_Price").alias("Total_Price"),
    )
    
    return df_total_by_date