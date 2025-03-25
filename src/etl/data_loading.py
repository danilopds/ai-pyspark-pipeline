import os

def load_products_data(spark):
    """
    Load products data from a CSV file.
    Ensure the file 'data/products.csv' exists
    """    
    file_path = os.path.join("data", "products.csv")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def load_transactions_data(spark):
    """
    Load transactions data from a CSV file.
    Ensure the file 'data/transactions.csv' exists
    """
    file_path = os.path.join("data", "transactions.csv")
    return spark.read.csv(file_path, header=True, inferSchema=True)