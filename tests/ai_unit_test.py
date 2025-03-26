# Add the project root directory to sys.path
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.data_aggregation import calculate_total_price_by_date

import unittest
from pyspark.sql import SparkSession

# Define schemas and sample data
products_schema = ["Product_ID", "Price"]
transactions_schema = ["Transaction_Date", "Transaction_ID", "Product_ID", "Quantity"]

products_data = [
    (101, 5.99),
    (102, 2.99),
    (103, 7.49)
]

transactions_data = [
    ("2023-01-01", 1001, 101, 2),
    ("2023-01-01", 1002, 102, 5),
    ("2023-01-02", 1003, 101, 3),
    ("2023-01-02", 1004, 103, 1),
]

# Test class
class TestCalculateTotalPriceByDate(unittest.TestCase):
    
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestCalculateTotalPriceByDate") \
            .getOrCreate()
        
        # Create test data frames from mock data and schemas
        self.products_df = self.spark.createDataFrame(products_data, schema=products_schema)
        self.transactions_df = self.spark.createDataFrame(transactions_data, schema=transactions_schema)

    def tearDown(self):
        self.spark.stop()

    def test_calculate_total_price_by_date(self):
        # Call the function with the data frames
        result = calculate_total_price_by_date(self.products_df, self.transactions_df)
        
        # Collect the result and convert to list of tuples for easier comparison
        result_list = result.collect()
        
        expected_result = [
            ("2023-01-01", 7, 26.93),
            ("2023-01-02", 4, 25.46)
        ]
        
        # Compare the result with expected output
        self.assertEqual(result_list, expected_result)

if __name__ == '__main__':
    unittest.main()