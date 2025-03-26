# Add the project root directory to sys.path
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import SparkSession
from pyspark.sql import SparkSession

# Import modules from the project structure
import src.etl.data_loading as dl
import src.etl.data_cleaning  as dc
import src.etl.data_aggregation as da
import src.quality_checks.ai_data_quality as dq

def _create_spark_session():
    return SparkSession.builder.appName("test_code").getOrCreate()

def run_data_etl():
    spark = _create_spark_session()

    # load and clean data
    df_products = dc.clean_products_data(dl.load_products_data(spark))
    df_transactions = dc.clean_transactions_data(dl.load_transactions_data(spark))

    if dq.check_data_quality(df_products, df_transactions):
        print("Data quality checks passed")

        # display data
        print("'df_products' and 'df_transactions'")
        df_products.show(truncate=False)
        df_transactions.show(truncate=False)

        # calculate total price by date    
        df_result_by_date = da.calculate_total_price_by_date(df_products, df_transactions)
        
        # display data
        print("'df_result_by_date'")
        df_result_by_date.orderBy("Transaction_Date").show(truncate=False)
        
    else:
        print("Data quality checks failed. Please correct the issues before proceeding.")

if __name__ == "__main__":
    run_data_etl()
   