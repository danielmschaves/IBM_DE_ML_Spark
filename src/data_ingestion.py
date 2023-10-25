from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import os
import logging

def initialize_spark(app_name, master_config):
    """
    initialize_spark(app_name, master_config)
    Purpose:
    This function initializes a Spark session with a given application name and master configuration.

    Parameters:
    app_name: The name of the Spark application. It helps in identifying your Spark application on the Spark web UI.
    master_config: The master URL to connect to, such as "local" to run Spark on the local machine.

    Code Explanation:
    SparkSession.builder: Initializes a SparkSession builder.
    .appName(app_name): Sets the application name.
    .config("spark.master", master_config): Sets the master URL.
    .getOrCreate(): Getss an existing SparkSession or creates a new one if none exist.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.master", master_config) \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark: {e}")
        raise

def load_data(spark, file_path):
    """
    Purpose:
    Loads data from a Parquet file into a Spark DataFrame.

    Parameters:
    spark: The Spark session.
    file_path: The path to the Parquet file.

    Code Explanation:
    spark.read.parquet(file_path): Reads a Parquet file into a Spark DataFrame.
    
    """
    try:
        df = spark.read.parquet(file_path)
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def preprocess_data(df):
    """
    Purpose:
    Acts as a placeholder for any data preprocessing steps like type conversion, renaming columns, etc.

    Parameters:
    df: The Spark DataFrame to preprocess.

    Code Explanation:
    Placeholder for user-defined preprocessing steps.
    """
    try:
        # Your preprocessing steps here
        return df
    except Exception as e:
        logging.error(f"Error in data preprocessing: {e}")
        raise

def split_data(df):
    """
    Purpose
    Splits the input DataFrame into training and test sets.

    Parameters
    df: The Spark DataFrame to split.
    Code Explanation
    df.randomSplit([0.8, 0.2], seed=42): Randomly splits the DataFrame into training and test sets. 
    
"""
    try:
        train, test = df.randomSplit([0.8, 0.2], seed=42)
        return train, test
    except Exception as e:
        logging.error(f"Error splitting data: {e}")
        raise

if __name__ == "__main__":
    # Initialize Logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize Spark
        spark = initialize_spark("RandomForestModel", "local[*]")

        # Load Data
        data_dir = os.getenv('DATA_DIR', './data/')
        parquet_file = os.path.join(data_dir, 'data.parquet')
        df = load_data(spark, parquet_file)

        # Preprocess Data
        df = preprocess_data(df)

        # Split Data
        train_data, test_data = split_data(df)
        
        # Log basic info
        logging.info(f"Number of training records: {train_data.count()}")
        logging.info(f"Number of testing records: {test_data.count()}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
