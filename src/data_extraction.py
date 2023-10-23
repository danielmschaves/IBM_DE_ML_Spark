import os
import fnmatch
import shutil
import random
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import lit

def initialize_spark(master="local[*]"):
    """
    Purpose:
    Initializes a SparkContext (sc) and SparkSession (spark), which are entry points for any Spark functionality.

    Parameters:
    master: The URL of the cluster it connects to. It defaults to "local[*]", which means that it runs locally using as many worker threads as logical cores on your machine.
    
    Exception Handling:
    It catches any Exception that might occur during initialization and raises it after logging.
    
    Code Notes:
    SparkContext.getOrCreate(SparkConf().setMaster(master)): Ensures a SparkContext is created or retrieved if it already exists.
    SparkSession.builder.getOrCreate(): Ensures a SparkSession is created or retrieved if it already exists.
    """
    try:
        sc = SparkContext.getOrCreate(SparkConf().setMaster(master))
        spark = SparkSession.builder.getOrCreate()
        return sc, spark
    except Exception as e:
        print(f"Error initializing Spark: {e}")
        raise

def clone_HMP_dataset():
    """
    Purpose:
    Clones the HMP dataset from GitHub into the local file system.

    Exception Handling:
    Catches any Exception that might occur during the cloning process and raises it after logging.
    
    Code Notes:
    os.system('rm -Rf HMP_Dataset'): Deletes any existing directory named HMP_Dataset.
    os.system('git clone https://github.com/wchill/HMP_Dataset'): Clones the dataset from the GitHub repository.
    """
    try:
        if not os.path.exists('data'):
            os.makedirs('data')
        os.chdir('data')  # Change to 'data' directory
        os.system('rm -Rf HMP_Dataset')
        os.system('git clone https://github.com/wchill/HMP_Dataset')
        os.chdir('..')  # Change back to root directory
    except Exception as e:
        print(f"Error cloning HMP dataset: {e}")
        raise

def create_dataframe(spark, schema, sample=1.0):
    """
    Purpose:
    Creates a Spark DataFrame by reading all data files from the HMP dataset.

    Parameters:
    spark: The SparkSession object.
    schema: The schema that the DataFrame should follow.
    sample: Fraction of the data to read from each file, used for sampling. Default is 1.0, meaning no sampling.
    
    Exception Handling:
    Catches any Exception that might occur during DataFrame creation and raises it after logging.
    """
    try:
        d = 'data/HMP_Dataset/'  # or the full path to where HMP_Dataset is downloaded
        if not os.path.exists(d):
            print(f"Directory {d} does not exist.")
            return
        
        file_list_filtered = [s for s in os.listdir(d) if os.path.isdir(os.path.join(d, s)) & ~fnmatch.fnmatch(s, '.*')]
        df = None
        
        for category in file_list_filtered:
            data_files = os.listdir(d + category)
            for data_file in data_files:
                if sample < 1.0 and random.random() > sample:
                    continue
                temp_df = spark.read.option("header", "false").option("delimiter", " ").csv(d + category + '/' + data_file, schema=schema)
                temp_df = temp_df.withColumn("source", lit(data_file))
                temp_df = temp_df.withColumn("class", lit(category))
                if df is None:
                    df = temp_df
                else:
                    df = df.union(temp_df)
        return df
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        raise

def write_to_csv(df, data_dir, data_csv):
    """
    Purpose:
    Writes the given DataFrame to a CSV file.

    Parameters:
    df: The DataFrame to write.
    data_dir: The directory where to write the data.
    data_csv: The name of the output CSV file.
    Exception Handling:
    Catches any Exception that might occur during the write process and raises it after logging.
    """
    try:
        if os.path.exists(data_dir + data_csv):
            shutil.rmtree(data_dir + data_csv)
        df.write.option("header", "true").csv(data_dir + data_csv)
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        raise

if __name__ == "__main__":
    master = "local[*]"
    sample = 1.0
    schema = StructType([StructField("x", IntegerType(), True),
                         StructField("y", IntegerType(), True),
                         StructField("z", IntegerType(), True)])
    data_dir = 'data/'
    data_csv = 'data.csv'
    
    sc, spark = initialize_spark(master)
    clone_HMP_dataset()
    df = create_dataframe(spark, schema, sample)
    write_to_csv(df, data_dir, data_csv)
