# Apache Spark Machine Learning Project
This project demonstrates how to utilize Apache Spark to convert Parquet file data into a CSV format and subsequently train a Random Forest model.

# Overview
Apache Spark is a powerful, open-source processing engine for data in the Hadoop cluster, built around speed, ease of use, and sophisticated analytics. It provides high-level APIs in Java, Scala, Python and R, and supports an optimized engine that supports general computation graphs for data analysis. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming.

# Project Description
In this project, we have two major steps:

- Parquet to CSV conversion: The project starts with the conversion of a data file from Parquet to CSV format using Apache Spark. Parquet is a columnar storage file format that is optimized for use with most tools in the Hadoop ecosystem. However, CSV is more commonly used and easily readable.

- Random Forest Model Training: Once the data is available in CSV format, we proceed to train a Random Forest model. The model training is accomplished using the Spark ML library. The final trained model is exported in Predictive Model Markup Language (PMML) format.
