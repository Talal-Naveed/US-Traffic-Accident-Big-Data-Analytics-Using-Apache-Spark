import os

# Tell Spark which Python installation to use
python_path = r"C:\Users\tnave\.venv\Scripts\python.exe"
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession


# Create a local Spark session
spark = (
    SparkSession.builder
    .appName("US Accidents Dataset")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Dataset location
file_path = r"..\data\US_Accidents_March23.csv"


# Load CSV into a Spark DataFrame
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)


# Basic dataset information
print("\n==============================")
print("US ACCIDENTS DATASET")
print("==============================")

print("\nNumber of rows:")
print(df.count())

print("\nNumber of columns:")
print(len(df.columns))

print("\nColumn names:")
print(df.columns)

print("\nFirst 5 records:")
df.show(5, truncate=False)

print("\nDataset schema:")
df.printSchema()


# Stop Spark
spark.stop()