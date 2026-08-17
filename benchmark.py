from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import pandas as pd
import time
import gc

spark = SparkSession.builder \
    .appName("IST3134 Simple Benchmark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark_file = "s3://ist3134-us-accidents-natalie/US_Accidents_March23.csv"
pandas_file = "/home/hadoop/US_Accidents_March23.csv"

sizes = [100000, 1000000, 7728394]

print("\n===== IST3134 BENCHMARK =====")

for size in sizes:

    print("\n====================================")
    print(f"Dataset size: {size:,} rows")
    print("====================================")

    # -----------------------
    # PySpark
    # -----------------------

    spark_df = (
        spark.read
        .option("header", True)
        .csv(spark_file)
        .select("State")
        .limit(size)
    )

    start = time.perf_counter()

    spark_result = (
        spark_df
        .filter(col("State").isNotNull())
        .groupBy("State")
        .count()
        .orderBy(desc("count"))
        .limit(10)
        .collect()
    )

    spark_time = time.perf_counter() - start

    print("PySpark time:",
          round(spark_time, 3),
          "seconds")

    # -----------------------
    # Pandas
    # -----------------------

    start = time.perf_counter()

    pandas_df = pd.read_csv(
        pandas_file,
        usecols=["State"],
        nrows=size
    )

    pandas_result = (
        pandas_df
        .dropna(subset=["State"])
        .groupby("State")
        .size()
        .sort_values(ascending=False)
        .head(10)
    )

    pandas_time = time.perf_counter() - start

    print("Pandas time:",
          round(pandas_time, 3),
          "seconds")

    print("\nPySpark top state:",
          spark_result[0])

    print("Pandas top state:")
    print(pandas_result.head(1))

    del pandas_df
    gc.collect()

print("\n===== BENCHMARK COMPLETE =====")

spark.stop()