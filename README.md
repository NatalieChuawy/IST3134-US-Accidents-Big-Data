# IST3134-US-Accidents-Big-Data
Big Data analysis of US traffic accident patterns using PySpark and Spark SQL on Amazon EMR, with Pandas as a comparison approach.

This project analyses the US Accidents (2016–2023) dataset using
PySpark and Spark SQL on Amazon EMR.

## Dataset

Dataset: US Accidents (2016–2023)

Source:
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

The dataset used in this project contains:

- 7,728,394 records
- 46 original attributes
- 14 selected attributes for the main analysis

The full dataset is not included in this repository because of its size.

## Technologies Used

- Python
- PySpark
- Apache Spark 3.5.6
- Hadoop 3.4.2
- Spark SQL
- Amazon EMR 7.13.0
- Amazon S3
- YARN
- Pandas

## AWS Environment

The main PySpark implementation was executed using Amazon EMR through
AWS Academy Learner Lab.

Cluster configuration:

- 1 Primary node
- 2 Core nodes
- 0 Task nodes
- Region: us-east-1 (N. Virginia)

## Analysis Performed

The project includes:

1. Data loading and attribute selection
2. Duplicate and missing-value analysis
3. Temporal feature engineering
4. Spatial accident analysis
5. Temporal accident analysis
6. Weather and environmental analysis
7. Traffic-impact severity analysis
8. Day and night analysis
9. Junction and traffic-signal analysis
10. Accident-duration analysis
11. Spark SQL validation
12. Spark physical execution plan analysis
13. PySpark and Pandas comparison

## Main Files

### pyspark_analysis.py
Main distributed PySpark implementation.

### pandas_analysis.py
Pandas implementation used as the conventional single-machine comparison.

### benchmark.py
Performance comparison between PySpark and Pandas using the full dataset.

### spark_sql_queries.sql
Spark SQL query used for state-level aggregation.

## Performance Comparison

For the full dataset of 7,728,394 records:

| Approach | Execution Time |
|---|---:|
| PySpark | 9.505 seconds |
| Pandas | 23.478 seconds |

Both approaches returned California (CA) as the state with the highest
number of recorded accidents, with 1,741,433 records.

The timing results represent the tested Amazon EMR environment and should
not be interpreted as evidence that PySpark is always faster than Pandas.

## Main Spatial Result

The three states with the highest number of recorded accidents were:

1. California - 1,741,433
2. Florida - 880,192
3. Texas - 582,837

## Authors
Group 43
IST3134 Big Data Analytics in the Cloud
May Semester 2026
Sunway University
