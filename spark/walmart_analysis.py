from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, month, year, round
)

spark = SparkSession.builder \
    .appName("Walmart-Big-Data-Analytics") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read dataset from HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:8020/walmart/input/train.csv")

print("\n========== WALMART DATA ANALYSIS ==========")

# Data cleaning
clean_df = df.dropna().dropDuplicates()

print("\nTotal records:", clean_df.count())

# 1. Overall sales
print("\n1. OVERALL SALES")
clean_df.select(
    round(sum("Weekly_Sales"), 2).alias("Total_Sales"),
    round(avg("Weekly_Sales"), 2).alias("Average_Sales")
).show()

# 2. Store-wise sales
print("\n2. STORE-WISE SALES")
store_sales = clean_df.groupBy("Store").agg(
    round(sum("Weekly_Sales"), 2).alias("Total_Sales")
).orderBy(col("Total_Sales").desc())

store_sales.show(10)

# 3. Department-wise sales
print("\n3. DEPARTMENT-WISE SALES")
dept_sales = clean_df.groupBy("Dept").agg(
    round(sum("Weekly_Sales"), 2).alias("Total_Sales")
).orderBy(col("Total_Sales").desc())

dept_sales.show(10)

# 4. Holiday comparison
print("\n4. HOLIDAY VS NON-HOLIDAY SALES")
holiday_sales = clean_df.groupBy("IsHoliday").agg(
    round(avg("Weekly_Sales"), 2).alias("Average_Sales"),
    count("*").alias("Record_Count")
)

holiday_sales.show()

# 5. Monthly sales trend
print("\n5. MONTHLY SALES TREND")
monthly_sales = clean_df.groupBy(
    year("Date").alias("Year"),
    month("Date").alias("Month")
).agg(
    round(sum("Weekly_Sales"), 2).alias("Total_Sales")
).orderBy("Year", "Month")

monthly_sales.show(50)

# Save results in Parquet format
store_sales.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/walmart/output/store_sales"
)

dept_sales.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/walmart/output/department_sales"
)

monthly_sales.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/walmart/output/monthly_sales"
)

holiday_sales.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/walmart/output/holiday_sales"
)

print("\nAll analytics completed successfully!")
print("Results saved in HDFS as Parquet files.")

spark.stop()
