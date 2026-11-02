from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

os.makedirs("/workspace/charts", exist_ok=True)

spark = SparkSession.builder \
    .appName("Walmart-Sales-Visualization") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read aggregated Parquet data from HDFS
store = spark.read.parquet("hdfs://namenode:8020/walmart/output/store_sales")
dept = spark.read.parquet("hdfs://namenode:8020/walmart/output/department_sales")
monthly = spark.read.parquet("hdfs://namenode:8020/walmart/output/monthly_sales")
holiday = spark.read.parquet("hdfs://namenode:8020/walmart/output/holiday_sales")

# 1. Top 10 stores
data = store.orderBy(col("Total_Sales").desc()).limit(10).toPandas()
plt.figure(figsize=(10, 6))
plt.bar(data["Store"].astype(str), data["Total_Sales"])
plt.title("Top 10 Walmart Stores by Sales")
plt.xlabel("Store")
plt.ylabel("Total Sales")
plt.tight_layout()
plt.savefig("/workspace/charts/top_stores.png")
plt.close()

# 2. Top 10 departments
data = dept.orderBy(col("Total_Sales").desc()).limit(10).toPandas()
plt.figure(figsize=(10, 6))
plt.bar(data["Dept"].astype(str), data["Total_Sales"])
plt.title("Top 10 Departments by Sales")
plt.xlabel("Department")
plt.ylabel("Total Sales")
plt.tight_layout()
plt.savefig("/workspace/charts/top_departments.png")
plt.close()

# 3. Monthly sales trend
data = monthly.orderBy("Year", "Month").toPandas()
data["Period"] = data["Year"].astype(str) + "-" + data["Month"].astype(str).str.zfill(2)
plt.figure(figsize=(13, 6))
plt.plot(data["Period"], data["Total_Sales"], marker="o")
plt.title("Walmart Monthly Sales Trend")
plt.xlabel("Year-Month")
plt.ylabel("Total Sales")
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig("/workspace/charts/monthly_sales.png")
plt.close()

# 4. Holiday vs non-holiday average sales
data = holiday.orderBy("IsHoliday").toPandas()
labels = ["Non-Holiday" if not value else "Holiday" for value in data["IsHoliday"]]
plt.figure(figsize=(8, 6))
plt.bar(labels, data["Average_Sales"])
plt.title("Average Sales: Holiday vs Non-Holiday")
plt.xlabel("Period")
plt.ylabel("Average Weekly Sales")
plt.tight_layout()
plt.savefig("/workspace/charts/holiday_comparison.png")
plt.close()

print("All four charts created successfully in /workspace/charts")

spark.stop()
