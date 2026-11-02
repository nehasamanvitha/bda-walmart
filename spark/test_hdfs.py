from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Walmart-HDFS-Test") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:8020/walmart/input/train.csv")

print("\nWALMART DATASET SCHEMA")
df.printSchema()

print("\nFIRST FIVE RECORDS")
df.show(5)

print("\nTOTAL NUMBER OF RECORDS:", df.count())

spark.stop()
