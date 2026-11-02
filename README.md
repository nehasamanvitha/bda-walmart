# Walmart Big Data Analytics

A Big Data Analytics project that analyzes Walmart historical weekly sales data using Hadoop, MapReduce, Apache Hive, Apache Pig and Apache Spark.

The project demonstrates how distributed data processing technologies can be used to process large datasets, identify sales trends, compare holiday and non-holiday performance, and discover top-performing stores and departments.

## Project Overview

Retail businesses generate large volumes of data every day. Analyzing this data helps organizations understand sales patterns, evaluate store performance and make better business decisions.

This project uses Walmart historical sales data to perform large-scale data processing and analytics through different Big Data technologies.

## Objectives

- Process and analyze large-scale Walmart sales data.
- Calculate total and average weekly sales.
- Identify top-performing stores and departments.
- Compare holiday and non-holiday sales.
- Analyze monthly sales trends.
- Visualize analytical results using Python charts.
- Explore Hadoop-based storage and distributed data processing.

## Dataset

**Dataset:** Walmart Historical Sales Dataset

**File:** `dataset/train 3.csv`

The dataset contains 421,570 sales records.

| Attribute | Description |
|---|---|
| Store | Store identification number |
| Dept | Department identification number |
| Date | Sales transaction week |
| Weekly_Sales | Weekly sales amount |
| IsHoliday | Indicates whether the week is a holiday |

## Technologies Used

| Technology | Purpose |
|---|---|
| Apache Hadoop | Distributed storage using HDFS |
| HDFS | Stores the Walmart dataset |
| MapReduce | Calculates total sales by store |
| Apache Hive | SQL-based data analysis
