
-- Load Walmart dataset from HDFS
sales = LOAD '/walmart/input/train.csv'
USING PigStorage(',')
AS (
    store_id:int,
    dept_id:int,
    sale_date:chararray,
    weekly_sales:double,
    holiday_flag:chararray
);

-- Remove header and invalid store records
filtered_sales = FILTER sales BY store_id > 0;

-- Calculate store-wise total sales
store_group = GROUP filtered_sales BY store_id;

store_sales = FOREACH store_group GENERATE
    group AS store_id,
    SUM(filtered_sales.weekly_sales) AS total_sales;

-- Calculate department-wise total sales
dept_group = GROUP filtered_sales BY dept_id;

dept_sales = FOREACH dept_group GENERATE
    group AS dept_id,
    SUM(filtered_sales.weekly_sales) AS total_sales;

-- Calculate holiday-wise average sales
holiday_group = GROUP filtered_sales BY holiday_flag;

holiday_sales = FOREACH holiday_group GENERATE
    group AS holiday,
    AVG(filtered_sales.weekly_sales) AS average_sales;

-- Display results
DUMP store_sales;
DUMP dept_sales;
DUMP holiday_sales;

-- Store results in HDFS
STORE store_sales INTO '/walmart/output/pig_store_sales' USING PigStorage(',');
STORE dept_sales INTO '/walmart/output/pig_department_sales' USING PigStorage(',');
STORE holiday_sales INTO '/walmart/output/pig_holiday_sales' USING PigStorage(',');
