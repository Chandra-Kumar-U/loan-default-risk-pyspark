# Databricks notebook source
# MAGIC %md #Loan Default Risk Analysis

# COMMAND ----------

# MAGIC %md #creating DataFrame

# COMMAND ----------

from  pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data1 = [
    ('L001', 'C001', 'Rahul Sharma', 'Mumbai', 500000, 24, 8.5, 'Home Loan', 25000, 0, 'Employed'),
    ('L002', 'C002', 'Priya Patel', 'Delhi', 300000, 36, 10.5, 'Personal Loan', 15000, 2, 'Employed'),
    ('L003', 'C003', 'Amit Singh', 'Bangalore', 750000, 48, 9.0, 'Home Loan', 35000, 0, 'Self Employed'),
    ('L004', 'C004', 'Sneha Reddy', 'Hyderabad', 200000, 24, 12.0, 'Personal Loan', 10000, 5, 'Unemployed'),
    ('L005', 'C005', 'Vikram Mehta', 'Chennai', 1000000, 60, 8.0, 'Home Loan', 50000, 0, 'Employed'),
    ('L006', 'C006', 'Ananya Gupta', 'Pune', 150000, 12, 14.0, 'Personal Loan', 8000, 8, 'Unemployed'),
    ('L007', 'C007', 'Rohit Kumar', 'Mumbai', 600000, 36, 9.5, 'Car Loan', 30000, 1, 'Employed'),
    ('L008', 'C008', 'Deepa Nair', 'Bangalore', 250000, 24, 11.0, 'Personal Loan', 12000, 3, 'Self Employed'),
    ('L009', 'C009', 'Karan Shah', 'Delhi', 800000, 48, 8.8, 'Home Loan', 40000, 0, 'Employed'),
    ('L010', 'C010', 'Meera Joshi', 'Mumbai', 180000, 12, 13.5, 'Personal Loan', 9000, 6, 'Unemployed'),
    ('L011', 'C011', 'Arun Verma', 'Chennai', 450000, 36, 9.2, 'Car Loan', 22000, 1, 'Employed'),
    ('L012', 'C012', 'Kavya Rao', 'Hyderabad', 900000, 60, 8.2, 'Home Loan', 45000, 0, 'Employed'),
    ('L013', 'C013', 'Suresh Iyer', 'Pune', 120000, 12, 15.0, 'Personal Loan', 6000, 10, 'Unemployed'),
    ('L014', 'C014', 'Pooja Menon', 'Mumbai', 550000, 36, 9.8, 'Car Loan', 28000, 2, 'Self Employed'),
    ('L015', 'C015', 'Rajesh Pillai', 'Delhi', 700000, 48, 8.6, 'Home Loan', 35000, 0, 'Employed'),
    ('L016', 'C016', 'Nisha Sharma', 'Bangalore', 160000, 12, 13.0, 'Personal Loan', 8500, 7, 'Unemployed'),
    ('L017', 'C017', 'Manoj Tiwari', 'Chennai', 400000, 36, 10.0, 'Car Loan', 20000, 3, 'Self Employed'),
    ('L018', 'C018', 'Divya Krishnan', 'Pune', 850000, 60, 8.3, 'Home Loan', 42000, 0, 'Employed'),
    ('L019', 'C019', 'Sanjay Dubey', 'Mumbai', 130000, 12, 14.5, 'Personal Loan', 7000, 9, 'Unemployed'),
    ('L020', 'C020', 'Preethi Nair', 'Delhi', 650000, 48, 9.1, 'Car Loan', 32000, 1, 'Employed')
]

# COMMAND ----------

schema1 = '''loan_id string,
 customer_id string,
 customer_name string,
 city string,
 loan_amount integer,
 tenure_months integer,
 interest_rate double,
 loan_type string,
 monthly_income integer,
 missed_payments integer,
 employment_status string
'''

# COMMAND ----------

df1 = spark.createDataFrame(data1,schema1)
df1.display()

# COMMAND ----------

df1 =  df1.withColumn('risk_category',when(col('missed_payments')>= 6, 'High Risk')\
    .when(((col("missed_payments")>=3) & (col('missed_payments')<6)),'Medium Risk')\
        .otherwise('Low Risk'))

# COMMAND ----------

df1 = df1.withColumn('loan_to_income_ratio', round((col("loan_amount")/col('monthly_income')),2))

# COMMAND ----------

df1 = df1.filter(lower(col('employment_status')).isin(['employed','self employed']))

# COMMAND ----------

df1.display()

# COMMAND ----------

df_grouped = df1.groupBy('loan_type').agg(
    count('loan_id').alias('total_loans'),
    sum('loan_amount').alias('total_loan_amount'),
    avg('interest_rate').alias('avg_interest_rate'),
    avg('missed_payments').alias('avg_missed_payments')
)

df_grouped.display()

# COMMAND ----------

df1 = df1.withColumn('rnk',rank().over(Window.partitionBy(col('loan_type')).orderBy(col('loan_amount').desc())))

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.createTempView('loan_data')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT city, sum(loan_amount) as total_loan_amount, avg(interest_rate) as avg_interest_rate 
# MAGIC FROM loan_data 
# MAGIC GROUP BY city;

# COMMAND ----------

# Load - Save as Delta table
df1.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('loan_default_risk_clean')

# COMMAND ----------

# Save grouped summary as Delta table
df_grouped.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('loan_type_summary')

# COMMAND ----------

