# Databricks notebook source
# MAGIC %md
# MAGIC Starting the Pyspark Session

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the Data Frame 1

# COMMAND ----------

df_1 = [
    (1, "Amit", 101, 50000, "2022-01-10"),
    (2, "Neha", 102, 60000, "2021-03-15"),
    (3, "Rahul", 103, 55000, "2020-07-20"),
    (4, "Priya", 101, 65000, "2019-11-01"),
    (5, "Suresh", None, 45000, "2023-02-05")
]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

schema = StructType([
    StructField("emp_id",IntegerType(), True),
    StructField("emp_name",StringType(),True),
    StructField("dept_id", IntegerType(),True),
    StructField("salary", IntegerType(), True),
    StructField("Join_date", StringType(), True)
])

# COMMAND ----------

df_1 = spark.createDataFrame(df_1, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC checking the output of dataframe 1

# COMMAND ----------

df_1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the dataframe 2

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

department_df = [
    (101, "HR", "Mumbai"),
    (102, "IT", "Bangalore"),
    (104, "Finance", "Delhi")
]

department_schema = StructType(
    [
        StructField("Departemnt_id", IntegerType(), True),
        StructField("Department_name" ,StringType(), True),
        StructField("Loaction", StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC checking the output of dataframe 2

# COMMAND ----------

df_2 = spark.createDataFrame(department_df, department_schema)
df_2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Showing both the dataframe 

# COMMAND ----------

df_1.show()
df_2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join and it's output

# COMMAND ----------

df_1.join(df_2,df_1.dept_id == df_2.Departemnt_id,"inner").show()

# COMMAND ----------

df_1.show()
df_2.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Left Join and it's output

# COMMAND ----------

df_1.join(df_2, df_1.dept_id == df_2.Departemnt_id, "left").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Right Join and it's output

# COMMAND ----------

df_1.join(df_2, df_1.dept_id == df_2.Departemnt_id, "right").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Outer Join and it's output

# COMMAND ----------

df_1.join(df_2, df_1.dept_id == df_2.Departemnt_id, "outer").show()