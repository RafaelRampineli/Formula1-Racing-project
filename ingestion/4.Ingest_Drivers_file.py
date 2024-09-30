# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Drivers.Json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#using widgets to pass notebook parameters
dbutils.widgets.text("data_source", "")
var_datasource = dbutils.widgets.get("data_source")

dbutils.widgets.text("file_date", "2021-03-21")
var_filedate = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark Dataframe reader API

# COMMAND ----------

# Use this way when you don't have createad a database inside databricks and save the data in a folder.
# We did it on: /Workspace/Formula1-Racing-Project/raw/1.create_raw_tables 

# # Defining Schema
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                      StructField("surname", StringType(), True)                        
])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True),

])

drivers_df = spark.read \
        .schema(drivers_schema) \
        .json(f'{raw_folder_path}/{var_filedate}/drivers.json')

# COMMAND ----------

# drivers_df = spark.sql("Select * from f1_raw.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename Columns and add new columns

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_df) \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_final_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Writing data to Datalake in parquet format

# COMMAND ----------

# Writing data to parquet
#drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")