# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Multiple MultiLine Qualifying.JSON 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using the spark Dataframe reader API

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# # Use this way when you don't have createad a database inside databricks and save the data in a folder.
# # We did it on: /Workspace/Formula1-Racing-Project/raw/1.create_raw_tables 

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
])

# Reading multiple csv files
qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{var_filedate}/qualifying") #Reading all inside a folder
    #.json("/mnt/formula1555/raw/qualifying/qualifying_split*.json")

# COMMAND ----------

#qualifying_df = spark.sql("Select * from f1_raw.qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC ####### 1. Rename qualifyingId, raceID, driverId, constructorId
# MAGIC ####### 2. Add Ingestion_date with current timestamp

# COMMAND ----------

qualifying_df = add_ingestion_date(qualifying_df) \
    .withColumnRenamed("qualifyingId", "qualifying_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Writing data to Datalake in parquet format

# COMMAND ----------

# Writing data to parquet
# qualifying_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
#qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

overwrite_partition(qualifying_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

display(qualifying_df)