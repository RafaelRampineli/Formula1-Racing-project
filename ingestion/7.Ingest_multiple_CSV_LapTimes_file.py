# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Multiple LapTimes.CSV Folder

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

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

# Reading multiple csv files
lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{var_filedate}/lap_times") #Reading all inside a folder
    #.csv("/mnt/formula1555/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

#lap_times_df = spark.sql("Select * from f1_raw.lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC ####### 1. Rename driverId and raceID
# MAGIC ####### 2. Add Ingestion_date with current timestamp

# COMMAND ----------

lap_times_df = add_ingestion_date(lap_times_df) \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Writing data to Datalake in parquet format

# COMMAND ----------

# Writing data to parquet
# lap_times_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
#lap_times_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

#overwrite_partition(lap_times_df, 'f1_processed', 'lap_times', 'race_id')

# Using Delta Lake:  input_df, db_name, table_name, folder_path, merge_condition, partition_column):
merge_condition = 'tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap'
merge_delta_data(lap_times_df, 'f1_processed','lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

display(lap_times_df)