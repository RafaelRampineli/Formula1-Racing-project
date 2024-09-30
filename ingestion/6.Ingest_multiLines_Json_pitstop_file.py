# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Pit_stops.Json MultiLines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark Dataframe reader API

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

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

# By Default Spark doesn't read MultiFiles JSON, so we need to specify it in the option
pit_stops_df = spark.read \
    .schema(pit_stop_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{var_filedate}/pit_stops.json")

# COMMAND ----------

# pit_stops_df = spark.sql("Select * from f1_raw.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC ####### 1. Rename driverId and raceID
# MAGIC ####### 2. Add Ingestion_date with current timestamp

# COMMAND ----------

pit_stops_df = add_ingestion_date(pit_stops_df) \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Writing data to Datalake in parquet format

# COMMAND ----------

# Writing data to parquet
# pit_stops_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
# pit_stops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

#overwrite_partition(pit_stops_df, 'f1_processed', 'pit_stops', 'race_id')

# Using Delta Lake:  input_df, db_name, table_name, folder_path, merge_condition, partition_column):
merge_condition = 'tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop'
merge_delta_data(pit_stops_df, 'f1_processed','pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

display(pit_stops_df)