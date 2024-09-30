# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Results.Json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType , FloatType
from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark Dataframe reader API

# COMMAND ----------

# # Use this way when you don't have createad a database inside databricks and save the data in a folder.
# # We did it on: /Workspace/Formula1-Racing-Project/raw/1.create_raw_tables 

# # Defining Schema
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(), True)
])

results_df = spark.read \
        .schema(results_schema) \
        .json(f'{raw_folder_path}/{var_filedate}/results.json')

# COMMAND ----------

# results_df = spark.sql("Select * from f1_raw.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename Columns and add new columns

# COMMAND ----------

results_final_df = add_ingestion_date(results_df) \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(var_datasource)) \
    .withColumn("file_date", lit(var_filedate)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns and duplicate values

# COMMAND ----------

results_final_df = results_final_df.drop("statusId")

# COMMAND ----------

from pyspark.sql import Row

results_final_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Writing data to Datalake in parquet format partition by race_id

# COMMAND ----------

# MAGIC %md
# MAGIC #Method 1: 
# MAGIC deleting partition before inserting
# MAGIC
# MAGIC Method 2 has improve performance than Method 1

# COMMAND ----------

# Check if partition exists and remove before insert new data
#for race_id_list in results_final_df.select("race_id").distinct().collect():
    #if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
     #  spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# Writing data to parquet
# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1555/processed/results")

# Writing data as a table saving on Database f1_processed in the workspace. Using Managed Tables
#results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #Method 2: 
# MAGIC Use SaveAsTable to first insertion, and use InsertInto for subsequent insertions
# MAGIC
# MAGIC Attention: 
# MAGIC saveAsTable uses the partition column and adds it at the end. 
# MAGIC insertInto works using the order of the columns (exactly as calling an SQL insertInto) instead of the columns name. 
# MAGIC
# MAGIC You can check this information at dislay above.
# MAGIC
# MAGIC So to work, we must select our columns putting de partition column at last column of dataset.
# MAGIC
# MAGIC We need to set the spark.sql.sources.partitionOverwriteMode setting to dynamic, the dataset needs to be partitioned, and the write mode overwrite.
# MAGIC Using this set, when data will be written and overwrite just the partition.

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# Using Delta Lake:  input_df, db_name, table_name, folder_path, merge_condition, partition_column):
merge_condition = 'tgt.result_id = src.result_id AND tgt.race_id = src.race_id'
merge_delta_data(results_final_df, 'f1_processed','results', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

display(results_final_df.select("race_id").distinct())

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC where race_id = 1053
# MAGIC GROUP BY race_id
# MAGIC order by race_id desc;